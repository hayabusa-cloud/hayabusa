package hybs

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lucas-clemente/quic-go"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/xtaci/kcp-go"
	"gopkg.in/yaml.v2"
)

type realtimeConfig struct {
	ID                 string        `yaml:"id" required:"true"`
	Network            string        `yaml:"network" default:"udp"`
	Address            string        `yaml:"address" default:"localhost:8443"`
	Protocol           string        `yaml:"protocol" defualt:"kcp"`
	ByteOrder          string        `yaml:"byte_order" default:"big"`
	CertFile           string        `yaml:"cert_file" default:"cert.pem"`
	KeyFile            string        `yaml:"key_file" default:"key.pem"`
	Password           string        `yaml:"password" default:""`
	Salt               string        `yaml:"salt" default:""`
	WriteTimeout       time.Duration `yaml:"write_timeout" default:"200ms"`
	ReadTimeout        time.Duration `yaml:"read_timeout" default:"10s"`
	Heartbeat          time.Duration `yaml:"heartbeat" default:"5s"`
	ControllerFilepath string        `yaml:"controller_filepath" default:"config/controllers/"`

	realtimeKCPConfig `yaml:",inline"`
}
type realtimeControllerConfigItem struct {
	Event       string   `yaml:"event" default:"request"`
	Code        string   `yaml:"code" required:"true"`
	Bits        string   `yaml:"bits" default:"16"`
	Description string   `yaml:"description"`
	Middlewares []string `yaml:"middlewares"`
	Handler     string   `yaml:"handler"`
	Redis       string   `yaml:"redis"`
	Cache       string   `yaml:"cache"`
	CsvMux      string   `yaml:"csv_mux"`
}
type realtimeControllerConfig []realtimeControllerConfigItem

const (
	rtPacketPayloadSizeBit  = 14
	rtPacketPayloadSize     = 1 << rtPacketPayloadSizeBit
	rtPacketPayloadSizeMask = rtPacketPayloadSize | (rtPacketPayloadSize << 1)

	rtPacketLenSize    = 2
	rtPacketHeaderSize = 1
	rtPacketCodeSize   = 2

	rtPacketHeaderMaxSize = 0x10
)

type rtMessageBuffer struct {
	status       uint8
	header       uint8
	length       uint16
	remain       uint16
	payload      []byte
	payloadIndex uint16
}

func (b *rtMessageBuffer) reset() {
	b.status, b.length, b.remain = 0, 0, 0
}

// RealtimeSession is interface rtSession implements
type RealtimeSession interface {
	// Server returns the reference of realtime server instance
	Server() (server RealtimeServer)
	// Close closes the connection with an error
	Close(code ErrorCode, err string)
	// AppID returns the application id of session
	AppID() []byte
	// UserID returns the user id(hayabusa id) of session
	UserID() []byte
	// UserID returns the string type user id(hayabusa id) of session
	StringUserID() string
	// Token returns the access token that session use
	Token() []byte
}

type rtConnInterface interface {
	AcceptStream(context.Context) (rtStreamInterface, error)
	CloseWithError(code ErrorCode) error
}

type rtStreamInterface interface {
	io.Reader
	io.Writer
	Close() error
}

type rtSession struct {
	// reference
	server *hybsRealtimeServer
	// network
	mu        sync.RWMutex
	id        uint16
	active    bool
	rawConn   rtConnInterface
	rawStream rtStreamInterface
	msgBuffer *rtMessageBuffer
	// limit too many requests
	loadValue int16
	loadAt    Time
	// authentication
	userBase *RealtimeUserBase

	// room
	roomID  *uint16
	roomPos *uint16
	// packets queue
	sending chan []byte
}

const udpSocketBufferLength = 0x4100

func (ss *rtSession) Server() (server RealtimeServer) {
	return ss.server
}
func (ss *rtSession) Close(code ErrorCode, err string) {
	// log err string
	ss.server.logger.WithField("err", err).Info("close conn")
	ss.rawConn.CloseWithError(code)
}
func (ss *rtSession) AppID() []byte {
	return ss.userBase.AppID
}
func (ss *rtSession) UserID() []byte {
	return ss.userBase.UserID
}
func (ss *rtSession) StringUserID() string {
	return ss.userBase.StringUserID
}
func (ss *rtSession) Permission() uint8 {
	return ss.userBase.Permission
}
func (ss *rtSession) Token() []byte {
	return ss.userBase.Token
}
func (ss *rtSession) receiveBytes() (received int, err error) {
	if ss.server.isClosing {
		return 0, nil
	}
	var bytes = ss.server.bufferPool.Get().([]byte)
	defer ss.server.bufferPool.Put(bytes)
	if received, err = ss.rawStream.Read(bytes); err != nil {
		return 0, err
	}
	ss.loadValue += 1
	if ss.loadValue > 0x800 {
		var seconds = ss.server.Now().Sub(ss.loadAt).Seconds()
		if seconds < 5 {
			ss.server.debug("too many requests:%d", ss.id)
			ss.Close(ErrorCodeTooManyConnections, "too many requests")
		}
		ss.loadValue = 0
		ss.loadAt = ss.server.Now()
	}
	// if transport layer protocol is udp or other datagram protocol
	received, err = ss.receivePacketBytes(bytes, received)
	// if transport layer protocol is tcp or other stream protocol
	// received, err = ss.receiveStreamBytes(bytes, received)
	if err != nil {
		return 0, err
	}
	return received, err
}

// *** this method is for datagram protocols only ***
// *** DO NOT use it for stream protocols. ***
// implement of state machine for receiving packets
func (ss *rtSession) receivePacketBytes(bytes []byte, received int) (n int, err error) {
	var offset, length = uint16(0), uint16(received)
	for offset < length {
		switch ss.msgBuffer.status {
		case rtPacketStatusHeader:
			ss.msgBuffer.header = bytes[offset]
			offset++
			ss.processHeaderDispatch()
			continue
		case rtPacketStatusAuthToken:
			ss.msgBuffer.length = uint16(bytes[offset])
			offset++
			if offset+ss.msgBuffer.length > length {
				return 0, fmt.Errorf("bad auth packet")
			}
			ss.msgBuffer.payload = bytes[offset : offset+ss.msgBuffer.length]
			offset += ss.msgBuffer.length
			ss.processAuthentication()
			ss.msgBuffer.status = rtPacketStatusHeader
			continue
		case rtPacketStatusNTP:
			if offset+8 > length {
				return 0, fmt.Errorf("bad timestamp")
			}
			ss.msgBuffer.payload = bytes[offset : offset+8]
			offset += 8
			ss.processNTP()
			ss.msgBuffer.status = rtPacketStatusHeader
			continue
		case rtPacketStatusPayloadLength:
			if offset+2 > length {
				return 0, fmt.Errorf("bad packet len")
			}
			ss.msgBuffer.length = (uint16(bytes[offset]) << 8) | uint16(bytes[offset+1])
			offset += 2
			if ss.msgBuffer.length > uint16(rtPacketPayloadSize) {
				return 0, fmt.Errorf("too large length")
			}
			ss.msgBuffer.remain = ss.msgBuffer.length - rtPacketHeaderSize - rtPacketLenSize
			ss.msgBuffer.payloadIndex = 0
			ss.msgBuffer.status = rtPacketStatusPayloadContents
			continue
		case rtPacketStatusPayloadContents:
			var appendLength = uint16(received&0xffff) - offset
			if ss.msgBuffer.remain < appendLength {
				appendLength = ss.msgBuffer.remain
			}
			// here DO NOT copy received buffered bytes
			ss.msgBuffer.payload = bytes[offset : offset+appendLength]
			offset += appendLength
			ss.msgBuffer.remain -= appendLength
			ss.msgBuffer.payloadIndex += appendLength
			if ss.msgBuffer.remain == 0 {
				ss.processPayload()
				ss.msgBuffer.payloadIndex = 0
				ss.msgBuffer.status = rtPacketStatusHeader
			}
			continue
		case rtPacketStatusEchoTest:
			bytes[offset-1] = rtHeaderPV0 | rtHeaderPDSC | rtHeaderCmdEchoTest
			ss.send(bytes[offset-1 : received])
			offset = length
			ss.msgBuffer.status = rtPacketStatusHeader
			continue
		default:
			break
		}
	}
	return received, nil
}
func (ss *rtSession) processHeaderDispatch() {
	// internal logic
	var cmd = ss.msgBuffer.header & 0x0f
	switch cmd {
	case rtHeaderCmdHello:
		ss.msgBuffer.status = rtPacketStatusNTP
	case rtHeaderCmdClosing:
		if ss.active {
			ss.server.onDisconnect(ss, ErrorCodeConnectionClosing)
		}
		break
	case rtHeaderCmdClosed:
		if ss.active {
			ss.server.onDisconnect(ss, ErrorCodeConnectionClosed)
		}
		break
	case rtHeaderCmdEstablished:
		// server -> client only
		break
	case rtHeaderCmdAuthenticate:
		ss.msgBuffer.status = rtPacketStatusAuthToken
		break
	case rtHeaderCmdProxy:
		// todo: gate<->server proxy(for builtin reversed proxy)
		break
	case rtHeaderCmdForward:
		// todo: server<->server rpc(for builtin server to server RPC)
		break
	case rtHeaderCmdBuiltin:
		ss.msgBuffer.status = rtPacketStatusPayloadLength
		break
	case rtHeaderCmdOriginal:
		ss.msgBuffer.status = rtPacketStatusPayloadLength
		break
	case rtHeaderCmdEchoTest:
		ss.msgBuffer.status = rtPacketStatusEchoTest
		break
	}
}

// implement of builtin NTP
func (ss *rtSession) processNTP() {
	var out = ss.server.outPacketPool.Get().(*rtPacket)
	defer ss.server.outPacketPool.Put(out)
	out.payload[0] = rtHeaderPV0 | rtHeaderPDSC | rtHeaderCmdNTP
	copy(out.payload[1:], ss.msgBuffer.payload)
	ss.rawStream.Write(out.payload[:1+8+8])
}

// implement of builtin authentication
func (ss *rtSession) processAuthentication() {
	var receivedToken = ss.msgBuffer.payload[:ss.msgBuffer.length]
	var dst = make([]byte, ss.msgBuffer.length/3*4+6)
	var n, err = base64.StdEncoding.Decode(dst, receivedToken)
	if err != nil {
		ss.Close(ErrorCodeConnectionClosing, "authentication failed")
		return
	}
	// original text is split by "#", app_id#user_id#token
	dst = dst[:n]
	var args = bytes.Split(dst, []byte{'#'})
	if len(args) < 3 {
		ss.Close(ErrorCodeConnectionClosing, "authentication failed")
		return
	}
	// here does authentication only
	// implement authorization middleware in business layer
	ss.userBase.AppID = args[0]
	ss.userBase.StringUserID = string(args[1])
	ss.userBase.UserID = args[1]
	ss.userBase.rawToken = args[2]
	ss.userBase.Token = receivedToken
	// permission = None(authenticated, but no permission)
	ss.userBase.Permission = RtAuthStatusNone
}
func (ss *rtSession) processPayload() {
	// context data table
	var ctxValue = ss.server.rtCtxValuePool.Get().(*rtCtxValueTable)
	// received data packet
	var inPacket = ss.server.inPacketPool.Get().(*rtPacket)
	// get reused sending packet from pool
	var outPacket = ss.server.outPacketPool.Get().(*rtPacket)
	inPacket.header = ss.msgBuffer.header
	// packet=header(1byte)+payload length(2bytes)+payload(n bytes)
	inPacket.length = ss.msgBuffer.length - rtPacketLenSize - rtPacketHeaderSize
	// payload=request code(2bytes)+body(m bytes)
	inPacket.offset = rtPacketCodeSize
	inPacket.code = 0
	// for multi-modules(goroutines) supporting, we should COPY the packet
	// was inPacket.payload = ss.msgBuffer.payload[:ss.msgBuffer.payloadIndex]
	copy(inPacket.payload, ss.msgBuffer.payload[:ss.msgBuffer.payloadIndex])
	outPacket.offset = rtPacketCodeSize
	if len(inPacket.payload) >= rtPacketCodeSize {
		inPacket.code = (uint16(inPacket.payload[0]) << 8) | uint16(inPacket.payload[1])
	}
	// set context of logger
	var logEntry = ss.server.logger.WithField("session", fmt.Sprintf("%x", ss.id))
	logEntry = logEntry.WithField("code", fmt.Sprintf("%x", inPacket.code))
	var newCtx = &rtCtx{
		// core component
		server:    ss.server,
		sessionID: ss.id,
		in:        inPacket,
		out:       outPacket,
		// context
		value:         ctxValue,
		defaultRedis:  "",
		defaultCache:  "",
		defaultCsvMux: "",
		// log
		logEntry: logEntry,
	}
	ss.server.serveCtx(newCtx)
}
func (ss *rtSession) send(b []byte) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	if ss.active {
		ss.sending <- b
	}
}
func (ss *rtSession) redis(insID ...string) (r RedisClient) {
	return ss.server.Redis(insID...)
}
func (ss *rtSession) cache(cacheID ...string) (cache *cache.Cache) {
	return ss.server.Cache(cacheID...)
}

// RealtimeServer is interface hybsRealtimeServer implements
type RealtimeServer interface {
	// Redis returns reference of specified redis connection
	Redis(instanceID ...string) (r RedisClient)
	// Cache returns reference of specified cache instance
	Cache(cacheID ...string) (cache *cache.Cache)
	// SendPacket sends packet to a specified session
	SendPacket(sessionID uint16, out OutPacket)
	// SendPacketMultiple sends packet to multiple sessions
	SendPacketMultiple(sessionIDList []uint16, out OutPacket)
	// BroadcastPacketRoom broadcasts packet to specified room
	BroadcastPacketRoom(roomID uint16, out OutPacket)
	// BroadcastPacketApplication broadcasts packet to specified application
	BroadcastPacketApplication(appID []byte, out OutPacket)
	// BroadcastPacketServer broadcasts packet to all online sessions
	BroadcastPacketServer(out OutPacket)
	// SessionNum returns session number
	SessionNum() int
	// CreateRoom creates new room and returns id
	CreateRoom() (roomID uint16, ok bool)
	// DestroyRoom destroys specified room
	DestroyRoom(roomID uint16)
	// RoomEnterUser lets specified user do enter specified room
	RoomEnterUser(roomID uint16, sessionID uint16) (ok bool)
	// RoomExitUser lets specified user do exit specified room
	RoomExitUser(roomID uint16, sessionID uint16) (ok bool)
	// RoomUserNum returns number of users in the specified room
	RoomUserNum(roomID uint16) int
	// RoomUserList returns user list in the room
	RoomUserList(roomID uint16) (list [][]byte)
	// RoomStatus returns room status information
	RoomStatus(roomID uint16) *RealtimeRoomStatus
	// RoomSetMatch sets auto matching parameters
	// mux is a multiplexer id that avoids data interference
	// scoreCenter, scoreDiff is for builtin matching algorithm
	RoomSetMatch(roomID uint16, mux []byte, scoreCenter float32, scoreDiff float32)
	// RoomSetUserNum sets min/max user num
	RoomSetUserNum(roomID uint16, min uint16, max uint16)
	// Now returns the time when request came at
	Now() Time
}

type rtListenerInterface interface {
	Close() error
	Addr() net.Addr
	Accept(context.Context) (rtConnInterface, error)
}

const (
	rtSessionPoolSizeBit = 15
	rtSessionPoolSize    = 1 << rtSessionPoolSizeBit
)

type rtSessionManager struct {
	sync.RWMutex
	pool      [rtSessionPoolSize]rtSession
	active    map[uint16]*rtSession
	currentID uint16
}

func (sm *rtSessionManager) init(rs *hybsRealtimeServer) {
	for i := uint16(0); i < rtSessionPoolSize; i++ {
		sm.pool[i].id = i
		sm.pool[i].server = rs
		sm.pool[i].loadValue = 5
		sm.pool[i].loadAt = rs.Now()
		sm.pool[i].userBase = &RealtimeUserBase{}
		sm.pool[i].msgBuffer = &rtMessageBuffer{}
		sm.pool[i].active = false
	}
	sm.active = make(map[uint16]*rtSession, rtSessionPoolSize>>1)
	sm.currentID = 1
}
func (sm *rtSessionManager) sessionNum() int {
	return len(sm.active)
}
func (sm *rtSessionManager) isFull() bool {
	return sm.sessionNum() >= rtSessionPoolSize>>1
}
func (sm *rtSessionManager) session(sessionID uint16) *rtSession {
	if sessionID >= rtSessionPoolSize {
		return nil
	}
	if !sm.pool[sessionID].active {
		return nil
	}
	return &sm.pool[sessionID]
}
func (sm *rtSessionManager) create() (session *rtSession, ok bool) {
	if sm.isFull() {
		return nil, false
	}
	sm.Lock()
	defer sm.Unlock()

	for sm.pool[sm.currentID].active {
		sm.currentID = (sm.currentID + 1) & (rtSessionPoolSize - 1)
	}
	session = &sm.pool[sm.currentID]
	session.active = true
	session.userBase = &RealtimeUserBase{}
	session.loadValue = 5
	session.loadAt = session.server.Now()
	session.roomID = nil
	session.roomPos = nil
	session.sending = make(chan []byte, 200)
	sm.active[session.id] = session
	return session, true
}
func (sm *rtSessionManager) destroy(sessionID uint16, err ErrorCode) {
	var session = sm.session(sessionID)
	session.mu.Lock()
	session.active = false
	close(session.sending)
	session.mu.Unlock()
	sm.Lock()
	delete(sm.active, session.id)
	sm.Unlock()
}

type hybsRealtimeServer struct {
	// config setting
	*realtimeConfig
	realtimeControllerConfig
	// engine reference
	engine *hybsEngine
	// core components
	mu                  sync.RWMutex
	listener            rtListenerInterface
	sessionManager      rtSessionManager
	handlerTable        rtHandlerTable
	builtinHandlerTable rtHandlerTable
	onConnected         func(session RealtimeSession)
	onDisconnected      func(session RealtimeSession, errorCode ErrorCode)
	networkByteOrder    binary.ByteOrder
	isClosing           bool
	// room management
	roomTable       rtRoomTable
	roomPool        *sync.Pool
	lastCreatedRoom uint16
	roomNum         int32
	// object pools
	bufferPool     *sync.Pool
	rtCtxValuePool *sync.Pool
	inPacketPool   *sync.Pool
	outPacketPool  *sync.Pool
	defaultModule  rtModule
	// middleware and tools
	logger *logrus.Entry
	// server to server
	httpClient *HttpClient
}

const rtMinHeartbeat = time.Second

func newRealtimeServer(engine *hybsEngine, c realtimeConfig) (newServer *hybsRealtimeServer) {
	if c.Heartbeat < rtMinHeartbeat {
		c.Heartbeat = rtMinHeartbeat
	}
	var newLogger, err = newRealtimeLogger(
		engine.config.LoggerConfig.LogFilePath,
		engine.config.LoggerConfig.RealtimeLogAge)
	if err != nil {
		fmt.Println("init realtime logger failed")
		return nil
	}
	var logEntry = newLogger.(*logrus.Logger).WithContext(nil)
	if engine.config.AppName != "" {
		logEntry = logEntry.WithField("app_name", engine.config.AppName)
	}
	if engine.config.Version != "" {
		logEntry = logEntry.WithField("version", engine.config.Version)
	}
	if engine.config.Env != "" {
		logEntry = logEntry.WithField("env", engine.config.Env)
	}
	if c.ID != "" {
		logEntry = logEntry.WithField("server_id", c.ID)
	}
	newServer = &hybsRealtimeServer{
		engine:                   engine,
		realtimeConfig:           &c,
		realtimeControllerConfig: make(realtimeControllerConfig, 0),
		lastCreatedRoom:          0,
		roomNum:                  0,
		roomPool: &sync.Pool{New: func() interface{} {
			var newRoom = rtRoomInitCreate()
			newRoom.server = newServer
			return newRoom
		}},
		bufferPool: &sync.Pool{New: func() interface{} {
			return make([]byte, udpSocketBufferLength)
		}},
		rtCtxValuePool: &sync.Pool{New: func() interface{} {
			return &rtCtxValueTable{}
		}},
		inPacketPool: &sync.Pool{New: func() interface{} {
			var ret = &rtPacket{server: newServer}
			ret.payload = newServer.bufferPool.Get().([]byte)
			return ret
		}},
		outPacketPool: &sync.Pool{New: func() interface{} {
			var ret = &rtPacket{server: newServer}
			ret.payload = newServer.bufferPool.Get().([]byte)
			return ret
		}},
		networkByteOrder: binary.BigEndian,
		isClosing:        false,
		logger:           logEntry,
		httpClient:       DefaultHttpClient,
	}
	if strings.HasPrefix(c.ByteOrder, "little") {
		newServer.networkByteOrder = binary.LittleEndian
	}
	newServer.httpClient.Timeout = time.Second * 2
	return newServer
}
func (rs *hybsRealtimeServer) setListener(l rtListenerInterface) {
	rs.listener = l
}
func (rs *hybsRealtimeServer) loadControllerFiles(path string) (files []string, err error) {
	fileInfos, err := ioutil.ReadDir(path)
	if err != nil {
		return []string{}, fmt.Errorf("read directory [%s] failed:%s", path, err)
	}
	files = make([]string, 0)
	for _, fileInfo := range fileInfos {
		var fileName = filepath.Join(path, fileInfo.Name())
		if fileInfo.IsDir() {
			appendingFiles, err := rs.loadControllerFiles(filepath.Join(fileName, ""))
			if err != nil {
				return files, err
			}
			files = append(files, appendingFiles...)
			continue
		}
		if !strings.HasSuffix(fileInfo.Name(), ".yml") && !strings.HasSuffix(fileInfo.Name(), ".toml") {
			continue
		}
		files = append(files, fileName)
	}
	return files, nil
}
func (rs *hybsRealtimeServer) loadControllerConfig() (err error) {
	// load controller config files
	var controllerConfigFiles []string
	controllerConfigFiles, err = rs.loadControllerFiles(rs.realtimeConfig.ControllerFilepath)
	if err != nil {
		return err
	}
	var controllerConfigBytes = make([]byte, 0)
	for _, configFile := range controllerConfigFiles {
		var fileBytes []byte
		if fileBytes, err = ioutil.ReadFile(configFile); err != nil {
			return fmt.Errorf("read file %s failed:%s", configFile, err)
		} else {
			controllerConfigBytes = append(controllerConfigBytes, fileBytes...)
		}
	}
	if err = yaml.Unmarshal(controllerConfigBytes, &rs.realtimeControllerConfig); err != nil {
		return fmt.Errorf("unmarshal failed:%s", err)
	}
	return nil
}
func (rs *hybsRealtimeServer) initHandlerMap() (err error) {
	var (
		handlerTree            = rtHandlerTreeCreateInit()
		onConnectedHandlers    = make([]func(ss RealtimeSession), 0, 0x10)
		onDisconnectedHandlers = make([]func(ss RealtimeSession, errorCode ErrorCode), 0, 0x10)
	)
	// build handler tree from controller config files
	for _, controller := range rs.realtimeControllerConfig {
		var code = uint16(0x0000)
		if controller.Code != "" {
			if u64val, err := strconv.ParseUint(controller.Code, 16, 16); err != nil {
				return fmt.Errorf("realtime controller parse uint16 code failed:%s", err)
			} else {
				code = uint16(u64val & 0xffff)
			}
		}
		var bits = uint8(16)
		if controller.Bits != "" {
			if u8val, err := strconv.ParseUint(controller.Bits, 10, 8); err != nil {
				return fmt.Errorf("realtime controller parse uint8 bits failed:%s", err)
			} else {
				bits = uint8(u8val & 0xff)
			}
		}
		var node = handlerTree.peekOrCreate(code, bits)
		if controller.Redis != "" {
			node.setDefaultRedis(controller.Redis)
		}
		if controller.Cache != "" {
			node.setDefaultCache(controller.Cache)
		}
		switch controller.Event {
		case "request":
			for _, handlerDefine := range rtProcessorHandlers[0] {
				if !strings.EqualFold(handlerDefine.id, controller.Handler) {
					continue
				}
				node.insertHandler(handlerDefine.h)
			}
			for _, middlewareID := range controller.Middlewares {
				for _, middlewareDefine := range defaultMiddlewareDefines {
					if !strings.EqualFold(middlewareDefine.id, middlewareID) {
						continue
					}
					node.insertMiddleware(middlewareDefine.m)
				}
			}
		case "connected":
			for _, handlerDefine := range defaultOnConnectedHandlers {
				if !strings.EqualFold(handlerDefine.id, controller.Handler) {
					continue
				}
				onConnectedHandlers = append(onConnectedHandlers, handlerDefine.h)
			}
		case "disconnected":
			for _, handlerDefine := range defaultOnDisconnectedHandlers {
				if !strings.EqualFold(handlerDefine.id, controller.Handler) {
					continue
				}
				onDisconnectedHandlers = append(onDisconnectedHandlers, handlerDefine.h)
			}
		case "timer":
		}
	}
	// insert builtin handlers
	onConnectedHandlers = append(onConnectedHandlers, func(ss RealtimeSession) {
		rtBuiltinSessionEventRoomBroadcastExit(ss.Server(), ss)
	})
	// on connected handler
	if len(onConnectedHandlers) == 0 {
		rs.onConnected = func(session RealtimeSession) {}
	} else if len(onConnectedHandlers) == 1 {
		rs.onConnected = onConnectedHandlers[0]
	} else if len(onConnectedHandlers) > 1 {
		rs.onConnected = func(session RealtimeSession) {
			for i := 0; i < len(onConnectedHandlers); i++ {
				onConnectedHandlers[i](session)
			}
		}
	}
	// on disconnected handler
	if len(onDisconnectedHandlers) == 0 {
		rs.onDisconnected = func(session RealtimeSession, errorCode ErrorCode) {}
	} else if len(onDisconnectedHandlers) == 1 {
		rs.onDisconnected = onDisconnectedHandlers[0]
	} else if len(onDisconnectedHandlers) > 1 {
		rs.onDisconnected = func(session RealtimeSession, errorCode ErrorCode) {
			for i := 0; i < len(onDisconnectedHandlers); i++ {
				onDisconnectedHandlers[i](session, errorCode)
			}
		}
	}
	// register builtin handlers
	rs.registerBuiltinHandlers()
	// store flat handlers into handler table
	for i := 0; i <= 0xffff; i++ {
		rs.handlerTable[i] = handlerTree.search(uint16(i))
	}
	return
}
func (rs *hybsRealtimeServer) registerBuiltinHandlers() {
	rs.builtinHandlerTable[RealtimeRequestCodeUserValue] = rtBuiltinHandlerUserValue
	rs.builtinHandlerTable[RealtimeRequestCodeUserMessage] = rtBuiltinHandlerUserMessage

	rs.builtinHandlerTable[RealtimeRequestCodeRoomCreate] = rtBuiltinHandlerRoomCreate
	rs.builtinHandlerTable[RealtimeRequestCodeRoomEnter] = rtBuiltinHandlerRoomEnter
	rs.builtinHandlerTable[RealtimeRequestCodeRoomExit] = rtBuiltinHandlerRoomExit
	rs.builtinHandlerTable[RealtimeRequestCodeRoomLock] = rtBuiltinHandlerRoomLock
	rs.builtinHandlerTable[RealtimeRequestCodeRoomUnlock] = rtBuiltinHandlerRoomUnlock
	rs.builtinHandlerTable[RealtimeRequestCodeRoomAutoMatch] = rtBuiltinHandlerRoomAutoMatch
}
func (rs *hybsRealtimeServer) currentRoomNum() int32 {
	return atomic.LoadInt32(&rs.roomNum)
}

func (rs *hybsRealtimeServer) serve() (err error) {
	// accept new connections
	go func() {
		for !rs.isClosing {
			if rs.sessionManager.isFull() {
				sleep(1)
				continue
			}
			var ctx = context.Background()
			if conn, err := rs.listener.Accept(ctx); err != nil {
				if err.Error() != "timeout" {
					rs.logger.Warnf("accept socket failed:%s", err)
				}
				continue
			} else if !rs.isClosing {
				go rs.serveConnection(conn)
			}
		}
		rs.debug("realtime services [%s] stopped accepting sessions", rs.ID)
	}()
	// start service loop
	go func() {
		for !rs.isClosing {
			rs.defaultModule.start(rs)
		}
	}()
	return
}

func (rs *hybsRealtimeServer) stop() {
	rs.isClosing = true
	sleep(1)
	// close all modules and sessions
	close(rs.defaultModule)
	for _, activeSession := range rs.sessionManager.active {
		activeSession.rawConn.CloseWithError(ErrorCodeServerStopped)
	}
	return
}

func (rs *hybsRealtimeServer) serveConnection(netConnection rtConnInterface) {
	var stream, err = netConnection.AcceptStream(context.Background())
	if err != nil {
		rs.logger.Warnf(err.Error())
		return
	}
	var session, ok = rs.sessionManager.create()
	if !ok {
		netConnection.CloseWithError(ErrorCodeTooManyConnections)
		return
	}
	// for load control
	session.loadValue = 5
	session.loadAt = rs.Now()
	session.rawConn = netConnection
	session.rawStream = stream
	rs.onConnect(session)
	// write loop
	go func() {
		for bytes := range session.sending {
			session.rawStream.Write(bytes)
		}
		session.Close(ErrorCodeConnectionClosed, "disconnected")
	}()
	for {
		if !session.active {
			break
		}
		if _, err = session.receiveBytes(); err != nil {
			break
		}
	}
	if session.active {
		rs.onDisconnect(session, ErrorCodeConnectionClosed)
	}
}

func (rs *hybsRealtimeServer) onConnect(ss *rtSession) {
	// session connected logic
	rs.onConnected(ss)
	// send session information
	var header, heartbeat, idHigh, idLow byte
	header = rtHeaderPV0 | rtHeaderPDSC | rtHeaderCmdEstablished
	heartbeat = uint8(rs.Heartbeat.Seconds())
	// notify session id
	idHigh, idLow = uint8(ss.id>>8), uint8(ss.id&0xff)
	ss.rawStream.Write([]byte{header, heartbeat, idHigh, idLow})
}
func (rs *hybsRealtimeServer) onDisconnect(ss *rtSession, err ErrorCode) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	// session disconnected logic
	rs.onDisconnected(ss, err)
	// exit room
	if ss.roomID != nil {
		var room = ss.server.roomTable.room(*ss.roomID)
		if room != nil {
			room.userRemove(ss)
			if room.userNum() < 1 {
				ss.server.disposeRoom(room.id)
			}
		}
		ss.roomID = nil
		ss.roomPos = nil
	}
	// destroy session
	rs.sessionManager.destroy(ss.id, err)
}

func (rs *hybsRealtimeServer) session(id uint16) *rtSession {
	return rs.sessionManager.session(id)
}
func (rs *hybsRealtimeServer) serveCtx(ctx *rtCtx) {
	// write log only on debug mode
	if rs.engine.debugMode() {
		var entry = rs.logger.WithField("session", fmt.Sprintf("%x", ctx.sessionID))
		entry = entry.WithField("header", fmt.Sprintf("%x", ctx.in.header))
		entry = entry.WithField("code", fmt.Sprintf("%x", ctx.in.code))
		entry = entry.WithField("len", fmt.Sprintf("%x", ctx.in.length-rtPacketLenSize-rtPacketHeaderSize))
		entry = entry.WithField("payload", fmt.Sprintf("%x", ctx.in.payload))
		entry.Infof("serving packet")
	}
	rs.defaultModule <- ctx
}
func (rs *hybsRealtimeServer) checkRoom(roomID uint16) {
	for rs.isClosing {
		return
	}
	// check room expired
	var room = rs.roomTable.room(roomID)
	if room != nil && room.check() == false {
		rs.disposeRoom(roomID)
	}
}
func (rs *hybsRealtimeServer) createRoom() (roomID uint16, ok bool) {
	if rs.currentRoomNum()&rtRoomNumMask != 0 {
		return 0, false
	}
	atomic.AddInt32(&rs.roomNum, 1)
	var searchRoomIdx = rs.lastCreatedRoom
	for {
		searchRoomIdx = (searchRoomIdx + 1) & (rtRoomTableSize - 1)
		if searchRoomIdx == rs.lastCreatedRoom {
			atomic.AddInt32(&rs.roomNum, -1)
			return 0, false
		}
		if rs.roomTable[searchRoomIdx] == nil {
			break
		}
		rs.checkRoom(searchRoomIdx)
	}
	rs.lastCreatedRoom = searchRoomIdx
	var newRoom = newRealtimeRoom(searchRoomIdx, rs)
	rs.roomTable[searchRoomIdx] = newRoom
	return newRoom.status.ID, true
}
func (rs *hybsRealtimeServer) disposeRoom(roomID uint16) {
	if rs.roomTable[roomID] == nil {
		return
	}
	rs.roomTable[roomID] = nil
}
func (rs *hybsRealtimeServer) doSendPacket(sessionID uint16, out OutPacket) {
	var session = rs.session(sessionID)
	if session == nil {
		return
	}
	var rawPacket = out.(*rtPacket)
	var part1 = []byte{rawPacket.header, uint8(rawPacket.offset >> 8), uint8(rawPacket.offset & 0xff)}
	part1 = append(part1, rawPacket.payload[:rawPacket.offset]...)
	session.send(part1)
}
func (rs *hybsRealtimeServer) debug(format string, args ...interface{}) {
	fmt.Printf(format+"\n", args...)
}
func (rs *hybsRealtimeServer) info(format string, args ...interface{}) {
	rs.logger.Infof(format, args...)
}
func (rs *hybsRealtimeServer) warn(format string, args ...interface{}) {
	rs.logger.Warnf(format, args...)
}
func (rs *hybsRealtimeServer) error(format string, args ...interface{}) {
	rs.logger.Errorf(format, args...)
}

func (rs *hybsRealtimeServer) Now() Time {
	return Now()
}
func (rs *hybsRealtimeServer) SessionNum() int {
	return rs.sessionManager.sessionNum()
}
func (rs *hybsRealtimeServer) KickOff(sessionID uint16) {
	var ss = rs.session(sessionID)
	if ss != nil {
		ss.rawStream.Write([]byte{rtHeaderPV0 | rtHeaderPDSC | rtHeaderCmdClosing})
		rs.onDisconnect(ss, ErrorCodeConnectionClosed)
	}
}
func (rs *hybsRealtimeServer) SendPacket(sessionID uint16, out OutPacket) {
	rs.doSendPacket(sessionID, out)
}
func (rs *hybsRealtimeServer) SendPacketMultiple(sessionIDList []uint16, out OutPacket) {
	for _, sessionID := range sessionIDList {
		rs.doSendPacket(sessionID, out)
	}
}
func (rs *hybsRealtimeServer) BroadcastPacketRoom(roomID uint16, out OutPacket) {
	var room = rs.roomTable[roomID]
	if room == nil {
		return
	}
	for _, roomUser := range room.userTable {
		if roomUser == nil {
			continue
		}
		rs.doSendPacket(roomUser.id, out)
	}
}
func (rs *hybsRealtimeServer) BroadcastPacketApplication(appID []byte, out OutPacket) {
	rs.sessionManager.RLock()
	defer rs.sessionManager.RUnlock()
	for id, ss := range rs.sessionManager.active {
		if !bytes.Equal(appID, ss.AppID()) {
			continue
		}
		rs.doSendPacket(id, out)
	}
}
func (rs *hybsRealtimeServer) BroadcastPacketServer(out OutPacket) {
	rs.sessionManager.RLock()
	defer rs.sessionManager.RUnlock()
	for id, _ := range rs.sessionManager.active {
		rs.doSendPacket(id, out)
	}
}
func (rs *hybsRealtimeServer) CreateRoom() (roomID uint16, ok bool) {
	roomID, ok = rs.createRoom()
	if !ok {
		return 0, false
	}
	return roomID, true
}
func (rs *hybsRealtimeServer) DestroyRoom(roomID uint16) {
	rs.disposeRoom(roomID)
}
func (rs *hybsRealtimeServer) RoomEnterUser(roomID uint16, sessionID uint16) (ok bool) {
	var room = rs.roomTable.room(roomID)
	if room == nil {
		return false
	}
	var session = rs.session(sessionID)
	if session == nil {
		return false
	}
	return room.userAdd(session)
}
func (rs *hybsRealtimeServer) RoomExitUser(roomID uint16, sessionID uint16) (ok bool) {
	var room = rs.roomTable.room(roomID)
	if room == nil {
		return false
	}
	var session = rs.session(sessionID)
	if session == nil {
		return false
	}
	room.userRemove(session)
	if room.userNum() < 1 {
		rs.disposeRoom(room.id)
		return true
	}
	// change room owner
	if session.id == room.owner.id {
		for i := 0; i < len(room.userTable); i++ {
			if room.userTable[i] == nil {
				continue
			}
			room.owner = room.userTable[i]
			break
		}
	}
	return true
}
func (rs *hybsRealtimeServer) RoomUserNum(roomID uint16) int {
	var room = rs.roomTable.room(roomID)
	if room == nil {
		return 0
	}
	return len(room.userTable)
}
func (rs *hybsRealtimeServer) RoomUserList(roomID uint16) (list [][]byte) {
	var room = rs.roomTable.room(roomID)
	if room == nil {
		return [][]byte{}
	}
	return room.UserIDList()
}
func (rs *hybsRealtimeServer) RoomStatus(roomID uint16) *RealtimeRoomStatus {
	var room = rs.roomTable.room(roomID)
	if room == nil {
		return nil
	}
	return room.status
}
func (rs *hybsRealtimeServer) RoomSetMatch(roomID uint16, mux []byte, scoreCenter float32, scoreDiff float32) {
	var room = rs.roomTable.room(roomID)
	if room == nil {
		return
	}
	room.status.MatchMux = make([]byte, len(mux))
	copy(room.status.MatchMux, mux)
	room.status.ScoreCenter = scoreCenter
	room.status.ScoreDiff = scoreDiff
}
func (rs *hybsRealtimeServer) RoomSetUserNum(roomID uint16, min uint16, max uint16) {
	var room = rs.roomTable.room(roomID)
	if room == nil {
		return
	}
	room.status.MinUser = min
	room.status.MaxUser = max
}
func (rs *hybsRealtimeServer) RoomLock(roomID uint16) (ok bool) {
	var room = rs.roomTable.room(roomID)
	if room == nil {
		return false
	}
	if !room.isCanLock() {
		return false
	}
	room.lock()
	return true
}
func (rs *hybsRealtimeServer) RoomUnlock(roomID uint16) (ok bool) {
	var room = rs.roomTable.room(roomID)
	if room == nil {
		return false
	}
	room.unlock()
	return true
}
func (rs *hybsRealtimeServer) Cache(cacheID ...string) (cache *cache.Cache) {
	if len(cacheID) == 0 {
		if len(rs.engine.cacheList) > 0 {
			return rs.engine.cacheList[0].Cache
		}
		return nil
	}
	return rs.engine.Cache(cacheID[0])
}
func (rs *hybsRealtimeServer) Redis(redisID ...string) (client RedisClient) {
	if len(redisID) == 0 {
		if len(rs.engine.redisList) > 0 {
			return rs.engine.redisList[0]
		}
		return nil
	}
	return rs.engine.Redis(redisID[0])
}

var getKcpListenerFn = func(config *realtimeConfig) (rtListenerInterface, error) {
	var l net.Listener = nil
	var err error = nil
	if l, err = kcp.Listen(config.Address); err != nil {
		return nil, err
	}
	return &kcpListener{Listener: l.(*kcp.Listener), config: config}, err
}
var getQuicListenerFn = func(config *realtimeConfig) (rtListenerInterface, error) {
	var cert, err = tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, err
	}
	var listener quic.Listener
	listener, err = quic.ListenAddr(config.Address, &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"hybs-rtv1"},
	}, &quic.Config{})
	if err != nil {
		return nil, err
	}
	return &quicListener{listener}, err
}
