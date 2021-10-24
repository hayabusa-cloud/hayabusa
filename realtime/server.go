package realtime

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/hayabusa-cloud/hayabusa/utils"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hayabusa-cloud/hayabusa/engine"
	"github.com/hayabusa-cloud/hayabusa/plugins"
	"github.com/lucas-clemente/quic-go"
	"github.com/sirupsen/logrus"
	"github.com/xtaci/kcp-go"
	"gopkg.in/yaml.v2"
)

type ServerConfig struct {
	ID                   string        `yaml:"id" required:"true"`
	Network              string        `yaml:"network" default:"udp"`
	Address              string        `yaml:"address" default:"localhost:8443"`
	ByteOrder            string        `yaml:"byte_order" default:"big"`
	CertFile             string        `yaml:"cert_file" default:"cert.pem"`
	KeyFile              string        `yaml:"key_file" default:"key.pem"`
	Password             string        `yaml:"password" default:""`
	Salt                 string        `yaml:"salt" default:""`
	WriteTimeout         time.Duration `yaml:"write_timeout" default:"200ms"`
	ReadTimeout          time.Duration `yaml:"read_timeout" default:"10s"`
	Heartbeat            time.Duration `yaml:"heartbeat" default:"5s"`
	Protocol             string        `yaml:"protocol" default:"kcp"`
	KcpConfig            `yaml:"kcp"`
	QuicConfig           `yaml:"quic"`
	ControllerFilepath   string `yaml:"controller_filepath" required:"true"`
	plugins.LoggerConfig `yaml:"log"`
}
type controllerConfigItem struct {
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
type controllerConfig []controllerConfigItem

const (
	messagePayloadSizeBit  = 14
	messagePayloadSize     = 1 << messagePayloadSizeBit
	messagePayloadSizeMask = messagePayloadSize | (messagePayloadSize << 1)

	messageLenSize    = 2
	messageHeaderSize = 1
	messageCodeSize   = 2
)

type messageObject struct {
	status       uint8
	header       uint8
	length       uint16
	remain       uint16
	payload      []byte
	payloadIndex uint16
}

// Connection is interface connection implements
type Connection interface {
	// Server returns the reference of realtime server instance
	Server() (server Server)
	// Close closes the connection with an error
	Close(code ErrorCode, err string)
	// AppID returns the application id of connection
	AppID() []byte
	// UserID returns the user id(hayabusa id) of connection
	UserID() []byte
	// StringUserID returns the string type user id(hayabusa id) of connection
	StringUserID() string
	// Token returns the access token that connection use
	Token() []byte
}

type connectionInterface interface {
	AcceptStream(context.Context) (streamInterface, error)
	CloseWithError(code ErrorCode) error
}

type streamInterface interface {
	io.Reader
	io.Writer
	Close() error
}

type connection struct {
	// reference
	server *serverImpl
	// network
	mu        sync.RWMutex
	id        uint16
	active    bool
	rawConn   connectionInterface
	rawStream streamInterface
	message   *messageObject
	// limit too many requests
	loadValue   int16
	loadValueAt time.Time
	// authorization
	auth *utils.Authorization

	// room
	roomID  *uint16
	roomPos *uint16

	sending chan []byte
}

const udpSocketBufferLength = 0x4100

func (conn *connection) Server() (server Server) {
	return conn.server
}
func (conn *connection) Close(code ErrorCode, err string) {
	// log err string
	conn.server.logEntry.WithField("err", err).Info("close connection")
	conn.rawConn.CloseWithError(code)
}
func (conn *connection) AppID() []byte {
	keys := conn.auth.AccessKeys()
	if AuthenticationLevelAppManager < len(keys) {
		return keys[AuthenticationLevelAppManager]
	}
	return []byte{}
}
func (conn *connection) UserID() []byte {
	keys := conn.auth.AccessKeys()
	if AuthenticationLevelUser < len(keys) {
		return keys[AuthenticationLevelUser]
	}
	return []byte{}
}
func (conn *connection) StringUserID() string {
	keys := conn.auth.StringAccessKeys()
	if AuthenticationLevelUser < len(keys) {
		return keys[AuthenticationLevelUser]
	}
	return ""
}
func (conn *connection) Token() []byte {
	return conn.auth.Token()
}
func (conn *connection) receiveBytes() (received int, err error) {
	if conn.server.isClosing {
		return 0, nil
	}
	var buf = conn.server.bufferPool.Get().([]byte)
	defer conn.server.bufferPool.Put(&buf)
	if received, err = conn.rawStream.Read(buf); err != nil {
		return 0, err
	}
	conn.loadValue += 1
	if conn.loadValue > 0x1000 {
		var seconds = conn.server.Now().Sub(conn.loadValueAt).Seconds()
		if seconds < 5 {
			conn.server.debug("too many requests:%d", conn.id)
			conn.Close(ErrorCodeTooManyRequests, "too many requests")
		}
		conn.loadValue = 0
		conn.loadValueAt = conn.server.Now()
	}
	// if transport layer protocol is udp or other datagram protocol
	received, err = conn.receiveMessageBytes(buf, received)
	// if transport layer protocol is tcp or other stream protocol
	// received, err = conn.receiveStreamBytes(bytes, received)
	if err != nil {
		return 0, err
	}
	return received, err
}

// *** this method is for datagram protocols only ***
// *** DO NOT use it for stream protocols. ***
// implement of state machine for receiving messages
func (conn *connection) receiveMessageBytes(receivedBytes []byte, received int) (n int, err error) {
	var offset, length = uint16(0), uint16(received)
	for offset < length {
		switch conn.message.status {
		case packetStatusHeader:
			conn.message.header = receivedBytes[offset]
			offset++
			conn.processHeaderDispatch()
			continue
		case packetStatusAuthToken:
			println("packetStatusAuthToken")
			if offset+2 > length {
				return 0, errors.New("bad packet len")
			}
			conn.message.length = (uint16(receivedBytes[offset]) << 8) | uint16(receivedBytes[offset+1])
			offset += 2
			if offset+conn.message.length > length {
				return 0, errors.New("bad auth packet")
			}
			conn.message.payload = receivedBytes[offset : offset+conn.message.length]
			offset += conn.message.length
			conn.processAuthentication()
			conn.message.status = packetStatusHeader
			continue
		case packetStatusNTP:
			if offset+8 > length {
				return 0, errors.New("bad timestamp")
			}
			conn.message.payload = receivedBytes[offset : offset+8]
			offset += 8
			conn.processNTP()
			conn.message.status = packetStatusHeader
			continue
		case packetStatusPayloadLength:
			println("packetStatusPayloadLength")
			if offset+2 > length {
				return 0, errors.New("bad packet len")
			}
			conn.message.length = (uint16(receivedBytes[offset]) << 8) | uint16(receivedBytes[offset+1])
			offset += 2
			if conn.message.length > uint16(messagePayloadSize) {
				return 0, errors.New("too large length")
			}
			conn.message.remain = conn.message.length - messageHeaderSize - messageLenSize
			conn.message.payloadIndex = 0
			conn.message.status = packetStatusPayloadContents
			continue
		case packetStatusPayloadContents:
			println("packetStatusPayloadContents")
			var appendLength = uint16(received&0xffff) - offset
			if conn.message.remain < appendLength {
				appendLength = conn.message.remain
			}
			// here DO NOT copy received buffered bytes
			conn.message.payload = receivedBytes[offset : offset+appendLength]
			offset += appendLength
			conn.message.remain -= appendLength
			conn.message.payloadIndex += appendLength
			if conn.message.remain == 0 {
				conn.processPayload()
				conn.message.payloadIndex = 0
				conn.message.status = packetStatusHeader
			}
			continue
		case packetStatusBroadcastTest:
			println("packetStatusBroadcastTest")
			if conn.server.engine.DebugMode() {
				receivedBytes[offset-1] = headerProtocolVer0 | headerCmdBroadcastTest
				conn.server.connectionManager.RLock()
				for _, dest := range conn.server.connectionManager.active {
					dest.send(receivedBytes[offset-1 : received])
				}
				conn.server.connectionManager.RUnlock()
			}

			offset = length
			conn.message.status = packetStatusHeader
			continue
		case packetStatusEchoTest:
			println("packetStatusEchoTest")
			if conn.server.engine.DebugMode() {
				receivedBytes[offset-1] = headerProtocolVer0 | headerCmdEchoTest
				conn.send(receivedBytes[offset-1 : received])
			}

			offset = length
			conn.message.status = packetStatusHeader
			continue
		default:
		}
	}
	return received, nil
}
func (conn *connection) processHeaderDispatch() {
	// check protocol version
	if (conn.message.header >> 4) != 0 {
		// only support 0 currently
		conn.server.debug("bad protocol version %x", conn.message.header)
		return
	}
	conn.server.debug("received header:[%x]", conn.message.header)
	// internal logic
	var cmd = conn.message.header & 0x0f
	switch cmd {
	case headerCmdHello:
		conn.message.status = packetStatusNTP
	case headerCmdClosing:
		if conn.active {
			conn.server.onDisconnect(conn, ErrorCodeConnectionClosed)
		}
	case headerCmdClosed:
		if conn.active {
			conn.server.onDisconnect(conn, ErrorCodeConnectionClosed)
		}
	case headerCmdEstablished:
		// server -> client only
		break
	case headerCmdAuthenticate:
		conn.message.status = packetStatusAuthToken
	case headerCmdProxy:
		// todo: gate<->server proxy(for builtin server proxy)
		break
	case headerCmdForward:
		// todo: server<->server rpc(for builtin server to server RPC)
		break
	case headerCmdBuiltin:
		conn.message.status = packetStatusPayloadLength
	case headerCmdOriginal:
		conn.message.status = packetStatusPayloadLength
	case headerCmdBroadcastTest:
		conn.message.status = packetStatusBroadcastTest
	case headerCmdEchoTest:
		conn.message.status = packetStatusEchoTest
	}
}

// implement of builtin NTP
func (conn *connection) processNTP() {
	var out = conn.server.responsePool.Get().(*message)
	defer conn.server.responsePool.Put(out)
	out.payload[0] = headerProtocolVer0 | headerCmdNTP
	copy(out.payload[1:], conn.message.payload)
	_, err := conn.rawStream.Write(out.payload[:1+8+8])
	if err != nil {
		conn.server.warn("send NTP: %s", err)
	}
}

func (conn *connection) processAuthentication() {
	// here does basic authentication(not authorization) only
	cred := utils.CredentialChainFromToken(conn.message.payload)
	conn.auth = utils.NewAuthorization(cred)
	if err := conn.auth.PushPermission(RolePermissionAuthorized); err != nil {
		conn.server.warn("push permission failed: %s", err)
	}
	conn.server.info("user %s authenticated", cred.StringID())
	// to implement authorization in application layer
}
func (conn *connection) processPayload() {
	// context data table
	var ctxValue = conn.server.ctxValuePool.Get().(*ctxValueTable)
	// received data packet
	var in = conn.server.requestPool.Get().(*message)
	// get reused sending packet from pool
	var out = conn.server.responsePool.Get().(*message)
	in.header = conn.message.header
	// packet=header(1byte)+payload length(2bytes)+payload(n bytes)
	in.length = conn.message.length - messageLenSize - messageHeaderSize
	// payload=request code(2bytes)+body(m bytes)
	in.offset = messageCodeSize
	in.code = 0
	// to support multi-processors(goroutines) in the future, we should COPY the message
	// was in.payload = conn.msgBuffer.payload[:conn.msgBuffer.payloadIndex]
	copy(in.payload, conn.message.payload[:conn.message.payloadIndex])
	out.offset = messageCodeSize
	if len(in.payload) >= messageCodeSize {
		in.code = (uint16(in.payload[0]) << 8) | uint16(in.payload[1])
	}
	// set context of logger
	var logEntry = conn.server.logEntry.WithField("connection", fmt.Sprintf("%x", conn.id))
	logEntry = logEntry.WithField("code", fmt.Sprintf("%x", in.code))
	var newCtx = &ctx{
		// core component
		server:   conn.server,
		connID:   conn.id,
		request:  in,
		response: out,
		// context
		value:         ctxValue,
		defaultRedis:  "",
		defaultCache:  "",
		defaultCsvMux: "",
		// log
		logEntry: logEntry,
	}
	conn.server.serveCtx(newCtx)
}
func (conn *connection) send(b []byte) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	fmt.Printf("send: %x\n", b)
	if conn.active {
		conn.sending <- b
	}
}

//lint:ignore U1000 ignore unused
func (conn *connection) redis(id string) (plugin *plugins.Redis) {
	return conn.server.Redis(id)
}

//lint:ignore U1000 ignore unused
func (conn *connection) cache(id string) (plugin *plugins.Cache) {
	return conn.server.Cache(id)
}

// Server is interface server implements
type Server interface {
	// Redis returns reference of specified redis connection
	Redis(instanceID string) (plugin *plugins.Redis)
	// Cache returns reference of specified cache instance
	Cache(cacheID string) (plugin *plugins.Cache)
	// SendMessage sends packet to a specified connection
	SendMessage(connID uint16, out Response)
	// SendMessageMultiple sends packet to multiple connections
	SendMessageMultiple(connectionIDList []uint16, out Response)
	// BroadcastMessageRoom broadcasts packet to specified room
	BroadcastMessageRoom(roomID uint16, out Response)
	// BroadcastMessageApplication broadcasts packet to specified application
	BroadcastMessageApplication(appID []byte, out Response)
	// BroadcastMessageServer broadcasts packet to all online connections
	BroadcastMessageServer(out Response)
	// ConnectionNum returns connection num
	ConnectionNum() int
	// CreateRoom creates new room and returns id
	CreateRoom() (roomID uint16, ok bool)
	// DestroyRoom destroys specified room
	DestroyRoom(roomID uint16)
	// RoomEnterUser lets specified user do enter specified room
	RoomEnterUser(roomID uint16, connectionID uint16) (ok bool)
	// RoomExitUser lets specified user do exit specified room
	RoomExitUser(roomID uint16, connectionID uint16) (ok bool)
	// RoomUserNum returns number of useserver in the specified room
	RoomUserNum(roomID uint16) int
	// RoomUserList returns user list in the room
	RoomUserList(roomID uint16) (list [][]byte)
	// RoomStatus returns room status information
	RoomStatus(roomID uint16) *RoomStatus
	// RoomSetMatch sets auto matching parameters
	// mux is a multiplexer id that avoids data interference
	// scoreCenter, scoreVariance is for builtin matching algorithm
	RoomSetMatch(roomID uint16, mux []byte, scoreCenter float32, scoreVariance float32)
	// RoomSetUserNum sets min/max user num
	RoomSetUserNum(roomID uint16, num uint16)
	// Now returns the time when request came at
	Now() time.Time
}

type Listener interface {
	Close() error
	Addr() net.Addr
	Accept(context.Context) (connectionInterface, error)
}

type serverImpl struct {
	// config setting
	config      *ServerConfig
	controllers controllerConfig
	// core components
	engine              engine.Interface
	mu                  sync.RWMutex
	listener            Listener
	connectionManager   connectionManager
	handlerTable        handlerTable
	builtinHandlerTable handlerTable
	onConnected         func(conn Connection)
	onDisconnected      func(conn Connection, errorCode ErrorCode)
	networkByteOrder    binary.ByteOrder
	isClosing           bool
	// room management
	roomTable       roomTable
	roomPool        *sync.Pool
	lastCreatedRoom uint16
	roomNum         int32
	// object pools
	bufferPool    *sync.Pool
	ctxValuePool  *sync.Pool
	requestPool   *sync.Pool
	responsePool  *sync.Pool
	mainProcessor processor
	// todo: roomProcessors []processor

	logEntry *logrus.Entry
}

func NewServer(engine engine.Interface, listener Listener, c ServerConfig) (newServer *serverImpl, err error) {
	if c.Heartbeat < minHeartbeatInterval {
		c.Heartbeat = minHeartbeatInterval
	}
	var newLogger plugins.Logger = nil
	newLogger, err = plugins.NewLogger(c.LoggerConfig)
	if err != nil {
		fmt.Println("create realtime logger error")
		return nil, fmt.Errorf("create realtime logger error")
	}
	var logEntry = newLogger.(*logrus.Entry).WithContext(engine.Context())
	if engine.AppName() != "" {
		logEntry = logEntry.WithField("app_name", engine.AppName())
	}
	if engine.Version() != "" {
		logEntry = logEntry.WithField("version", engine.Version())
	}
	if engine.Env() != "" {
		logEntry = logEntry.WithField("env", engine.Env())
	}
	if c.ID != "" {
		logEntry = logEntry.WithField("server_id", c.ID)
	}
	newServer = &serverImpl{
		engine:          engine,
		listener:        listener,
		config:          &c,
		controllers:     make(controllerConfig, 0),
		lastCreatedRoom: 0,
		roomNum:         0,
		roomPool: &sync.Pool{New: func() interface{} {
			var newRoom = createRoom(newServer)
			newRoom.server = newServer
			return newRoom
		}},
		bufferPool: &sync.Pool{New: func() interface{} {
			return make([]byte, udpSocketBufferLength)
		}},
		ctxValuePool: &sync.Pool{New: func() interface{} {
			return &ctxValueTable{}
		}},
		requestPool: &sync.Pool{New: func() interface{} {
			var ret = &message{server: newServer}
			ret.payload = newServer.bufferPool.Get().([]byte)
			return ret
		}},
		responsePool: &sync.Pool{New: func() interface{} {
			var ret = &message{server: newServer}
			ret.payload = newServer.bufferPool.Get().([]byte)
			return ret
		}},
		networkByteOrder: binary.BigEndian,
		isClosing:        false,
		logEntry:         logEntry,
	}
	if strings.HasPrefix(c.ByteOrder, "little") {
		newServer.networkByteOrder = binary.LittleEndian
		fmt.Println("set network byte order=little endian")
	}
	if err = newServer.loadControllerConfig(); err != nil {
		return nil, fmt.Errorf("load controllers error:%s", err)
	}
	if err = newServer.registerHandlers(); err != nil {
		return nil, fmt.Errorf("register handlers error:%s", err)
	}
	newServer.connectionManager.init(newServer)
	newServer.mainProcessor = newProcessor()
	return newServer, nil
}

func (server *serverImpl) ID() string {
	return server.config.ID
}

func (server *serverImpl) loadControllerFiles(path string) (files []string, err error) {
	fileInfos, err := ioutil.ReadDir(path)
	if err != nil {
		return []string{}, fmt.Errorf("read directory [%s] error:%s", path, err)
	}
	files = make([]string, 0)
	for _, fileInfo := range fileInfos {
		var fileName = filepath.Join(path, fileInfo.Name())
		if fileInfo.IsDir() {
			appendingFiles, err := server.loadControllerFiles(filepath.Join(fileName, ""))
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
func (server *serverImpl) loadControllerConfig() (err error) {
	// load controller config files
	var controllerConfigFiles []string
	controllerConfigFiles, err = server.loadControllerFiles(server.config.ControllerFilepath)
	if err != nil {
		return err
	}
	var controllerConfigBytes = make([]byte, 0)
	for _, configFile := range controllerConfigFiles {
		var fileBytes []byte
		if fileBytes, err = ioutil.ReadFile(configFile); err != nil {
			return fmt.Errorf("read file %s error:%s", configFile, err)
		} else {
			controllerConfigBytes = append(controllerConfigBytes, fileBytes...)
		}
	}
	if err = yaml.Unmarshal(controllerConfigBytes, &server.controllers); err != nil {
		return fmt.Errorf("unmarshal error:%s", err)
	}
	return nil
}
func (server *serverImpl) registerHandlers() (err error) {
	var (
		newHandlerTree         = createNewHandlerTree()
		onConnectedHandlers    = make([]func(conn Connection), 0, 0x10)
		onDisconnectedHandlers = make([]func(conn Connection, errorCode ErrorCode), 0, 0x10)
	)
	// build handler tree from controller config files
	for _, controller := range server.controllers {
		var code = uint16(0x0000)
		if controller.Code != "" {
			if u64val, err := strconv.ParseUint(controller.Code, 16, 16); err != nil {
				return fmt.Errorf("realtime controller paserver. uint16 code error:%s", err)
			} else {
				code = uint16(u64val & 0xffff)
			}
		}
		var bits = uint8(16)
		if controller.Bits != "" {
			if u8val, err := strconv.ParseUint(controller.Bits, 10, 8); err != nil {
				return fmt.Errorf("realtime controller paserver. uint8 bits error:%s", err)
			} else {
				bits = uint8(u8val & 0xff)
			}
		}
		var node = newHandlerTree.peekOrCreate(code, bits)
		if controller.Redis != "" {
			node.setDefaultRedis(controller.Redis)
		}
		if controller.Cache != "" {
			node.setDefaultCache(controller.Cache)
		}
		if controller.CsvMux != "" {
			node.setDefaultCsvMux(controller.CsvMux)
		}
		switch controller.Event {
		case "request":
			for _, handlerDefine := range processorHandlers[0] {
				if !strings.EqualFold(handlerDefine.id, controller.Handler) {
					continue
				}
				node.insertHandler(handlerDefine.h)
			}
			for _, middlewareID := range controller.Middlewares {
				for _, middlewareDefine := range defaultMiddlewareTupleList {
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
		}
	}
	// insert builtin handlers
	onConnectedHandlers = append(onConnectedHandlers, func(conn Connection) {
		builtinEventRoomBroadcastExit(conn.Server(), conn)
	})
	// on connected handler
	if len(onConnectedHandlers) == 0 {
		server.onConnected = func(connection Connection) {}
	} else if len(onConnectedHandlers) == 1 {
		server.onConnected = onConnectedHandlers[0]
	} else if len(onConnectedHandlers) > 1 {
		server.onConnected = func(connection Connection) {
			for i := 0; i < len(onConnectedHandlers); i++ {
				onConnectedHandlers[i](connection)
			}
		}
	}
	// on disconnected handler
	if len(onDisconnectedHandlers) == 0 {
		server.onDisconnected = func(connection Connection, errorCode ErrorCode) {}
	} else if len(onDisconnectedHandlers) == 1 {
		server.onDisconnected = onDisconnectedHandlers[0]
	} else if len(onDisconnectedHandlers) > 1 {
		server.onDisconnected = func(connection Connection, errorCode ErrorCode) {
			for i := 0; i < len(onDisconnectedHandlers); i++ {
				onDisconnectedHandlers[i](connection, errorCode)
			}
		}
	}
	// register builtin handlers
	server.registerBuiltinHandlers()
	// store raw handlers into handler table
	for i := 0; i <= 0xffff; i++ {
		server.handlerTable[i] = newHandlerTree.search(uint16(i))
	}
	return
}
func (server *serverImpl) registerBuiltinHandlers() {
	server.builtinHandlerTable[RequestCodeUserValue] = builtinHandlerUserValue
	server.builtinHandlerTable[RequestCodeUserMessage] = builtinHandlerUserMessage

	server.builtinHandlerTable[RequestCodeRoomCreate] = builtinHandlerRoomCreate
	server.builtinHandlerTable[RequestCodeRoomEnter] = builtinHandlerRoomEnter
	server.builtinHandlerTable[RequestCodeRoomExit] = builtinHandlerRoomExit
	server.builtinHandlerTable[RequestCodeRoomLock] = builtinHandlerRoomLock
	server.builtinHandlerTable[RequestCodeRoomUnlock] = builtinHandlerRoomUnlock
	server.builtinHandlerTable[RequestCodeRoomAutoMatch] = builtinHandlerRoomAutoMatch
}
func (server *serverImpl) currentRoomNum() int32 {
	return atomic.LoadInt32(&server.roomNum)
}

// Serve is non-blocking
func (server *serverImpl) Serve(ctx context.Context) (err error) {
	// accept new connections(although udp is a datagram protocol, we say "connection" here)
	go func() {
		for !server.isClosing {
			if server.connectionManager.isFull() {
				sleep(1)
				continue
			}
			if conn, err := server.listener.Accept(ctx); err != nil {
				if err.Error() != "timeout" {
					server.logEntry.Warnf("accept socket error:%s", err)
				}
				continue
			} else if !server.isClosing {
				go server.serveConnection(conn)
			}
		}
		server.debug("realtime services [%s] stopped accepting connections", server.config.ID)
	}()
	// main-loop
	go func() {
		for !server.isClosing {
			server.mainProcessor.start(server)
		}
	}()
	fmt.Printf("realtime server [%s] started, listening on %s://%s\n", server.config.ID, server.config.Network, server.config.Address)
	return nil
}

func (server *serverImpl) Stop() {
	server.isClosing = true
	sleep(1)
	// close all processors and connections
	close(server.mainProcessor)
	for _, activeConnection := range server.connectionManager.active {
		if err := activeConnection.rawConn.CloseWithError(ErrorCodeServerStopped); err != nil {
			server.info("close conn id=%x failed: %s", activeConnection.id, err)
		}
	}
}

func (server *serverImpl) serveConnection(netConnection connectionInterface) {
	var stream, err = netConnection.AcceptStream(context.Background())
	if err != nil {
		server.warn("accept stream failed: %s", err)
		return
	}
	var conn, ok = server.connectionManager.create()
	if !ok {
		err = netConnection.CloseWithError(ErrorCodeTooManyConnections)
		if err != nil {
			server.error("close with error failed: %s", err)
		}
		return
	}
	// load controlling
	conn.loadValue = 5
	conn.loadValueAt = server.Now()
	conn.rawConn = netConnection
	conn.rawStream = stream
	server.onConnect(conn)
	// write loop
	go func() {
		for sendingBytes := range conn.sending {
			_, err = conn.rawStream.Write(sendingBytes)
			if err != nil {
				server.warn("send bytes failed: %s", err)
			}
		}
		conn.Close(ErrorCodeConnectionClosed, "disconnected")
	}()
	for {
		if !conn.active {
			break
		}
		if _, err = conn.receiveBytes(); err != nil {
			break
		}
	}
	if conn.active {
		server.onDisconnect(conn, ErrorCodeConnectionClosed)
	}
}

func (server *serverImpl) onConnect(conn *connection) {
	// connection connected logic
	server.onConnected(conn)
	// send connection information
	var header, heartbeat, idHigh, idLow byte
	header = headerProtocolVer0 | headerCmdEstablished
	heartbeat = uint8(server.config.Heartbeat.Seconds())
	// notify connection id
	idHigh, idLow = uint8(conn.id>>8), uint8(conn.id&0xff)
	if _, err := conn.rawStream.Write([]byte{header, heartbeat, idHigh, idLow}); err != nil {
		server.warn("raw stream write failed: %s", err)
	}
}
func (server *serverImpl) onDisconnect(conn *connection, err ErrorCode) {
	server.mu.Lock()
	defer server.mu.Unlock()
	// connection disconnected logic
	server.onDisconnected(conn, err)
	// exit room
	if conn.roomID != nil {
		var r = conn.server.roomTable.room(*conn.roomID)
		if r != nil {
			r.userRemove(conn)
			if r.userNum() < 1 {
				conn.server.disposeRoom(r.id)
			}
		}
		conn.roomID = nil
		conn.roomPos = nil
	}
	// destroy connection
	server.connectionManager.destroy(conn.id, err)
}

func (server *serverImpl) connection(id uint16) *connection {
	return server.connectionManager.connection(id)
}
func (server *serverImpl) serveCtx(ctx *ctx) {
	// write log only on debug mode
	if server.engine.DebugMode() {
		var entry = server.logEntry.WithField("connection", fmt.Sprintf("%x", ctx.connID))
		entry = entry.WithField("header", fmt.Sprintf("%x", ctx.request.header))
		entry = entry.WithField("code", fmt.Sprintf("%x", ctx.request.code))
		entry = entry.WithField("len", fmt.Sprintf("%x", ctx.request.length-messageLenSize-messageHeaderSize))
		entry = entry.WithField("payload", fmt.Sprintf("%x", ctx.request.payload))
		entry.Infof("serving request")
	}
	server.mainProcessor <- ctx
}
func (server *serverImpl) checkRoom(roomID uint16) {
	for server.isClosing {
		return
	}
	var r = server.roomTable.room(roomID)
	if r != nil && !r.check() {
		server.disposeRoom(roomID)
	}
}
func (server *serverImpl) createRoom() (roomID uint16, ok bool) {
	if server.currentRoomNum()&roomNumMask != 0 {
		return 0, false
	}
	atomic.AddInt32(&server.roomNum, 1)
	var searchRoomIdx = server.lastCreatedRoom
	for {
		searchRoomIdx = (searchRoomIdx + 1) & (roomTableSize - 1)
		if searchRoomIdx == server.lastCreatedRoom {
			atomic.AddInt32(&server.roomNum, -1)
			return 0, false
		}
		if server.roomTable[searchRoomIdx] == nil {
			break
		}
		server.checkRoom(searchRoomIdx)
	}
	server.lastCreatedRoom = searchRoomIdx
	var newRoom = newRealtimeRoom(searchRoomIdx, server)
	server.roomTable[searchRoomIdx] = newRoom
	server.info("room id=%x created", roomID)
	return newRoom.status.ID, true
}
func (server *serverImpl) disposeRoom(roomID uint16) {
	if server.roomTable[roomID] == nil {
		return
	}
	server.roomTable[roomID] = nil
}
func (server *serverImpl) doSendMessage(connectionID uint16, out Response) {
	var conn = server.connection(connectionID)
	if conn == nil {
		return
	}
	var rawMessage = out.(*message)
	var metadata = []byte{rawMessage.header, uint8(rawMessage.offset >> 8), uint8(rawMessage.offset & 0xff)}
	rawBytes := append(metadata, rawMessage.payload[:rawMessage.offset]...)
	conn.send(rawBytes)
}
func (server *serverImpl) debug(format string, args ...interface{}) {
	fmt.Printf(format+"\n", args...)
}
func (server *serverImpl) info(format string, args ...interface{}) {
	server.logEntry.Infof(format, args...)
}
func (server *serverImpl) warn(format string, args ...interface{}) {
	server.logEntry.Warnf(format, args...)
}
func (server *serverImpl) error(format string, args ...interface{}) {
	server.logEntry.Errorf(format, args...)
}

func (server *serverImpl) Now() time.Time {
	return server.engine.Now()
}
func (server *serverImpl) ConnectionNum() int {
	return server.connectionManager.connectionNum()
}
func (server *serverImpl) KickOff(connectionID uint16) {
	var conn = server.connection(connectionID)
	if conn != nil {
		if _, err := conn.rawStream.Write([]byte{headerProtocolVer0 | headerCmdClosing}); err != nil {
			server.warn("raw stream write failed: %s", err)
		}
		server.onDisconnect(conn, ErrorCodeConnectionClosed)
	}
}
func (server *serverImpl) SendMessage(connectionID uint16, out Response) {
	server.doSendMessage(connectionID, out)
}
func (server *serverImpl) SendMessageMultiple(connectionIDList []uint16, out Response) {
	for _, connectionID := range connectionIDList {
		server.doSendMessage(connectionID, out)
	}
}
func (server *serverImpl) BroadcastMessageRoom(roomID uint16, out Response) {
	var r = server.roomTable[roomID]
	if r == nil {
		return
	}
	for _, ru := range r.userTable {
		if ru == nil {
			continue
		}
		server.doSendMessage(ru.conn.id, out)
	}
}
func (server *serverImpl) BroadcastMessageApplication(appID []byte, out Response) {
	server.connectionManager.RLock()
	defer server.connectionManager.RUnlock()
	for id, conn := range server.connectionManager.active {
		if !bytes.Equal(appID, conn.AppID()) {
			continue
		}
		server.doSendMessage(id, out)
	}
}
func (server *serverImpl) BroadcastMessageServer(out Response) {
	server.connectionManager.RLock()
	defer server.connectionManager.RUnlock()
	for id := range server.connectionManager.active {
		server.doSendMessage(id, out)
	}
}
func (server *serverImpl) CreateRoom() (roomID uint16, ok bool) {
	roomID, ok = server.createRoom()
	if !ok {
		return 0, false
	}
	return roomID, true
}
func (server *serverImpl) DestroyRoom(roomID uint16) {
	server.disposeRoom(roomID)
}
func (server *serverImpl) RoomEnterUser(roomID uint16, connectionID uint16) (ok bool) {
	var r = server.roomTable.room(roomID)
	if r == nil {
		return false
	}
	var conn = server.connection(connectionID)
	if conn == nil {
		return false
	}
	return r.userAdd(conn)
}
func (server *serverImpl) RoomExitUser(roomID uint16, connectionID uint16) (ok bool) {
	var r = server.roomTable.room(roomID)
	if r == nil {
		return false
	}
	var conn = server.connection(connectionID)
	if conn == nil {
		return false
	}
	r.userRemove(conn)
	if r.userNum() < 1 {
		server.disposeRoom(r.id)
		return true
	}
	// change r owner
	if conn.id == r.owner.conn.id {
		for i := 0; i < len(r.userTable); i++ {
			if r.userTable[i] == nil {
				continue
			}
			r.owner = r.userTable[i]
			break
		}
	}
	return true
}
func (server *serverImpl) RoomUserNum(roomID uint16) int {
	var r = server.roomTable.room(roomID)
	if r == nil {
		return 0
	}
	return len(r.userTable)
}
func (server *serverImpl) RoomUserList(roomID uint16) (list [][]byte) {
	var r = server.roomTable.room(roomID)
	if r == nil {
		return [][]byte{}
	}
	return r.UserIDList()
}
func (server *serverImpl) RoomStatus(roomID uint16) *RoomStatus {
	var r = server.roomTable.room(roomID)
	if r == nil {
		return nil
	}
	return r.status
}
func (server *serverImpl) RoomSetMatch(roomID uint16, mux []byte, scoreCenter float32, scoreDiff float32) {
	var r = server.roomTable.room(roomID)
	if r == nil {
		return
	}
	r.status.Mux = make([]byte, len(mux))
	copy(r.status.Mux, mux)
	r.status.ScoreCenter = scoreCenter
	r.status.ScoreVariance = scoreDiff
}
func (server *serverImpl) RoomSetUserNum(roomID uint16, userNum uint16) {
	var r = server.roomTable.room(roomID)
	if r == nil {
		return
	}
	r.status.UserNum = userNum
}
func (server *serverImpl) RoomLock(roomID uint16) (ok bool) {
	var r = server.roomTable.room(roomID)
	if r == nil {
		return false
	}
	if !r.isCanLock() {
		return false
	}
	r.lock()
	return true
}
func (server *serverImpl) RoomUnlock(roomID uint16) (ok bool) {
	var r = server.roomTable.room(roomID)
	if r == nil {
		return false
	}
	r.unlock()
	return true
}
func (server *serverImpl) Cache(cacheID string) (cache *plugins.Cache) {
	return server.engine.Cache(cacheID)
}
func (server *serverImpl) Redis(redisID string) (client *plugins.Redis) {
	return server.engine.Redis(redisID)
}

//lint:ignore U1000 reserving
var getKcpListenerFn = func(config *ServerConfig) (Listener, error) {
	var l net.Listener = nil
	var err error = nil
	if l, err = kcp.Listen(config.Address); err != nil {
		return nil, err
	}
	return &KcpListener{Listener: l.(*kcp.Listener), Config: config}, err
}

//lint:ignore U1000 reserving
var getQuicListenerFn = func(config *ServerConfig) (Listener, error) {
	var cert, err = tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, err
	}
	var listener quic.Listener
	listener, err = quic.ListenAddr(config.Address, &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"hayabusa-v0"},
	}, &quic.Config{})
	if err != nil {
		return nil, err
	}
	return &QuicListener{listener}, err
}

var sleepCnt = uint8(0)

func sleep(frame ...int) {
	if len(frame) > 0 && frame[0] > 0 {
		time.Sleep(time.Millisecond * 10 * time.Duration(frame[0]))
	} else {
		sleepCnt++
		if sleepCnt&0x3f == 0 {
			time.Sleep(time.Millisecond * 10)
		}
	}
}
