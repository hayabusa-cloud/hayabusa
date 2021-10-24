package realtime

import (
	"fmt"
	"math"
	"time"

	"github.com/hayabusa-cloud/hayabusa/plugins"
	"github.com/sirupsen/logrus"
)

// Ctx is interface ctx implements
type Ctx interface {
	// Request is the received requested packet
	Request
	// Response is the sending response packet
	Response
	// Server returns reference of realtime server
	Server() Server
	// AppID returns using application id
	AppID() []byte
	// UserID returns the id of the request source user
	UserID() []byte
	// Token returns the token of the request source user
	Token() []byte
	// ConnectionID returns the connection id of the request source user
	ConnectionID() uint16
	// Header returns request header
	Header() uint8
	// EventCode returns request event code
	EventCode() uint16
	// Payload returns request payload body
	Payload() []byte
	// OutMessage returns default sending message
	OutMessage() Response
	// Cache returns reference of cache instance
	Cache(cacheID ...string) (plugin *plugins.Cache)
	// Redis returns reference of redis connection
	Redis(instanceID ...string) (plugin *plugins.Redis)
	// CsvTable find and returns csv table under given table name
	CsvTable(tableName string, args ...string) plugins.MasterTable
	// GetConnection find and returns connection by connection id
	GetConnection(ConnectionID uint16) Connection
	// CloseConnection closes the connection in used with error code
	CloseConnection(code ErrorCode, err string)
	// IsInRoom returns true when current user is in a room
	IsInRoom() bool
	// CurrentRoomID returns the current room id
	CurrentRoomID() uint16
	// Send sends packet to specified user
	Send(destConnID uint16, out ...Response) Ctx
	// BroadcastServer broadcasts packet to all online users
	BroadcastServer(out ...Response) Ctx
	// BroadcastApplication broadcasts packet to users in the same application
	BroadcastApplication(out ...Response) Ctx
	// BroadcastRoom broadcasts packet to users in the same room
	BroadcastRoom(out ...Response) Ctx
	// Authorization returns stored user authorization information
	Authorization() interface{}
	// SetAuthorization set and stores user authorization information
	SetAuthorization(authorization interface{}) Ctx
	// ValueTable returns user original parameters table
	ValueTable() CtxValueTable
	// Debug writes debug level log
	Debug(format string, args ...interface{}) Ctx
	// Infof writes info level log
	Infof(format string, args ...interface{}) Ctx
	// Warnf writes warn level log
	Warnf(format string, args ...interface{}) Ctx
	// Assert throws a fatal error if ok is not true
	Assert(ok bool, format string, args ...interface{}) Ctx
	// ErrorClientRequest responses client side error: bad request, unauthorized, forbidden etc.
	ErrorClientRequest(errorCode uint16, format string, args ...interface{})
	// ErrorServerInternal responses server side internal error
	ErrorServerInternal(format string, args ...interface{})
	// Now returns the time when request came
	Now() time.Time
}

// Request is interface that received binary data packet implements
type Request interface {
	ReadBool(val *bool) Request
	ReadInt8(val *int8) Request
	ReadUint8(val *uint8) Request
	ReadInt16(val *int16) Request
	ReadUint16(val *uint16) Request
	ReadInt32(val *int32) Request
	ReadUint32(val *uint32) Request
	ReadInt64(val *int64) Request
	ReadUint64(val *uint64) Request
	ReadFloat32(val *float32) Request
	ReadFloat64(val *float64) Request
	ReadBytes() []byte
	ReadString() string
	Payload() []byte
}

// Response is interface that sending binary data packet implements
type Response interface {
	WriteBool(val bool) Response
	WriteInt8(val int8) Response
	WriteUint8(val uint8) Response
	WriteInt16(val int16) Response
	WriteUint16(val uint16) Response
	WriteInt32(val int32) Response
	WriteUint32(val uint32) Response
	WriteInt64(val int64) Response
	WriteUint64(val uint64) Response
	WriteFloat32(val float32) Response
	WriteFloat64(val float64) Response
	WriteBytes(val []byte) Response
	WriteString(val string) Response
	WriteBytesNoLen(val []byte) Response

	WriteRoom(roomID uint16) Response
	WriteUserStatus(connectionID uint16) Response
	SetHeader(header uint8) Response
	SetHeader2(protocolVer, cmd uint8) Response
	SetEventCode(code uint16) Response
	Reset() Response
}

const ctxValueTableSize = 1 << 4

type ctxValueTable [ctxValueTableSize]interface{}

// CtxValueTable is interface ctxValueTable implements
type CtxValueTable interface {
	Set(key int, value interface{}) CtxValueTable
	GetInt(key int, def ...int) int
	GetUint(key int, def ...uint) uint
	GetInt16(key int, def ...int16) int16
	GetUint16(key int, def ...uint16) uint16
	GetByte(key int, def ...byte) byte
	GetBool(key int, def ...bool) bool
	GetFloat32(key int, def ...float32) float32
	GetFloat64(key int, def ...float64) float64
	GetBytes(key int) []byte
	GetString(key int, def ...string) string
}

func (t *ctxValueTable) Set(key int, value interface{}) CtxValueTable {
	if key >= ctxValueTableSize {
		panic("bad ctxValue index")
	}
	if key < 0 {
		panic("bad ctxValue index")
	}
	t[key] = value
	return t
}
func (t *ctxValueTable) GetInt(key int, def ...int) int {
	var val, ok = t[key].(int)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *ctxValueTable) GetUint(key int, def ...uint) uint {
	var val, ok = t[key].(uint)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *ctxValueTable) GetInt16(key int, def ...int16) int16 {
	var val, ok = t[key].(int16)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *ctxValueTable) GetUint16(key int, def ...uint16) uint16 {
	var val, ok = t[key].(uint16)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *ctxValueTable) GetByte(key int, def ...byte) byte {
	var val, ok = t[key].(byte)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *ctxValueTable) GetBool(key int, def ...bool) bool {
	var val, ok = t[key].(bool)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return false
	}
	return val
}
func (t *ctxValueTable) GetFloat32(key int, def ...float32) float32 {
	var val, ok = t[key].(float32)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *ctxValueTable) GetFloat64(key int, def ...float64) float64 {
	var val, ok = t[key].(float64)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *ctxValueTable) GetBytes(key int) []byte {
	var val = t[key]
	if val == nil {
		return nil
	}
	var bytes, ok = val.([]byte)
	if !ok {
		return nil
	}
	return bytes
}
func (t *ctxValueTable) GetString(key int, def ...string) string {
	var val, ok = t[key].(string)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return ""
	}
	return val
}

type ctx struct {
	// core component
	server   *serverImpl
	connID   uint16
	request  *message
	response *message
	// context
	value         *ctxValueTable
	authorization interface{}
	defaultRedis  string
	defaultCache  string
	defaultCsvMux string
	// log
	logEntry *logrus.Entry
}

type message struct {
	header  uint8
	length  uint16
	offset  uint16
	code    uint16
	payload []uint8
	server  *serverImpl
}

func (ctx *ctx) Server() Server {
	return ctx.server
}
func (ctx *ctx) AppID() []byte {
	var ss = ctx.GetConnection(ctx.connID)
	if ss == nil {
		return []byte{}
	}
	return ss.AppID()
}
func (ctx *ctx) UserID() []byte {
	var ss = ctx.GetConnection(ctx.connID)
	if ss == nil {
		return []byte{}
	}
	return ss.UserID()
}
func (ctx *ctx) StringUserID() string {
	var ss = ctx.GetConnection(ctx.connID)
	if ss == nil {
		return ""
	}
	return ss.StringUserID()
}
func (ctx *ctx) Token() []byte {
	var ss = ctx.GetConnection(ctx.connID)
	if ss == nil {
		return []byte{}
	}
	return ss.Token()
}
func (ctx *ctx) ConnectionID() uint16 {
	return ctx.connID
}
func (ctx *ctx) Header() uint8 {
	return ctx.request.header
}
func (ctx *ctx) EventCode() uint16 {
	return ctx.request.code
}
func (ctx *ctx) Payload() []byte {
	return ctx.request.payload
}
func (ctx *ctx) Now() time.Time {
	return ctx.server.Now()
}
func (ctx *ctx) ConnectionNum() int {
	return ctx.server.connectionManager.connectionNum()
}
func (ctx *ctx) CloseConnection(code ErrorCode, err string) {
	if ctx.IsInRoom() {
		ctx.Server().RoomExitUser(ctx.CurrentRoomID(), ctx.connID)
	}
	ctx.server.connection(ctx.connID).Close(code, err)
}
func (ctx *ctx) GetConnection(connID uint16) Connection {
	var ss = ctx.server.connection(connID)
	if ss == nil {
		return nil
	}
	return ss
}
func (ctx *ctx) ReadBool(val *bool) Request {
	if ctx.request.offset >= ctx.request.length {
		return ctx
	}
	*val = ctx.request.payload[ctx.request.offset] == 1
	ctx.request.offset++
	return ctx
}
func (ctx *ctx) ReadInt8(val *int8) Request {
	if ctx.request.offset >= ctx.request.length {
		return ctx
	}
	*val = int8(ctx.request.payload[ctx.request.offset])
	ctx.request.offset++
	return ctx
}
func (ctx *ctx) ReadUint8(val *uint8) Request {
	if ctx.request.offset >= ctx.request.length {
		return ctx
	}
	*val = ctx.request.payload[ctx.request.offset]
	ctx.request.offset++
	return ctx
}
func (ctx *ctx) ReadInt16(val *int16) Request {
	if ctx.request.offset+2 > ctx.request.length {
		return ctx
	}
	*val = int16(ctx.server.networkByteOrder.Uint16(ctx.request.payload[ctx.request.offset:]))
	ctx.request.offset += 2
	return ctx
}
func (ctx *ctx) ReadUint16(val *uint16) Request {
	if ctx.request.offset+2 > ctx.request.length {
		return ctx
	}
	*val = ctx.server.networkByteOrder.Uint16(ctx.request.payload[ctx.request.offset:])
	ctx.request.offset += 2
	return ctx
}
func (ctx *ctx) ReadInt32(val *int32) Request {
	if ctx.request.offset+4 > ctx.request.length {
		return ctx
	}
	*val = int32(ctx.server.networkByteOrder.Uint32(ctx.request.payload[ctx.request.offset:]))
	ctx.request.offset += 4
	return ctx
}
func (ctx *ctx) ReadUint32(val *uint32) Request {
	if ctx.request.offset+4 > ctx.request.length {
		return ctx
	}
	*val = ctx.server.networkByteOrder.Uint32(ctx.request.payload[ctx.request.offset:])
	ctx.request.offset += 4
	return ctx
}
func (ctx *ctx) ReadInt64(val *int64) Request {
	if ctx.request.offset+8 > ctx.request.length {
		return ctx
	}
	*val = int64(ctx.server.networkByteOrder.Uint64(ctx.request.payload[ctx.request.offset:]))
	ctx.request.offset += 8
	return ctx
}
func (ctx *ctx) ReadUint64(val *uint64) Request {
	if ctx.request.offset+8 > ctx.request.length {
		return ctx
	}
	*val = ctx.server.networkByteOrder.Uint64(ctx.request.payload[ctx.request.offset:])
	ctx.request.offset += 8
	return ctx
}
func (ctx *ctx) ReadFloat32(val *float32) Request {
	if ctx.request.offset+4 > ctx.request.length {
		return ctx
	}
	*val = math.Float32frombits(ctx.server.networkByteOrder.Uint32(ctx.request.payload[ctx.request.offset:]))
	ctx.request.offset += 4
	return ctx
}
func (ctx *ctx) ReadFloat64(val *float64) Request {
	if ctx.request.offset+8 > ctx.request.length {
		return ctx
	}
	*val = math.Float64frombits(ctx.server.networkByteOrder.Uint64(ctx.request.payload[ctx.request.offset:]))
	ctx.request.offset += 8
	return ctx
}
func (ctx *ctx) ReadBytes() []byte {
	if ctx.request.offset+2 > ctx.request.length {
		return nil
	}
	var byteLen = ctx.server.networkByteOrder.Uint16(ctx.request.payload[ctx.request.offset:])
	ctx.request.offset += 2
	if ctx.request.offset+byteLen > ctx.request.length {
		return nil
	}
	var ret = ctx.request.payload[ctx.request.offset : ctx.request.offset+byteLen]
	ctx.request.offset += byteLen
	return ret
}
func (ctx *ctx) ReadString() string {
	return string(ctx.ReadBytes())
}
func (ctx *ctx) InPayload() []byte {
	return ctx.request.payload
}
func (ctx *ctx) Send(connID uint16, out ...Response) Ctx {
	if len(out) < 1 {
		ctx.server.SendMessage(connID, ctx.response)
	} else {
		ctx.server.SendMessage(connID, out[0])
	}
	return ctx
}
func (ctx *ctx) BroadcastServer(out ...Response) Ctx {
	if len(out) < 1 {
		ctx.server.BroadcastMessageServer(ctx.response)
	} else {
		ctx.server.BroadcastMessageServer(out[0])
	}
	return ctx
}
func (ctx *ctx) BroadcastApplication(out ...Response) Ctx {
	var pkt Response = ctx.response
	if len(out) > 0 {
		pkt = out[0]
	}
	var connection = ctx.server.connection(ctx.connID)
	if connection != nil && connection.AppID() != nil {
		ctx.server.BroadcastMessageApplication(connection.AppID(), pkt)
	}
	return ctx
}
func (ctx *ctx) BroadcastRoom(out ...Response) Ctx {
	var pkt Response = ctx.response
	if len(out) > 0 {
		pkt = out[0]
	}
	var connection = ctx.server.connection(ctx.connID)
	if connection != nil && connection.roomID != nil {
		ctx.server.BroadcastMessageRoom(*connection.roomID, pkt)
	}
	return ctx
}
func (ctx *ctx) OutMessage() Response {
	return ctx.response
}
func (ctx *ctx) WriteBool(val bool) Response {
	return ctx.response.WriteBool(val)
}
func (ctx *ctx) WriteInt8(val int8) Response {
	return ctx.response.WriteInt8(val)
}
func (ctx *ctx) WriteUint8(val uint8) Response {
	return ctx.response.WriteUint8(val)
}
func (ctx *ctx) WriteInt16(val int16) Response {
	return ctx.response.WriteInt16(val)
}
func (ctx *ctx) WriteUint16(val uint16) Response {
	return ctx.response.WriteUint16(val)
}
func (ctx *ctx) WriteInt32(val int32) Response {
	return ctx.response.WriteInt32(val)
}
func (ctx *ctx) WriteUint32(val uint32) Response {
	return ctx.response.WriteUint32(val)
}
func (ctx *ctx) WriteInt64(val int64) Response {
	return ctx.response.WriteInt64(val)
}
func (ctx *ctx) WriteUint64(val uint64) Response {
	return ctx.response.WriteUint64(val)
}
func (ctx *ctx) WriteFloat32(val float32) Response {
	return ctx.response.WriteFloat32(val)
}
func (ctx *ctx) WriteFloat64(val float64) Response {
	return ctx.response.WriteFloat64(val)
}
func (ctx *ctx) WriteBytes(val []byte) Response {
	return ctx.response.WriteBytes(val)
}
func (ctx *ctx) WriteString(val string) Response {
	return ctx.response.WriteString(val)
}
func (ctx *ctx) WriteBytesNoLen(val []byte) Response {
	return ctx.response.WriteBytesNoLen(val)
}
func (ctx *ctx) WriteRoom(roomID uint16) Response {
	return ctx.response.WriteRoom(roomID)
}
func (ctx *ctx) WriteUserStatus(connectionID uint16) Response {
	return ctx.response.WriteUserStatus(connectionID)
}
func (ctx *ctx) SetHeader(header uint8) Response {
	return ctx.response.SetHeader(header)
}
func (ctx *ctx) SetHeader2(protocolVer, cmd uint8) Response {
	return ctx.response.SetHeader2(protocolVer, cmd)
}
func (ctx *ctx) SetEventCode(code uint16) Response {
	return ctx.response.SetEventCode(code)
}
func (ctx *ctx) Reset() Response {
	return ctx.response.Reset()
}
func (ctx *ctx) Authorization() interface{} {
	return ctx.authorization
}
func (ctx *ctx) SetAuthorization(authorization interface{}) Ctx {
	ctx.authorization = authorization
	return ctx
}
func (ctx *ctx) ValueTable() CtxValueTable {
	return ctx.value
}
func (ctx *ctx) Redis(instanceID ...string) (plugin *plugins.Redis) {
	if len(instanceID) > 0 {
		return ctx.server.Redis(instanceID[0])
	}
	return ctx.server.Redis(ctx.defaultRedis)
}
func (ctx *ctx) Cache(cacheID ...string) (plugin *plugins.Cache) {
	if len(cacheID) > 0 {
		return ctx.server.Cache(cacheID[0])
	}
	return ctx.server.Cache(ctx.defaultCache)
}
func (ctx *ctx) CsvTable(tableName string, args ...string) plugins.MasterTable {
	var mux string
	if len(args) > 0 {
		mux = args[0]
	} else {
		mux = ctx.defaultCsvMux
	}
	var val, ok = ctx.server.engine.MasterData().Table(tableName, mux)
	if ok && ctx.server.engine.DebugMode() {
		ctx.logEntry.Debugf("loaded master table:%s", tableName)
	}
	var l plugins.Logger = nil
	if !ctx.server.engine.DebugMode() {
		l = ctx.logEntry
	}
	return plugins.MasterTableWithLogger(l, mux, tableName, val)
}
func (ctx *ctx) Debug(format string, args ...interface{}) Ctx {
	ctx.server.debug(format, args...)
	return ctx
}
func (ctx *ctx) Infof(format string, args ...interface{}) Ctx {
	ctx.logEntry.Infof(format, args...)
	return ctx
}
func (ctx *ctx) Warnf(format string, args ...interface{}) Ctx {
	ctx.logEntry.Warnf(format, args...)
	return ctx
}
func (ctx *ctx) Assert(ok bool, format string, args ...interface{}) Ctx {
	if ok {
		return ctx
	}
	panic(fmt.Sprintf(format, args...))
}
func (ctx *ctx) ErrorClientRequest(errorCode uint16, format string, args ...interface{}) {
	var str = fmt.Sprintf(format, args...)
	ctx.logEntry.Info(str)
	ctx.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
	ctx.SetEventCode(EventCodeErrorClient)
	ctx.WriteUint16(errorCode).WriteString(str)
	ctx.Send(ctx.connID)
	ctx.Reset()
}
func (ctx *ctx) ErrorServerInternal(format string, args ...interface{}) {
	var str = fmt.Sprintf(format, args...)
	ctx.logEntry.Error(str)
	ctx.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
	ctx.SetEventCode(EventCodeErrorServer)
	ctx.WriteUint16(ErrorCodeInternal).WriteString(str)
	ctx.Send(ctx.connID)
	ctx.Reset()
}

func (ctx *ctx) IsInRoom() bool {
	return ctx.server.connection(ctx.connID).roomID != nil
}
func (ctx *ctx) CurrentRoomID() uint16 {
	var roomIDPtr = ctx.server.connection(ctx.connID).roomID
	if roomIDPtr == nil {
		return 0
	}
	return *roomIDPtr
}
func (ctx *ctx) dispose() *ctx {
	ctx.server.responsePool.Put(ctx.response)
	ctx.server.requestPool.Put(ctx.request)
	ctx.server.ctxValuePool.Put(ctx.value)
	return ctx
}

func (m *message) WriteBool(val bool) Response {
	if (m.offset+1)&messagePayloadSizeMask != 0 {
		return m
	}
	if val {
		m.payload[m.offset] = 1
	} else {
		m.payload[m.offset] = 0
	}
	m.offset++
	return m
}
func (m *message) WriteInt8(val int8) Response {
	if (m.offset+1)&messagePayloadSizeMask != 0 {
		return m
	}
	m.payload[m.offset] = uint8(val)
	m.offset++
	return m
}
func (m *message) WriteUint8(val uint8) Response {
	if (m.offset+1)&messagePayloadSizeMask != 0 {
		return m
	}
	m.payload[m.offset] = val
	m.offset++
	return m
}
func (m *message) WriteInt16(val int16) Response {
	if (m.offset+2)&messagePayloadSizeMask != 0 {
		return m
	}
	m.payload[m.offset+1] = uint8(val & 0xff)
	m.payload[m.offset] = uint8(val >> 8)
	m.offset += 2
	return m
}
func (m *message) WriteUint16(val uint16) Response {
	if (m.offset+2)&messagePayloadSizeMask != 0 {
		return m
	}
	m.payload[m.offset+1] = uint8(val & 0xff)
	m.payload[m.offset] = uint8(val >> 8)
	m.offset += 2
	return m
}
func (m *message) WriteInt32(val int32) Response {
	if (m.offset+4)&messagePayloadSizeMask != 0 {
		return m
	}
	m.payload[m.offset+3] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+2] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+1] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset] = uint8(val & 0xff)
	m.offset += 4
	return m
}
func (m *message) WriteUint32(val uint32) Response {
	if (m.offset+4)&messagePayloadSizeMask != 0 {
		return m
	}
	m.payload[m.offset+3] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+2] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+1] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset] = uint8(val & 0xff)
	m.offset += 4
	return m
}
func (m *message) WriteInt64(val int64) Response {
	if (m.offset+8)&messagePayloadSizeMask != 0 {
		return m
	}
	m.payload[m.offset+7] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+6] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+5] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+4] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+3] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+2] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+1] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset] = uint8(val & 0xff)
	m.offset += 8
	return m
}
func (m *message) WriteUint64(val uint64) Response {
	if (m.offset+8)&messagePayloadSizeMask != 0 {
		return m
	}
	m.payload[m.offset+7] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+6] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+5] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+4] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+3] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+2] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset+1] = uint8(val & 0xff)
	val >>= 8
	m.payload[m.offset] = uint8(val & 0xff)
	m.offset += 8
	return m
}
func (m *message) WriteFloat32(val float32) Response {
	return m.WriteUint32(math.Float32bits(val))
}
func (m *message) WriteFloat64(val float64) Response {
	return m.WriteUint64(math.Float64bits(val))
}
func (m *message) WriteBytes(val []byte) Response {
	var l = uint16(len(val))
	if (m.offset+1)&messagePayloadSizeMask != 0 {
		return m
	}
	m.payload[m.offset+1] = uint8(l & 0xff)
	m.payload[m.offset] = uint8(l >> 8)
	m.offset += 2
	copy(m.payload[m.offset:], val)
	m.offset += l
	return m
}
func (m *message) WriteString(val string) Response {
	return m.WriteBytes([]byte(val))
}
func (m *message) WriteBytesNoLen(val []byte) Response {
	if (m.offset+1)&messagePayloadSizeMask != 0 {
		return m
	}
	copy(m.payload[m.offset:], val)
	m.offset += uint16(len(val))
	return m
}
func (m *message) WriteRoom(roomID uint16) Response {
	var room = m.server.roomTable.room(roomID)
	if room == nil {
		m.server.logEntry.WithField("room", fmt.Sprintf("%x", roomID)).Warnf("nil room")
		return m
	}
	room.status.writeToMessage(m)
	return m
}
func (m *message) WriteUserStatus(connectionID uint16) Response {
	var connection = m.server.connection(connectionID)
	if connection == nil {
		m.server.logEntry.WithField("connection", fmt.Sprintf("%x", connectionID)).Warnf("nil user status")
		return m
	}
	const (
		inServer uint8 = 0
		inRoom   uint8 = 1
	)
	m.WriteUint16(connection.id)
	m.WriteBytes(connection.UserID())
	if connection.roomID != nil && connection.roomPos != nil {
		m.WriteUint8(inRoom).WriteUint16(*connection.roomID).WriteUint16(*connection.roomPos)
	} else {
		m.WriteUint8(inServer)
	}
	return m
}
func (m *message) SetHeader(header uint8) Response {
	m.header = header
	return m
}
func (m *message) SetHeader2(protocolVer, cmd uint8) Response {
	m.header = protocolVer | cmd
	return m
}
func (m *message) SetEventCode(code uint16) Response {
	m.code = code
	m.payload[0], m.payload[1] = uint8(code>>8), uint8(code&0xff)
	return m
}
func (m *message) Reset() Response {
	m.offset = 2
	return m
}

const (
	AuthenticationLevelProvider = iota
	AuthenticationLevelDeveloper
	AuthenticationLevelAppManager
	AuthenticationLevelUser
)
