package hybs

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"

	"github.com/labstack/gommon/random"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
)

// RealtimeCtx is interface rtCtx implements
type RealtimeCtx interface {
	// InPacket is received packet
	InPacket
	// OutPacket is sending packet
	OutPacket
	// Server returns reference of realtime server
	Server() RealtimeServer
	// AppID returns using application id
	AppID() []byte
	// UserID returns the id of the request source user
	UserID() []byte
	// Token returns the token of the request source user
	Token() []byte
	// SessionID returns the session id of the request source user
	SessionID() uint16
	// Header returns request header
	Header() uint8
	// EventCode returns request event code
	EventCode() uint16
	// Payload returns request payload body
	Payload() []byte
	// OutPacket returns sending/response packet
	OutPacket() OutPacket
	// Cache returns reference of cache instance
	Cache(cacheID ...string) (cache *cache.Cache)
	// Redis returns reference of redis connection
	Redis(instanceID ...string) (r RedisClient)
	// MasterTable find and returns master table by table name
	MasterTable(tableName string, args ...string) MasterTable
	// GetSession find and returns session by session id
	GetSession(sessionID uint16) RealtimeSession
	// CloseSession closes the session in used with error code
	CloseSession(code ErrorCode, err string)
	// IsInRoom returns true when current user is in a room
	IsInRoom() bool
	// CurrentRoomID returns the current room id
	CurrentRoomID() uint16
	// Response responses packet back to the request source user
	Response(out ...OutPacket) RealtimeCtx
	// Send sends packet to specified user
	Send(destSessionID uint16, out ...OutPacket) RealtimeCtx
	// BroadcastServer broadcasts packet to all online users
	BroadcastServer(out ...OutPacket) RealtimeCtx
	// BroadcastApplication broadcasts packet to users in the same application
	BroadcastApplication(out ...OutPacket) RealtimeCtx
	// BroadcastRoom broadcasts packet to users in the same room
	BroadcastRoom(out ...OutPacket) RealtimeCtx
	// Authorization returns stored user authorization information
	Authorization() interface{}
	// SetAuthorization set and stores user authorization information
	SetAuthorization(authorization interface{}) RealtimeCtx
	// ValueTable returns user original parameters table
	ValueTable() RTCtxValueTable
	// HttpClient returns the default http client that used for server-server communication
	HttpClient() *HttpClient
	// Debug writes debug level log
	Debug(format string, args ...interface{}) RealtimeCtx
	// Infof writes info level log
	Infof(format string, args ...interface{}) RealtimeCtx
	// Warnf writes warn level log
	Warnf(format string, args ...interface{}) RealtimeCtx
	// Assert throws a fatal error if ok is not true
	Assert(ok bool, format string, args ...interface{}) RealtimeCtx
	// ErrorClientRequest responses client side error: bad request, unauthorized, forbidden etc.
	ErrorClientRequest(errorCode uint16, format string, args ...interface{})
	// ErrorServerInternal responses server side internal error
	ErrorServerInternal(format string, args ...interface{})
	// Now returns the time when request came
	Now() Time
}

// InPacket is interface that received binary data packet implements
type InPacket interface {
	ReadBool(val *bool) InPacket
	ReadInt8(val *int8) InPacket
	ReadUint8(val *uint8) InPacket
	ReadInt16(val *int16) InPacket
	ReadUint16(val *uint16) InPacket
	ReadInt32(val *int32) InPacket
	ReadUint32(val *uint32) InPacket
	ReadInt64(val *int64) InPacket
	ReadUint64(val *uint64) InPacket
	ReadFloat32(val *float32) InPacket
	ReadFloat64(val *float64) InPacket
	ReadBytes() []byte
	ReadString() string
	InPayload() []byte
}

// OutPacket is interface that sending binary data packet implements
type OutPacket interface {
	WriteBool(val bool) OutPacket
	WriteInt8(val int8) OutPacket
	WriteUint8(val uint8) OutPacket
	WriteInt16(val int16) OutPacket
	WriteUint16(val uint16) OutPacket
	WriteInt32(val int32) OutPacket
	WriteUint32(val uint32) OutPacket
	WriteInt64(val int64) OutPacket
	WriteUint64(val uint64) OutPacket
	WriteFloat32(val float32) OutPacket
	WriteFloat64(val float64) OutPacket
	WriteBytes(val []byte) OutPacket
	WriteString(val string) OutPacket
	WriteBytesNoLen(val []byte) OutPacket

	WriteRoom(roomID uint16) OutPacket
	WriteUserStatus(sessionID uint16) OutPacket
	SetHeader(header uint8) OutPacket
	SetHeader3(pv, pd, cmd uint8) OutPacket
	SetEventCode(code uint16) OutPacket
	Reset() OutPacket
}

const rtCtxValueTableSize = 1 << 4

type rtCtxValueTable [rtCtxValueTableSize]interface{}

// RTCtxValueTable is interface rtCtxValueTable implements
type RTCtxValueTable interface {
	Set(key int, value interface{}) RTCtxValueTable
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

func (t *rtCtxValueTable) Set(key int, value interface{}) RTCtxValueTable {
	if key >= rtCtxValueTableSize {
		panic("bad rtCtxValue index")
		return t
	}
	if key < 0 {
		panic("bad rtCtxValue index")
		return t
	}
	t[key] = value
	return t
}
func (t *rtCtxValueTable) GetInt(key int, def ...int) int {
	var val, ok = t[key].(int)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *rtCtxValueTable) GetUint(key int, def ...uint) uint {
	var val, ok = t[key].(uint)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *rtCtxValueTable) GetInt16(key int, def ...int16) int16 {
	var val, ok = t[key].(int16)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *rtCtxValueTable) GetUint16(key int, def ...uint16) uint16 {
	var val, ok = t[key].(uint16)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *rtCtxValueTable) GetByte(key int, def ...byte) byte {
	var val, ok = t[key].(byte)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *rtCtxValueTable) GetBool(key int, def ...bool) bool {
	var val, ok = t[key].(bool)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return false
	}
	return val
}
func (t *rtCtxValueTable) GetFloat32(key int, def ...float32) float32 {
	var val, ok = t[key].(float32)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *rtCtxValueTable) GetFloat64(key int, def ...float64) float64 {
	var val, ok = t[key].(float64)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return val
}
func (t *rtCtxValueTable) GetBytes(key int) []byte {
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
func (t *rtCtxValueTable) GetString(key int, def ...string) string {
	var val, ok = t[key].(string)
	if !ok {
		if len(def) > 0 {
			return def[0]
		}
		return ""
	}
	return val
}

type rtCtx struct {
	// core component
	server    *hybsRealtimeServer
	sessionID uint16
	in        *rtPacket
	out       *rtPacket
	// context
	value         *rtCtxValueTable
	authorization interface{}
	defaultRedis  string
	defaultCache  string
	defaultCsvMux string
	// log
	logEntry *logrus.Entry
}

type rtPacket struct {
	header  uint8
	length  uint16
	offset  uint16
	code    uint16
	payload []uint8
	server  *hybsRealtimeServer
}

func (ctx *rtCtx) Server() RealtimeServer {
	return ctx.server
}
func (ctx *rtCtx) AppID() []byte {
	var ss = ctx.GetSession(ctx.sessionID)
	if ss == nil {
		return []byte{}
	}
	return ss.AppID()
}
func (ctx *rtCtx) UserID() []byte {
	var ss = ctx.GetSession(ctx.sessionID)
	if ss == nil {
		return []byte{}
	}
	return ss.UserID()
}
func (ctx *rtCtx) StringUserID() string {
	var ss = ctx.GetSession(ctx.sessionID)
	if ss == nil {
		return ""
	}
	return ss.StringUserID()
}
func (ctx *rtCtx) Permission() uint8 {
	var ss = ctx.GetSession(ctx.sessionID)
	if ss == nil {
		return UserPermissionGuest
	}
	return ss.(*rtSession).Permission()
}
func (ctx *rtCtx) Token() []byte {
	var ss = ctx.GetSession(ctx.sessionID)
	if ss == nil {
		return []byte{}
	}
	return ss.Token()
}
func (ctx *rtCtx) SessionID() uint16 {
	return ctx.sessionID
}
func (ctx *rtCtx) Header() uint8 {
	return ctx.in.header
}
func (ctx *rtCtx) EventCode() uint16 {
	return ctx.in.code
}
func (ctx *rtCtx) Payload() []byte {
	return ctx.in.payload
}
func (ctx *rtCtx) Now() Time {
	return ctx.server.Now()
}
func (ctx *rtCtx) SessionNum() int {
	return ctx.server.sessionManager.sessionNum()
}
func (ctx *rtCtx) CloseSession(code ErrorCode, err string) {
	if ctx.IsInRoom() {
		ctx.Server().RoomExitUser(ctx.CurrentRoomID(), ctx.sessionID)
	}
	ctx.server.session(ctx.sessionID).Close(code, err)
}
func (ctx *rtCtx) GetSession(sessionID uint16) RealtimeSession {
	var ss = ctx.server.session(sessionID)
	if ss == nil {
		return nil
	}
	return ss
}
func (ctx *rtCtx) ReadBool(val *bool) InPacket {
	if ctx.in.offset >= ctx.in.length {
		return ctx
	}
	*val = ctx.in.payload[ctx.in.offset] == 1
	ctx.in.offset++
	return ctx
}
func (ctx *rtCtx) ReadInt8(val *int8) InPacket {
	if ctx.in.offset >= ctx.in.length {
		return ctx
	}
	*val = int8(ctx.in.payload[ctx.in.offset])
	ctx.in.offset++
	return ctx
}
func (ctx *rtCtx) ReadUint8(val *uint8) InPacket {
	if ctx.in.offset >= ctx.in.length {
		return ctx
	}
	*val = ctx.in.payload[ctx.in.offset]
	ctx.in.offset++
	return ctx
}
func (ctx *rtCtx) ReadInt16(val *int16) InPacket {
	if ctx.in.offset+2 > ctx.in.length {
		return ctx
	}
	*val = int16(ctx.server.networkByteOrder.Uint16(ctx.in.payload[ctx.in.offset:]))
	ctx.in.offset += 2
	return ctx
}
func (ctx *rtCtx) ReadUint16(val *uint16) InPacket {
	if ctx.in.offset+2 > ctx.in.length {
		return ctx
	}
	*val = ctx.server.networkByteOrder.Uint16(ctx.in.payload[ctx.in.offset:])
	ctx.in.offset += 2
	return ctx
}
func (ctx *rtCtx) ReadInt32(val *int32) InPacket {
	if ctx.in.offset+4 > ctx.in.length {
		return ctx
	}
	*val = int32(ctx.server.networkByteOrder.Uint32(ctx.in.payload[ctx.in.offset:]))
	ctx.in.offset += 4
	return ctx
}
func (ctx *rtCtx) ReadUint32(val *uint32) InPacket {
	if ctx.in.offset+4 > ctx.in.length {
		return ctx
	}
	*val = ctx.server.networkByteOrder.Uint32(ctx.in.payload[ctx.in.offset:])
	ctx.in.offset += 4
	return ctx
}
func (ctx *rtCtx) ReadInt64(val *int64) InPacket {
	if ctx.in.offset+8 > ctx.in.length {
		return ctx
	}
	*val = int64(ctx.server.networkByteOrder.Uint64(ctx.in.payload[ctx.in.offset:]))
	ctx.in.offset += 8
	return ctx
}
func (ctx *rtCtx) ReadUint64(val *uint64) InPacket {
	if ctx.in.offset+8 > ctx.in.length {
		return ctx
	}
	*val = ctx.server.networkByteOrder.Uint64(ctx.in.payload[ctx.in.offset:])
	ctx.in.offset += 8
	return ctx
}
func (ctx *rtCtx) ReadFloat32(val *float32) InPacket {
	if ctx.in.offset+4 > ctx.in.length {
		return ctx
	}
	*val = math.Float32frombits(ctx.server.networkByteOrder.Uint32(ctx.in.payload[ctx.in.offset:]))
	ctx.in.offset += 4
	return ctx
}
func (ctx *rtCtx) ReadFloat64(val *float64) InPacket {
	if ctx.in.offset+8 > ctx.in.length {
		return ctx
	}
	*val = math.Float64frombits(ctx.server.networkByteOrder.Uint64(ctx.in.payload[ctx.in.offset:]))
	ctx.in.offset += 8
	return ctx
}
func (ctx *rtCtx) ReadBytes() []byte {
	if ctx.in.offset+2 > ctx.in.length {
		return nil
	}
	var byteLen = ctx.server.networkByteOrder.Uint16(ctx.in.payload[ctx.in.offset:])
	ctx.in.offset += 2
	if ctx.in.offset+byteLen > ctx.in.length {
		return nil
	}
	var ret = ctx.in.payload[ctx.in.offset : ctx.in.offset+byteLen]
	ctx.in.offset += byteLen
	return ret
}
func (ctx *rtCtx) ReadString() string {
	return string(ctx.ReadBytes())
}
func (ctx *rtCtx) InPayload() []byte {
	return ctx.in.payload
}
func (ctx *rtCtx) Response(out ...OutPacket) RealtimeCtx {
	if len(out) < 1 {
		ctx.server.SendPacket(ctx.sessionID, ctx.out)
	} else {
		ctx.server.SendPacket(ctx.sessionID, out[0])
	}
	return ctx
}
func (ctx *rtCtx) Send(sessionID uint16, out ...OutPacket) RealtimeCtx {
	if len(out) < 1 {
		ctx.server.SendPacket(sessionID, ctx.out)
	} else {
		ctx.server.SendPacket(sessionID, out[0])
	}
	return ctx
}
func (ctx *rtCtx) BroadcastServer(out ...OutPacket) RealtimeCtx {
	if len(out) < 1 {
		ctx.server.BroadcastPacketServer(ctx.out)
	} else {
		ctx.server.BroadcastPacketServer(out[0])
	}
	return ctx
}
func (ctx *rtCtx) BroadcastApplication(out ...OutPacket) RealtimeCtx {
	var pkt OutPacket = ctx.out
	if len(out) > 0 {
		pkt = out[0]
	}
	var session = ctx.server.session(ctx.sessionID)
	if session != nil && session.AppID() != nil {
		ctx.server.BroadcastPacketApplication(session.AppID(), pkt)
	}
	return ctx
}
func (ctx *rtCtx) BroadcastRoom(out ...OutPacket) RealtimeCtx {
	var pkt OutPacket = ctx.out
	if len(out) > 0 {
		pkt = out[0]
	}
	var session = ctx.server.session(ctx.sessionID)
	if session != nil && session.roomID != nil {
		ctx.server.BroadcastPacketRoom(*session.roomID, pkt)
	}
	return ctx
}
func (ctx *rtCtx) OutPacket() OutPacket {
	return ctx.out
}
func (ctx *rtCtx) WriteBool(val bool) OutPacket {
	return ctx.out.WriteBool(val)
}
func (ctx *rtCtx) WriteInt8(val int8) OutPacket {
	return ctx.out.WriteInt8(val)
}
func (ctx *rtCtx) WriteUint8(val uint8) OutPacket {
	return ctx.out.WriteUint8(val)
}
func (ctx *rtCtx) WriteInt16(val int16) OutPacket {
	return ctx.out.WriteInt16(val)
}
func (ctx *rtCtx) WriteUint16(val uint16) OutPacket {
	return ctx.out.WriteUint16(val)
}
func (ctx *rtCtx) WriteInt32(val int32) OutPacket {
	return ctx.out.WriteInt32(val)
}
func (ctx *rtCtx) WriteUint32(val uint32) OutPacket {
	return ctx.out.WriteUint32(val)
}
func (ctx *rtCtx) WriteInt64(val int64) OutPacket {
	return ctx.out.WriteInt64(val)
}
func (ctx *rtCtx) WriteUint64(val uint64) OutPacket {
	return ctx.out.WriteUint64(val)
}
func (ctx *rtCtx) WriteFloat32(val float32) OutPacket {
	return ctx.out.WriteFloat32(val)
}
func (ctx *rtCtx) WriteFloat64(val float64) OutPacket {
	return ctx.out.WriteFloat64(val)
}
func (ctx *rtCtx) WriteBytes(val []byte) OutPacket {
	return ctx.out.WriteBytes(val)
}
func (ctx *rtCtx) WriteString(val string) OutPacket {
	return ctx.out.WriteString(val)
}
func (ctx *rtCtx) WriteBytesNoLen(val []byte) OutPacket {
	return ctx.out.WriteBytesNoLen(val)
}
func (ctx *rtCtx) WriteRoom(roomID uint16) OutPacket {
	return ctx.out.WriteRoom(roomID)
}
func (ctx *rtCtx) WriteUserStatus(sessionID uint16) OutPacket {
	return ctx.out.WriteUserStatus(sessionID)
}
func (ctx *rtCtx) SetHeader(header uint8) OutPacket {
	return ctx.out.SetHeader(header)
}
func (ctx *rtCtx) SetHeader3(pv, pd, cmd uint8) OutPacket {
	return ctx.out.SetHeader3(pv, pd, cmd)
}
func (ctx *rtCtx) SetEventCode(code uint16) OutPacket {
	return ctx.out.SetEventCode(code)
}
func (ctx *rtCtx) Reset() OutPacket {
	return ctx.out.Reset()
}
func (ctx *rtCtx) Authorization() interface{} {
	return ctx.authorization
}
func (ctx *rtCtx) SetAuthorization(authorization interface{}) RealtimeCtx {
	ctx.authorization = authorization
	return ctx
}
func (ctx *rtCtx) ValueTable() RTCtxValueTable {
	return ctx.value
}
func (ctx *rtCtx) Redis(instanceID ...string) (r RedisClient) {
	return ctx.server.Redis(instanceID...)
}
func (ctx *rtCtx) Cache(cacheID ...string) (cache *cache.Cache) {
	return ctx.server.Cache(cacheID...)
}
func (ctx *rtCtx) MasterTable(tableName string, args ...string) MasterTable {
	var mux string
	if len(args) > 0 {
		mux = args[0]
	} else {
		mux = ctx.defaultCsvMux
	}
	var val, ok = ctx.server.engine.masterDataMgr.loadTable(tableName, mux)
	if ok && ctx.server.engine.debugMode() {
		ctx.logEntry.Debugf("loaded master table:%s", tableName)
	}
	var l masterDataLogger = nil
	if !ctx.server.engine.debugMode() {
		l = ctx
	}
	return &masterDataTable{
		l:         l,
		mux:       mux,
		tableName: tableName,
		container: val,
	}
}
func (ctx *rtCtx) Debug(format string, args ...interface{}) RealtimeCtx {
	ctx.server.debug(format, args...)
	return ctx
}
func (ctx *rtCtx) Infof(format string, args ...interface{}) RealtimeCtx {
	ctx.logEntry.Infof(format, args...)
	return ctx
}
func (ctx *rtCtx) Warnf(format string, args ...interface{}) RealtimeCtx {
	ctx.logEntry.Warnf(format, args...)
	return ctx
}
func (ctx *rtCtx) Assert(ok bool, format string, args ...interface{}) RealtimeCtx {
	if ok {
		return ctx
	}
	panic(fmt.Sprintf(format, args...))
}
func (ctx *rtCtx) ErrorClientRequest(errorCode uint16, format string, args ...interface{}) {
	var str = fmt.Sprintf(format, args...)
	ctx.logEntry.Info(str)
	ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
	ctx.SetEventCode(RealtimeEventCodeErrorClient)
	ctx.WriteUint16(errorCode).WriteString(str)
	ctx.Response()
	ctx.Reset()
}
func (ctx *rtCtx) ErrorServerInternal(format string, args ...interface{}) {
	var str = fmt.Sprintf(format, args...)
	ctx.logEntry.Error(str)
	ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
	ctx.SetEventCode(RealtimeEventCodeErrorServer)
	ctx.WriteUint16(RealtimeErrorCodeInternal).WriteString(str)
	ctx.Response()
	ctx.Reset()
}

func (ctx *rtCtx) IsInRoom() bool {
	return ctx.server.session(ctx.sessionID).roomID != nil
}
func (ctx *rtCtx) CurrentRoomID() uint16 {
	var roomIDPtr = ctx.server.session(ctx.sessionID).roomID
	if roomIDPtr == nil {
		return 0
	}
	return *roomIDPtr
}
func (ctx *rtCtx) HttpClient() *HttpClient {
	return DefaultHttpClient
}
func (ctx *rtCtx) dispose() *rtCtx {
	ctx.server.outPacketPool.Put(ctx.out)
	ctx.server.inPacketPool.Put(ctx.in)
	ctx.server.rtCtxValuePool.Put(ctx.value)
	return ctx
}
func (ctx *rtCtx) logf(format string, args ...interface{}) {
	ctx.logEntry.Debugf(format, args...)
}

func (o *rtPacket) WriteBool(val bool) OutPacket {
	if (o.offset+1)&rtPacketPayloadSizeMask != 0 {
		return o
	}
	if val {
		o.payload[o.offset] = 1
	} else {
		o.payload[o.offset] = 0
	}
	o.offset++
	return o
}
func (o *rtPacket) WriteInt8(val int8) OutPacket {
	if (o.offset+1)&0xc000 != 0 {
		return o
	}
	o.payload[o.offset] = uint8(val)
	o.offset++
	return o
}
func (o *rtPacket) WriteUint8(val uint8) OutPacket {
	if (o.offset+1)&0xc000 != 0 {
		return o
	}
	o.payload[o.offset] = val
	o.offset++
	return o
}
func (o *rtPacket) WriteInt16(val int16) OutPacket {
	if (o.offset+2)&0xc000 != 0 {
		return o
	}
	o.payload[o.offset+1] = uint8(val & 0xff)
	o.payload[o.offset] = uint8(val >> 8)
	o.offset += 2
	return o
}
func (o *rtPacket) WriteUint16(val uint16) OutPacket {
	if (o.offset+2)&0xc000 != 0 {
		return o
	}
	o.payload[o.offset+1] = uint8(val & 0xff)
	o.payload[o.offset] = uint8(val >> 8)
	o.offset += 2
	return o
}
func (o *rtPacket) WriteInt32(val int32) OutPacket {
	if (o.offset+4)&0xc000 != 0 {
		return o
	}
	o.payload[o.offset+3] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+2] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+1] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset] = uint8(val & 0xff)
	o.offset += 4
	return o
}
func (o *rtPacket) WriteUint32(val uint32) OutPacket {
	if (o.offset+4)&0xc000 != 0 {
		return o
	}
	o.payload[o.offset+3] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+2] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+1] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset] = uint8(val & 0xff)
	o.offset += 4
	return o
}
func (o *rtPacket) WriteInt64(val int64) OutPacket {
	if (o.offset+8)&0xc000 != 0 {
		return o
	}
	o.payload[o.offset+7] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+6] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+5] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+4] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+3] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+2] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+1] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset] = uint8(val & 0xff)
	o.offset += 8
	return o
}
func (o *rtPacket) WriteUint64(val uint64) OutPacket {
	if (o.offset+8)&0xc000 != 0 {
		return o
	}
	o.payload[o.offset+7] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+6] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+5] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+4] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+3] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+2] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset+1] = uint8(val & 0xff)
	val >>= 8
	o.payload[o.offset] = uint8(val & 0xff)
	o.offset += 8
	return o
}
func (o *rtPacket) WriteFloat32(val float32) OutPacket {
	return o.WriteUint32(math.Float32bits(val))
}
func (o *rtPacket) WriteFloat64(val float64) OutPacket {
	return o.WriteUint64(math.Float64bits(val))
}
func (o *rtPacket) WriteBytes(val []byte) OutPacket {
	var l = uint16(len(val))
	if (o.offset+1)&0xc000 != 0 {
		return o
	}
	o.payload[o.offset+1] = uint8(l & 0xff)
	o.payload[o.offset] = uint8(l >> 8)
	o.offset += 2
	copy(o.payload[o.offset:], val)
	o.offset += l
	return o
}
func (o *rtPacket) WriteString(val string) OutPacket {
	return o.WriteBytes([]byte(val))
}
func (o *rtPacket) WriteBytesNoLen(val []byte) OutPacket {
	if (o.offset+1)&0xc000 != 0 {
		return o
	}
	copy(o.payload[o.offset:], val)
	o.offset += uint16(len(val))
	return o
}
func (o *rtPacket) WriteRoom(roomID uint16) OutPacket {
	var room = o.server.roomTable.room(roomID)
	if room == nil {
		o.server.logger.WithField("room", fmt.Sprintf("%x", roomID)).Warnf("write room nil")
		return o
	}
	room.writeToPacket(o)
	return o
}
func (o *rtPacket) WriteUserStatus(sessionID uint16) OutPacket {
	var session = o.server.session(sessionID)
	if session == nil {
		o.server.logger.WithField("session", fmt.Sprintf("%x", sessionID)).Warnf("write user status nil")
		return o
	}
	const (
		inServer uint8 = 0
		inRoom   uint8 = 1
	)
	o.WriteUint16(session.id)
	o.WriteBytes(session.UserID())
	if session.roomID != nil && session.roomPos != nil {
		o.WriteUint8(inRoom).WriteUint16(*session.roomID).WriteUint16(*session.roomPos)
	} else {
		o.WriteUint8(inServer)
	}
	return o
}
func (o *rtPacket) SetHeader(header uint8) OutPacket {
	o.header = header
	return o
}
func (o *rtPacket) SetHeader3(pv, pd, cmd uint8) OutPacket {
	o.header = pv | pd | cmd
	return o
}
func (o *rtPacket) SetEventCode(code uint16) OutPacket {
	o.code = code
	o.payload[0], o.payload[1] = uint8(code>>8), uint8(code&0xff)
	return o
}
func (o *rtPacket) Reset() OutPacket {
	o.offset = 2
	return o
}

// RealtimeUserBase represents basic user authorization information
type RealtimeUserBase struct {
	AppID        []byte
	StringUserID string
	UserID       []byte
	rawToken     []byte
	Permission   uint8
	Token        []byte
}

func newRTUserBaseFromString(token string) (t *RealtimeUserBase) {
	var decodedToken, err = base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil
	}
	args := bytes.Split(decodedToken, []byte{'#'})
	if len(args) != 2 {
		return nil
	}
	t = &RealtimeUserBase{
		StringUserID: string(args[0]),
		UserID:       args[0],
		rawToken:     args[1],
		Token:        decodedToken,
		Permission:   UserPermissionGuest,
	}
	return t
}

func (t *RealtimeUserBase) randomize() {
	t.rawToken = []byte(random.String(53, userIDCharset))
	var token = append(t.UserID, byte('#'))
	token = append(token, t.rawToken...)
	base64.StdEncoding.Encode(t.Token, token)
}
