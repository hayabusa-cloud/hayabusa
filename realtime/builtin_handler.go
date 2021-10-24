package realtime

import (
	"fmt"
	"math"
)

type ErrorCode = uint16
type RequestCode = uint16
type EventCode = uint16
type PermissionCode = int32

const (
	ErrorCodeBadRequest         ErrorCode = 0x0000 // bad request etc.
	ErrorCodeUnauthorized       ErrorCode = 0x0001 // unauthenticated
	ErrorCodeForbidden          ErrorCode = 0x0003 // short permission
	ErrorCodeNotFound           ErrorCode = 0x0004 // undefined request code
	ErrorCodeTooManyRequests    ErrorCode = 0x001d // too many requests
	ErrorCodeConnectionClosed   ErrorCode = 0x007d // connection closed
	ErrorCodeTooManyConnections ErrorCode = 0x007e // too many connections
	ErrorCodeServerStopped      ErrorCode = 0x007f // server stopper

	ErrorCodeRoomCreateError    ErrorCode = 0x0100 // create room error
	ErrorCodeRoomEnterError     ErrorCode = 0x0101 // enter room error
	ErrorCodeRoomExitError      ErrorCode = 0x0102 // exit room error
	ErrorCodeRoomLockError      ErrorCode = 0x0103 // lock room error
	ErrorCodeRoomUnlockError    ErrorCode = 0x0104 // unlock room error
	ErrorCodeRoomAutoMatchError ErrorCode = 0x0110 // auto match error

	ErrorCodeInternal ErrorCode = 0xffff // server internal error

	RequestCodeUserValue   RequestCode = 0x00fe // request code of broadcast user original value
	RequestCodeUserMessage RequestCode = 0x00ff // request code of send user original message

	RequestCodeRoomCreate    RequestCode = 0x0100
	RequestCodeRoomEnter     RequestCode = 0x0101
	RequestCodeRoomExit      RequestCode = 0x0102
	RequestCodeRoomLock      RequestCode = 0x0103
	RequestCodeRoomUnlock    RequestCode = 0x0104
	RequestCodeRoomAutoMatch RequestCode = 0x0110
	RequestCodeRoomBroadcast RequestCode = 0x01ff

	EventCodeUserUpdated       EventCode = 0x8000
	EventCodeUserEnteredRoom   EventCode = 0x8002
	EventCodeUserExitedRoom    EventCode = 0x8003
	EventCodeUserEnteredServer EventCode = 0x8006
	EventCodeUserExitedServer  EventCode = 0x8007
	EventCodeUserEnteredAOIMap EventCode = 0x8010
	EventCodeUserExitedAOIMap  EventCode = 0x8011
	EventCodeUserMovedOnAOIMap EventCode = 0x8012
	EventCodeUserValue         EventCode = 0x80fe
	EventCodeUserMessage       EventCode = 0x80ff

	EventCodeRoomCreated   EventCode = 0x8100
	EventCodeRoomUpdated   EventCode = 0x8101
	EventCodeRoomRemoved   EventCode = 0x8102
	EventCodeRoomLocked    EventCode = 0x8103
	EventCodeRoomUnlocked  EventCode = 0x8104
	EventCodeRoomUserList  EventCode = 0x8105
	EventCodeRoomBroadcast EventCode = 0x81ff

	EventCodeErrorServer EventCode = 0x8ffe
	EventCodeErrorClient EventCode = 0x8fff
)

const (
	PermissionCodeAuthorized = 1 << (27 - iota)
	PermissionCodeReadonlyAccess
	PermissionCodeNormalAccess
	PermissionCodeIgnoreMaintenance
	PermissionCodeIgnoreEventLimit
	PermissionCodeCustomerSupportTicketAccess
	PermissionCodeSystemMailAccess
	PermissionCodeUserManagementAccess
	PermissionCodeNotificationAccess
	PermissionCodeAdminAccess

	RolePermissionUnauthorized = 0
	RolePermissionAuthorized   = PermissionCodeAuthorized
	RolePermissionNormalUser   = RolePermissionAuthorized | PermissionCodeReadonlyAccess | PermissionCodeNormalAccess
	RolePermissionStaff        = RolePermissionNormalUser | PermissionCodeIgnoreMaintenance | PermissionCodeIgnoreEventLimit
	RolePermissionOperator     = RolePermissionStaff |
		PermissionCodeCustomerSupportTicketAccess |
		PermissionCodeSystemMailAccess |
		PermissionCodeUserManagementAccess |
		PermissionCodeNotificationAccess
	RolePermissionOperatorManager = RolePermissionOperator | PermissionCodeAdminAccess
	RolePermissionProprietor      = 0x7fffffff
)

var (
	builtinHandlerMap = make(map[string]Handler)
)

var (
	builtinHandlerNop Handler = func(ctx Ctx) {
		fmt.Println("nop")
	}

	builtinHandlerUserValue Handler = func(c Ctx) {
		// check r
		if !c.IsInRoom() {
			return
		}
		key := uint8(0)
		var r = c.(*ctx).server.roomTable.room(c.CurrentRoomID())
		if r == nil {
			return
		}
		// response
		c.ReadUint8(&key)
		var originBytes []byte = nil
		if c.(*ctx).request.offset < c.(*ctx).request.length {
			originBytes = c.(*ctx).request.payload[c.(*ctx).request.offset:c.(*ctx).request.length]
		}
		r.valueTable.Set(key, originBytes)
		c.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
		c.SetEventCode(EventCodeUserValue)
		c.WriteUint8(key)
		if originBytes != nil {
			c.WriteBytesNoLen(originBytes)
		}
		c.BroadcastRoom()
	}

	builtinHandlerUserMessage Handler = func(c Ctx) {
		// receive message
		var destType, destId, command = uint8(0), uint16(0), uint16(0)
		c.ReadUint8(&destType).ReadUint16(&destId).ReadUint16(&command)
		var originMsg = c.(*ctx).request.payload[c.(*ctx).request.offset:c.(*ctx).request.length]
		// make packet
		c.SetEventCode(EventCodeUserMessage)
		c.WriteUint16(c.ConnectionID())
		c.WriteUint8(destType)
		c.WriteUint16(destId)
		c.WriteUint16(command)
		c.WriteBytesNoLen(originMsg)
		switch destType {
		case 0: // self
			c.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
			c.Send(c.ConnectionID())
		case 1: // specified
			c.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
			c.Send(destId)
		case 2: // room
			c.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
			c.BroadcastRoom()
		case 3: // server
			c.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
			c.BroadcastServer()
		}
	}

	builtinHandlerRoomCreate Handler = func(ctx Ctx) {
		var roomID, ok = ctx.Server().CreateRoom()
		ctx.ValueTable().Set(0, roomID).Set(1, ok)
	}
	builtinHandlerRoomRemove Handler = func(ctx Ctx) {
		var roomID = ctx.ValueTable().GetUint16(0)
		ctx.Server().DestroyRoom(roomID)
	}

	builtinHandlerRoomEnter Handler = func(ctx Ctx) {
		var roomID = uint16(0)
		ctx.ReadUint16(&roomID)
		var ok = ctx.Server().RoomEnterUser(roomID, ctx.ConnectionID())
		ctx.ValueTable().Set(0, roomID).Set(1, ok)
	}

	builtinHandlerRoomExit Handler = func(ctx Ctx) {
		var roomID = ctx.CurrentRoomID()
		var ok = ctx.Server().RoomExitUser(roomID, ctx.ConnectionID())
		ctx.ValueTable().Set(0, roomID).Set(1, ok)
	}

	builtinHandlerRoomLock Handler = func(c Ctx) {
		var server = c.(*ctx).server
		if !c.IsInRoom() {
			c.ErrorClientRequest(ErrorCodeRoomLockError, "not in room")
			return
		}
		var ok = server.RoomLock(c.CurrentRoomID())
		if !ok {
			c.ErrorClientRequest(ErrorCodeRoomLockError, "lock room error")
			return
		}
		c.ValueTable().Set(0, ok)
		// room broadcast
		c.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
		c.SetEventCode(EventCodeRoomLocked)
		c.WriteUint16(c.CurrentRoomID())
		c.BroadcastRoom()
		c.Reset()
	}

	builtinHandlerRoomUnlock Handler = func(c Ctx) {
		var server = c.(*ctx).server
		if !c.IsInRoom() {
			c.ErrorClientRequest(ErrorCodeRoomUnlockError, "not in room")
			return
		}
		var ok = server.RoomUnlock(c.CurrentRoomID())
		if !ok {
			c.ErrorClientRequest(ErrorCodeRoomUnlockError, "lock room error")
			return
		}
		c.ValueTable().Set(0, ok)
		// room broadcast
		c.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
		c.SetEventCode(EventCodeRoomUnlocked)
		c.WriteUint16(c.CurrentRoomID())
		c.BroadcastRoom()
		c.Reset()
	}

	builtinHandlerRoomAutoMatch Handler = func(c Ctx) {
		var (
			roomMux                        = c.ReadBytes()
			scoreCenter, scoreDiff float32 = 0, math.MaxFloat32
			userNum                uint16  = 0
		)
		c.ReadFloat32(&scoreCenter).ReadFloat32(&scoreDiff).ReadUint16(&userNum)
		c.ValueTable().Set(0, roomMux).Set(1, scoreCenter).Set(2, scoreDiff).Set(3, userNum)
		// random select matched room
		var room = c.(*ctx).server.roomTable.randomSelect(c.AppID(), roomMux, scoreCenter)
		if room == nil {
			// newly create room
			var newRoomID, ok = c.Server().CreateRoom()
			if !ok {
				c.ErrorServerInternal("run new room error")
				return
			}
			// init set room parameters
			c.Server().RoomSetMatch(newRoomID, roomMux, scoreCenter, scoreDiff)
			c.Server().RoomSetUserNum(newRoomID, userNum)
			// room owner enter room
			c.Server().RoomEnterUser(newRoomID, c.ConnectionID())
			// broadcast new room created
			c.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
			c.SetEventCode(EventCodeRoomCreated)
			c.WriteRoom(newRoomID)
			c.BroadcastServer()
			c.Reset()
			// return value
			c.ValueTable().Set(4, true).Set(5, newRoomID)
		} else {
			// user enter existed room
			c.Server().RoomEnterUser(room.id, c.ConnectionID())
			// broadcast update room information
			c.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
			c.SetEventCode(EventCodeRoomUpdated)
			c.WriteRoom(room.id)
			c.BroadcastServer()
			c.Reset()
			// return value
			c.ValueTable().Set(4, false).Set(5, room.id)
		}
		// broadcast user entered room
		c.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
		c.SetEventCode(EventCodeUserEnteredRoom)
		c.WriteUserStatus(c.ConnectionID())
		c.BroadcastRoom()
	}
)

var (
	builtinMiddlewareMap = make(map[string]Middleware)
)

var (
	builtinMiddleware0 = middlewareZero
	builtinMiddleware1 = middlewareIE
)

func builtinMiddlewareDefaultRedis(id string) Middleware {
	return func(h Handler) Handler {
		return func(c Ctx) {
			c.(*ctx).defaultRedis = id
			h(c)
		}
	}
}
func builtinMiddlewareDefaultCache(id string) Middleware {
	return func(h Handler) Handler {
		return func(c Ctx) {
			c.(*ctx).defaultCache = id
			h(c)
		}
	}
}
func builtinMiddlewareDefaultCsvMux(mux string) Middleware {
	return func(h Handler) Handler {
		return func(c Ctx) {
			c.(*ctx).defaultCsvMux = mux
			h(c)
		}
	}
}

//lint:ignore U1000 todo
func builtinMiddlewareWithCtxValue(k int, v interface{}) Middleware {
	if k < 0 || k >= ctxValueTableSize {
		return builtinMiddleware1
	}
	return func(h Handler) Handler {
		return func(c Ctx) {
			c.(*ctx).value.Set(k, v)
			h(c)
		}
	}
}

func RunHandler(ctx Ctx, builtinHandler string) {
	var h, ok = builtinHandlerMap[builtinHandler]
	if !ok {
		ctx.ErrorServerInternal("handler %s not found", builtinHandler)
		return
	}
	h(ctx)
}

var (
	builtinEventRoomBroadcastExit = func(s Server, conn Connection) {
		var rawConnection, rawServer = conn.(*connection), s.(*serverImpl)
		if rawConnection.roomID != nil {
			var message = rawServer.responsePool.Get().(Response)
			defer rawServer.responsePool.Put(message)
			message.Reset()
			message.SetHeader2(headerProtocolVer0, headerCmdBuiltin)
			message.SetEventCode(EventCodeUserExitedRoom)
			message.WriteUint16(rawConnection.id)
			message.WriteUint16(*rawConnection.roomID)
			s.BroadcastMessageRoom(*rawConnection.roomID, message)
		}
	}
)

func init() {
	// register builtin handlers
	builtinHandlerMap["Nop"] = builtinHandlerNop
	builtinHandlerMap["UserValue"] = builtinHandlerUserValue
	builtinHandlerMap["UserMessage"] = builtinHandlerUserMessage
	builtinHandlerMap["RoomCreate"] = builtinHandlerRoomCreate
	builtinHandlerMap["RoomRemove"] = builtinHandlerRoomRemove
	builtinHandlerMap["RoomEnter"] = builtinHandlerRoomEnter
	builtinHandlerMap["RoomExit"] = builtinHandlerRoomExit
	builtinHandlerMap["RoomLock"] = builtinHandlerRoomLock
	builtinHandlerMap["RoomUnlock"] = builtinHandlerRoomUnlock
	builtinHandlerMap["RoomAutoMatch"] = builtinHandlerRoomAutoMatch
	for id, h := range builtinHandlerMap {
		processorHandlers[0] = append(processorHandlers[0], handlerTuple{id: id, h: h})
	}

	// register builtin middlewares
	builtinMiddlewareMap["Nop"] = builtinMiddleware0
	builtinMiddlewareMap["Origin"] = builtinMiddleware1
	for id, m := range builtinMiddlewareMap {
		defaultMiddlewareTupleList = append(defaultMiddlewareTupleList, middlewareTuple{id: id, m: m})
	}
}
