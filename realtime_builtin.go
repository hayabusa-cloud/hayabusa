package hybs

import (
	"fmt"
	"math"
)

// realtime built-in middlewares and handlers
const (
	// client side error code
	RTErrorCodeBadRequest          uint16 = 0x0000 // bad request etc.
	RTErrorCodeUnauthorized        uint16 = 0x0001 // unauthenticated
	RTErrorCodeForbidden           uint16 = 0x0003 // short permission
	RTErrorCodeNotFound            uint16 = 0x0004 // undefined request code
	RTErrorCodeRoomCreateFailed    uint16 = 0x0100 // create room failed
	RTErrorCodeRoomEnterFailed     uint16 = 0x0101 // enter room failed
	RTErrorCodeRoomExitFailed      uint16 = 0x0102 // exit room failed
	RTErrorCodeRoomLockFailed      uint16 = 0x0103 // lock room failed
	RTErrorCodeRoomUnlockFailed    uint16 = 0x0104 // unlock room failed
	RTErrorCodeRoomAutoMatchFailed uint16 = 0x0110 // auto match failed
	// server side error code
	RTErrorCodeInternal uint16 = 0xffff // server internal error

	// request code
	RTRequestCodeUserValue   uint16 = 0x00fe // request code of broadcast user original value
	RTRequestCodeUserMessage uint16 = 0x00ff // request code of send user original message

	RTRequestCodeRoomCreate    uint16 = 0x0100
	RTRequestCodeRoomEnter     uint16 = 0x0101
	RTRequestCodeRoomExit      uint16 = 0x0102
	RTRequestCodeRoomLock      uint16 = 0x0103
	RTRequestCodeRoomUnlock    uint16 = 0x0104
	RTRequestCodeRoomAutoMatch uint16 = 0x0110
	RTRequestCodeRoomBroadcast uint16 = 0x01ff

	// event code
	RTEventCodeUserUpdated       uint16 = 0x8000
	RTEventCodeUserEnteredRoom   uint16 = 0x8002
	RTEventCodeUserExitedRoom    uint16 = 0x8003
	RTEventCodeUserEnteredServer uint16 = 0x8006
	RTEventCodeUserExitedServer  uint16 = 0x8007
	RTEventCodeUserEnteredAOIMap uint16 = 0x8010
	RTEventCodeUserExitedAOIMap  uint16 = 0x8011
	RTEventCodeUserMovedOnAOIMap uint16 = 0x8012
	RTEventCodeUserValue         uint16 = 0x80fe
	RTEventCodeUserMessage       uint16 = 0x80ff

	RTEventCodeRoomCreated   uint16 = 0x8100
	RTEventCodeRoomUpdated   uint16 = 0x8101
	RTEventCodeRoomRemoved   uint16 = 0x8102
	RTEventCodeRoomLocked    uint16 = 0x8103
	RTEventCodeRoomUnlocked  uint16 = 0x8104
	RTEventCodeRoomUserList  uint16 = 0x8105
	RTEventCodeRoomBroadcast uint16 = 0x81ff

	RTEventCodeErrorServer uint16 = 0x8ffe
	RTEventCodeErrorClient uint16 = 0x8fff
)

var (
	rtBuiltinHandlerMap = make(map[string]RTHandler)
)

var (
	rtBuiltinHandlerNop RTHandler = func(ctx RTCtx) {
		fmt.Println("nop")
	}

	rtBuiltinHandlerUserValue RTHandler = func(ctx RTCtx) {
		// check room
		if !ctx.IsInRoom() {
			return
		}
		var rawCtx, key = ctx.(*rtCtx), uint8(0)
		var room = rawCtx.server.roomTable.room(rawCtx.CurrentRoomID())
		if room == nil {
			return
		}
		// response
		rawCtx.ReadUint8(&key)
		var originBytes []byte = nil
		if rawCtx.in.offset < rawCtx.in.length {
			originBytes = rawCtx.in.payload[rawCtx.in.offset:rawCtx.in.length]
		}
		room.valueTable.Set(key, originBytes)
		ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
		ctx.SetEventCode(RTEventCodeUserValue)
		ctx.WriteUint8(key)
		if originBytes != nil {
			ctx.WriteBytesNoLen(originBytes)
		}
		ctx.BroadcastRoom()
	}

	rtBuiltinHandlerUserMessage RTHandler = func(ctx RTCtx) {
		// receive message
		var rawCtx = ctx.(*rtCtx)
		var destType, destId, command = uint8(0), uint16(0), uint16(0)
		rawCtx.ReadUint8(&destType).ReadUint16(&destId).ReadUint16(&command)
		var originMsg = rawCtx.in.payload[rawCtx.in.offset:rawCtx.in.length]
		// make packet
		ctx.SetEventCode(RTEventCodeUserMessage)
		ctx.WriteUint16(ctx.SessionID())
		ctx.WriteUint8(destType)
		ctx.WriteUint16(destId)
		ctx.WriteUint16(command)
		ctx.WriteBytesNoLen(originMsg)
		switch destType {
		case 0: // self
			ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
			ctx.Response()
			break
		case 1: // specified
			ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
			ctx.Send(destId)
			break
		case 2: // room
			ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
			ctx.BroadcastRoom()
			break
		case 3: // server
			ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
			ctx.BroadcastServer()
			break
		}
	}

	rtBuiltinHandlerRoomCreate RTHandler = func(ctx RTCtx) {
		var roomID, ok = ctx.Server().CreateRoom()
		ctx.ValueTable().Set(0, roomID).Set(1, ok)
	}
	rtBuiltinHandlerRoomRemove RTHandler = func(ctx RTCtx) {
		var roomID = ctx.ValueTable().GetUint16(0)
		ctx.Server().DestroyRoom(roomID)
	}

	rtBuiltinHandlerRoomEnter RTHandler = func(ctx RTCtx) {
		var roomID = uint16(0)
		ctx.ReadUint16(&roomID)
		var ok = ctx.Server().RoomEnterUser(roomID, ctx.SessionID())
		ctx.ValueTable().Set(0, roomID).Set(1, ok)
	}

	rtBuiltinHandlerRoomExit RTHandler = func(ctx RTCtx) {
		var roomID = ctx.CurrentRoomID()
		var ok = ctx.Server().RoomExitUser(roomID, ctx.SessionID())
		ctx.ValueTable().Set(0, roomID).Set(1, ok)
	}

	rtBuiltinHandlerRoomLock RTHandler = func(ctx RTCtx) {
		var server = ctx.(*rtCtx).server
		if !ctx.IsInRoom() {
			ctx.ErrorClientRequest(RTErrorCodeRoomLockFailed, "not in room")
			return
		}
		var ok = server.RoomLock(ctx.CurrentRoomID())
		if !ok {
			ctx.ErrorClientRequest(RTErrorCodeRoomLockFailed, "lock room failed")
			return
		}
		ctx.ValueTable().Set(0, ok)
		// room broadcast
		ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
		ctx.SetEventCode(RTEventCodeRoomLocked)
		ctx.WriteUint16(ctx.CurrentRoomID())
		ctx.BroadcastRoom()
		ctx.Reset()
	}

	rtBuiltinHandlerRoomUnlock RTHandler = func(ctx RTCtx) {
		var server = ctx.(*rtCtx).server
		if !ctx.IsInRoom() {
			ctx.ErrorClientRequest(RTErrorCodeRoomUnlockFailed, "not in room")
			return
		}
		var ok = server.RoomUnlock(ctx.CurrentRoomID())
		if !ok {
			ctx.ErrorClientRequest(RTErrorCodeRoomUnlockFailed, "lock room failed")
			return
		}
		ctx.ValueTable().Set(0, ok)
		// room broadcast
		ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
		ctx.SetEventCode(RTEventCodeRoomUnlocked)
		ctx.WriteUint16(ctx.CurrentRoomID())
		ctx.BroadcastRoom()
		ctx.Reset()
	}

	rtBuiltinHandlerRoomAutoMatch RTHandler = func(ctx RTCtx) {
		var roomMux = ctx.ReadBytes()
		var scoreCenter, scoreDiff float32 = 0, math.MaxFloat32
		var userMin, userMax uint16 = 0, 0
		ctx.ReadFloat32(&scoreCenter).ReadFloat32(&scoreDiff)
		ctx.ReadUint16(&userMin).ReadUint16(&userMax)
		ctx.ValueTable().Set(0, roomMux).Set(1, scoreCenter).Set(2, scoreDiff)
		ctx.ValueTable().Set(3, userMin).Set(4, userMax)
		// check parameters
		if userMin > userMax {
			ctx.(*rtCtx).ErrorClientRequest(RTErrorCodeRoomAutoMatchFailed, "bad room user num:%d>%d", userMax, userMin)
		}
		// random select matched room
		var room = ctx.(*rtCtx).server.roomTable.randomSelect(ctx.AppID(), roomMux, scoreCenter)
		if room == nil {
			// newly create room
			var newRoomID, ok = ctx.Server().CreateRoom()
			if !ok {
				ctx.ErrorServerInternal("run new room failed")
				return
			}
			// init set room parameters
			ctx.Server().RoomSetMatch(newRoomID, roomMux, scoreCenter, scoreDiff)
			ctx.Server().RoomSetUserNum(newRoomID, userMin, userMax)
			// room owner enter room
			ctx.Server().RoomEnterUser(newRoomID, ctx.SessionID())
			// broadcast new room created
			ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
			ctx.SetEventCode(RTEventCodeRoomCreated)
			ctx.WriteRoom(newRoomID)
			ctx.BroadcastServer()
			ctx.Reset()
			// return value
			ctx.ValueTable().Set(5, true).Set(6, newRoomID)
		} else {
			// user enter existed room
			ctx.Server().RoomEnterUser(room.id, ctx.SessionID())
			// broadcast update room information
			ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
			ctx.SetEventCode(RTEventCodeRoomUpdated)
			ctx.WriteRoom(room.id)
			ctx.BroadcastServer()
			ctx.Reset()
			// return value
			ctx.ValueTable().Set(5, false).Set(6, room.id)
		}
		// broadcast user entered room
		ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
		ctx.SetEventCode(RTEventCodeUserEnteredRoom)
		ctx.WriteUserStatus(ctx.SessionID())
		ctx.BroadcastRoom()
		return
	}
)

var (
	rtBuiltinMiddlewareMap = make(map[string]RTMiddleware)
)

var (
	rtBuiltinMiddleware0 = rtMiddlewareZero
	rtBuiltinMiddleware1 = rtMiddlewareIE
)

func rtBuiltinMiddlewareDefaultRedis(id string) RTMiddleware {
	return func(h RTHandler) RTHandler {
		return func(ctx RTCtx) {
			ctx.(*rtCtx).defaultRedis = id
			h(ctx)
		}
	}
}
func rtBuiltinMiddlewareDefaultCache(id string) RTMiddleware {
	return func(h RTHandler) RTHandler {
		return func(ctx RTCtx) {
			ctx.(*rtCtx).defaultCache = id
			h(ctx)
		}
	}
}
func rtBuiltinMiddlewareDefaultCsvMux(mux string) RTMiddleware {
	return func(h RTHandler) RTHandler {
		return func(ctx RTCtx) {
			ctx.(*rtCtx).defaultCsvMux = mux
			h(ctx)
		}
	}
}
func rtBuiltinMiddlewareWithCtxValue(k int, v interface{}) RTMiddleware {
	if k < 0 || k >= rtCtxValueTableSize {
		return rtBuiltinMiddleware1
	}
	return func(h RTHandler) RTHandler {
		return func(ctx RTCtx) {
			ctx.(*rtCtx).value.Set(k, v)
			h(ctx)
		}
	}
}

func RunRTHandler(ctx RTCtx, builtinHandler string) {
	var h, ok = rtBuiltinHandlerMap[builtinHandler]
	if !ok {
		ctx.ErrorServerInternal("handler %s not found", builtinHandler)
		return
	}
	h(ctx)
}

var (
	rtBuiltinSessionEventRoomBroadcastExit = func(server RealtimeServer, session RTSession) {
		var rawSession, rawServer = session.(*rtSession), server.(*hybsRealtimeServer)
		if rawSession.roomID != nil {
			var pkt = rawServer.outPacketPool.Get().(OutPacket)
			defer rawServer.outPacketPool.Put(pkt)
			pkt.Reset()
			pkt.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
			pkt.SetEventCode(RTEventCodeUserExitedRoom)
			pkt.WriteUint16(rawSession.id)
			pkt.WriteUint16(*rawSession.roomID)
			server.BroadcastPacketRoom(*rawSession.roomID, pkt)
		}
	}
)

func init() {
	// register builtin handlers
	rtBuiltinHandlerMap["Nop"] = rtBuiltinHandlerNop
	rtBuiltinHandlerMap["UserValue"] = rtBuiltinHandlerUserValue
	rtBuiltinHandlerMap["UserMessage"] = rtBuiltinHandlerUserMessage
	rtBuiltinHandlerMap["RoomCreate"] = rtBuiltinHandlerRoomCreate
	rtBuiltinHandlerMap["RoomRemove"] = rtBuiltinHandlerRoomRemove
	rtBuiltinHandlerMap["RoomEnter"] = rtBuiltinHandlerRoomEnter
	rtBuiltinHandlerMap["RoomExit"] = rtBuiltinHandlerRoomExit
	rtBuiltinHandlerMap["RoomLock"] = rtBuiltinHandlerRoomLock
	rtBuiltinHandlerMap["RoomUnlock"] = rtBuiltinHandlerRoomUnlock
	rtBuiltinHandlerMap["RoomAutoMatch"] = rtBuiltinHandlerRoomAutoMatch
	for id, h := range rtBuiltinHandlerMap {
		rtProcessorHandlers[0] = append(rtProcessorHandlers[0], rtHandlerDefineInfo{id: id, h: h})
	}

	// register builtin middlewares
	rtBuiltinMiddlewareMap["Nop"] = rtBuiltinMiddleware0
	rtBuiltinMiddlewareMap["Origin"] = rtBuiltinMiddleware1
	for id, m := range rtBuiltinMiddlewareMap {
		defaultMiddlewareDefines = append(defaultMiddlewareDefines, rtMiddlewareDefineInfo{id: id, m: m})
	}
}
