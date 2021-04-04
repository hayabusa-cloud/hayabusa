package hybs

import (
	"fmt"
	"math"
)

type RealtimeErrorCode = uint16
type RealtimeRequestCode = uint16
type RealtimeEventCode = uint16

const (
	// client side error code
	RealtimeErrorCodeBadRequest          RealtimeErrorCode = 0x0000 // bad request etc.
	RealtimeErrorCodeUnauthorized        RealtimeErrorCode = 0x0001 // unauthenticated
	RealtimeErrorCodeForbidden           RealtimeErrorCode = 0x0003 // short permission
	RealtimeErrorCodeNotFound            RealtimeErrorCode = 0x0004 // undefined request code
	RealtimeErrorCodeRoomCreateFailed    RealtimeErrorCode = 0x0100 // create room failed
	RealtimeErrorCodeRoomEnterFailed     RealtimeErrorCode = 0x0101 // enter room failed
	RealtimeErrorCodeRoomExitFailed      RealtimeErrorCode = 0x0102 // exit room failed
	RealtimeErrorCodeRoomLockFailed      RealtimeErrorCode = 0x0103 // lock room failed
	RealtimeErrorCodeRoomUnlockFailed    RealtimeErrorCode = 0x0104 // unlock room failed
	RealtimeErrorCodeRoomAutoMatchFailed RealtimeErrorCode = 0x0110 // auto match failed
	// server side error code
	RealtimeErrorCodeInternal RealtimeErrorCode = 0xffff // server internal error

	// request code
	RealtimeRequestCodeUserValue   RealtimeRequestCode = 0x00fe // request code of broadcast user original value
	RealtimeRequestCodeUserMessage RealtimeRequestCode = 0x00ff // request code of send user original message

	RealtimeRequestCodeRoomCreate    RealtimeRequestCode = 0x0100
	RealtimeRequestCodeRoomEnter     RealtimeRequestCode = 0x0101
	RealtimeRequestCodeRoomExit      RealtimeRequestCode = 0x0102
	RealtimeRequestCodeRoomLock      RealtimeRequestCode = 0x0103
	RealtimeRequestCodeRoomUnlock    RealtimeRequestCode = 0x0104
	RealtimeRequestCodeRoomAutoMatch RealtimeRequestCode = 0x0110
	RealtimeRequestCodeRoomBroadcast RealtimeRequestCode = 0x01ff

	// event code
	RealtimeEventCodeUserUpdated       RealtimeEventCode = 0x8000
	RealtimeEventCodeUserEnteredRoom   RealtimeEventCode = 0x8002
	RealtimeEventCodeUserExitedRoom    RealtimeEventCode = 0x8003
	RealtimeEventCodeUserEnteredServer RealtimeEventCode = 0x8006
	RealtimeEventCodeUserExitedServer  RealtimeEventCode = 0x8007
	RealtimeEventCodeUserEnteredAOIMap RealtimeEventCode = 0x8010
	RealtimeEventCodeUserExitedAOIMap  RealtimeEventCode = 0x8011
	RealtimeEventCodeUserMovedOnAOIMap RealtimeEventCode = 0x8012
	RealtimeEventCodeUserValue         RealtimeEventCode = 0x80fe
	RealtimeEventCodeUserMessage       RealtimeEventCode = 0x80ff

	RealtimeEventCodeRoomCreated   RealtimeEventCode = 0x8100
	RealtimeEventCodeRoomUpdated   RealtimeEventCode = 0x8101
	RealtimeEventCodeRoomRemoved   RealtimeEventCode = 0x8102
	RealtimeEventCodeRoomLocked    RealtimeEventCode = 0x8103
	RealtimeEventCodeRoomUnlocked  RealtimeEventCode = 0x8104
	RealtimeEventCodeRoomUserList  RealtimeEventCode = 0x8105
	RealtimeEventCodeRoomBroadcast RealtimeEventCode = 0x81ff

	RealtimeEventCodeErrorServer RealtimeEventCode = 0x8ffe
	RealtimeEventCodeErrorClient RealtimeEventCode = 0x8fff
)

var (
	rtBuiltinHandlerMap = make(map[string]RealtimeHandler)
)

var (
	rtBuiltinHandlerNop RealtimeHandler = func(ctx RealtimeCtx) {
		fmt.Println("nop")
	}

	rtBuiltinHandlerUserValue RealtimeHandler = func(ctx RealtimeCtx) {
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
		ctx.SetEventCode(RealtimeEventCodeUserValue)
		ctx.WriteUint8(key)
		if originBytes != nil {
			ctx.WriteBytesNoLen(originBytes)
		}
		ctx.BroadcastRoom()
	}

	rtBuiltinHandlerUserMessage RealtimeHandler = func(ctx RealtimeCtx) {
		// receive message
		var rawCtx = ctx.(*rtCtx)
		var destType, destId, command = uint8(0), uint16(0), uint16(0)
		rawCtx.ReadUint8(&destType).ReadUint16(&destId).ReadUint16(&command)
		var originMsg = rawCtx.in.payload[rawCtx.in.offset:rawCtx.in.length]
		// make packet
		ctx.SetEventCode(RealtimeEventCodeUserMessage)
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

	rtBuiltinHandlerRoomCreate RealtimeHandler = func(ctx RealtimeCtx) {
		var roomID, ok = ctx.Server().CreateRoom()
		ctx.ValueTable().Set(0, roomID).Set(1, ok)
	}
	rtBuiltinHandlerRoomRemove RealtimeHandler = func(ctx RealtimeCtx) {
		var roomID = ctx.ValueTable().GetUint16(0)
		ctx.Server().DestroyRoom(roomID)
	}

	rtBuiltinHandlerRoomEnter RealtimeHandler = func(ctx RealtimeCtx) {
		var roomID = uint16(0)
		ctx.ReadUint16(&roomID)
		var ok = ctx.Server().RoomEnterUser(roomID, ctx.SessionID())
		ctx.ValueTable().Set(0, roomID).Set(1, ok)
	}

	rtBuiltinHandlerRoomExit RealtimeHandler = func(ctx RealtimeCtx) {
		var roomID = ctx.CurrentRoomID()
		var ok = ctx.Server().RoomExitUser(roomID, ctx.SessionID())
		ctx.ValueTable().Set(0, roomID).Set(1, ok)
	}

	rtBuiltinHandlerRoomLock RealtimeHandler = func(ctx RealtimeCtx) {
		var server = ctx.(*rtCtx).server
		if !ctx.IsInRoom() {
			ctx.ErrorClientRequest(RealtimeErrorCodeRoomLockFailed, "not in room")
			return
		}
		var ok = server.RoomLock(ctx.CurrentRoomID())
		if !ok {
			ctx.ErrorClientRequest(RealtimeErrorCodeRoomLockFailed, "lock room failed")
			return
		}
		ctx.ValueTable().Set(0, ok)
		// room broadcast
		ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
		ctx.SetEventCode(RealtimeEventCodeRoomLocked)
		ctx.WriteUint16(ctx.CurrentRoomID())
		ctx.BroadcastRoom()
		ctx.Reset()
	}

	rtBuiltinHandlerRoomUnlock RealtimeHandler = func(ctx RealtimeCtx) {
		var server = ctx.(*rtCtx).server
		if !ctx.IsInRoom() {
			ctx.ErrorClientRequest(RealtimeErrorCodeRoomUnlockFailed, "not in room")
			return
		}
		var ok = server.RoomUnlock(ctx.CurrentRoomID())
		if !ok {
			ctx.ErrorClientRequest(RealtimeErrorCodeRoomUnlockFailed, "lock room failed")
			return
		}
		ctx.ValueTable().Set(0, ok)
		// room broadcast
		ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
		ctx.SetEventCode(RealtimeEventCodeRoomUnlocked)
		ctx.WriteUint16(ctx.CurrentRoomID())
		ctx.BroadcastRoom()
		ctx.Reset()
	}

	rtBuiltinHandlerRoomAutoMatch RealtimeHandler = func(ctx RealtimeCtx) {
		var roomMux = ctx.ReadBytes()
		var scoreCenter, scoreDiff float32 = 0, math.MaxFloat32
		var userMin, userMax uint16 = 0, 0
		ctx.ReadFloat32(&scoreCenter).ReadFloat32(&scoreDiff)
		ctx.ReadUint16(&userMin).ReadUint16(&userMax)
		ctx.ValueTable().Set(0, roomMux).Set(1, scoreCenter).Set(2, scoreDiff)
		ctx.ValueTable().Set(3, userMin).Set(4, userMax)
		// check parameters
		if userMin > userMax {
			ctx.(*rtCtx).ErrorClientRequest(RealtimeErrorCodeRoomAutoMatchFailed, "bad room user num:%d>%d", userMax, userMin)
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
			ctx.SetEventCode(RealtimeEventCodeRoomCreated)
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
			ctx.SetEventCode(RealtimeEventCodeRoomUpdated)
			ctx.WriteRoom(room.id)
			ctx.BroadcastServer()
			ctx.Reset()
			// return value
			ctx.ValueTable().Set(5, false).Set(6, room.id)
		}
		// broadcast user entered room
		ctx.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
		ctx.SetEventCode(RealtimeEventCodeUserEnteredRoom)
		ctx.WriteUserStatus(ctx.SessionID())
		ctx.BroadcastRoom()
		return
	}
)

var (
	rtBuiltinMiddlewareMap = make(map[string]RealtimeMiddleware)
)

var (
	rtBuiltinMiddleware0 = rtMiddlewareZero
	rtBuiltinMiddleware1 = rtMiddlewareIE
)

func rtBuiltinMiddlewareDefaultRedis(id string) RealtimeMiddleware {
	return func(h RealtimeHandler) RealtimeHandler {
		return func(ctx RealtimeCtx) {
			ctx.(*rtCtx).defaultRedis = id
			h(ctx)
		}
	}
}
func rtBuiltinMiddlewareDefaultCache(id string) RealtimeMiddleware {
	return func(h RealtimeHandler) RealtimeHandler {
		return func(ctx RealtimeCtx) {
			ctx.(*rtCtx).defaultCache = id
			h(ctx)
		}
	}
}
func rtBuiltinMiddlewareDefaultCsvMux(mux string) RealtimeMiddleware {
	return func(h RealtimeHandler) RealtimeHandler {
		return func(ctx RealtimeCtx) {
			ctx.(*rtCtx).defaultCsvMux = mux
			h(ctx)
		}
	}
}
func rtBuiltinMiddlewareWithCtxValue(k int, v interface{}) RealtimeMiddleware {
	if k < 0 || k >= rtCtxValueTableSize {
		return rtBuiltinMiddleware1
	}
	return func(h RealtimeHandler) RealtimeHandler {
		return func(ctx RealtimeCtx) {
			ctx.(*rtCtx).value.Set(k, v)
			h(ctx)
		}
	}
}

func RunRealtimeHandler(ctx RealtimeCtx, builtinHandler string) {
	var h, ok = rtBuiltinHandlerMap[builtinHandler]
	if !ok {
		ctx.ErrorServerInternal("handler %s not found", builtinHandler)
		return
	}
	h(ctx)
}

var (
	rtBuiltinSessionEventRoomBroadcastExit = func(server RealtimeServer, session RealtimeSession) {
		var rawSession, rawServer = session.(*rtSession), server.(*hybsRealtimeServer)
		if rawSession.roomID != nil {
			var pkt = rawServer.outPacketPool.Get().(OutPacket)
			defer rawServer.outPacketPool.Put(pkt)
			pkt.Reset()
			pkt.SetHeader3(rtHeaderPV0, rtHeaderPDSC, rtHeaderCmdBuiltin)
			pkt.SetEventCode(RealtimeEventCodeUserExitedRoom)
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
