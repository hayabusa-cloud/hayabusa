package hybs

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type rtModule chan *rtCtx

const (
	rtModuleTableSizeBit = 8
	rtModuleTableSize    = 1 << rtModuleTableSizeBit
)

// rtModule divides the business logic into several independent categories
// business logic within the same rtModule will be executed in a single goroutine,
// therefore, it is no need to consider concurrency safety problems if run in the same rtModule
type rtModuleTable [rtModuleTableSize]rtModule

func newRTModule() rtModule {
	return make(chan *rtCtx)
}

func (p rtModule) start(rs *hybsRealtimeServer) {
	for ctx := range p {
		var cmd, code, h = ctx.in.header & 0xf, ctx.in.code, RTHandler(nil)
		if cmd == rtHeaderCmdBuiltin {
			h = rs.builtinHandlerTable[code]
		} else if cmd == rtHeaderCmdOriginal {
			h = rs.handlerTable[code]
		}
		if h != nil {
			h(ctx)
		}
		ctx.dispose()
	}
}

const (
	rtRoomTableSizeBit = 14
	rtRoomTableSize    = 1 << rtRoomTableSizeBit
	rtRoomNumMask      = rtRoomTableSize | (rtRoomTableSize << 1)
)

type rtRoomTable [rtRoomTableSize]*rtRoom

func (t rtRoomTable) room(roomID uint16) *rtRoom {
	if roomID&rtRoomNumMask != 0 {
		return nil
	}
	return t[roomID]
}

type rtRoomSortInfo struct {
	room   *rtRoom
	weight float32
	r      float32
}
type rtRoomSortSlice []*rtRoomSortInfo

func (s rtRoomSortSlice) Less(i, j int) bool {
	return s[i].r > s[j].r
}
func (s rtRoomSortSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s rtRoomSortSlice) Len() int {
	return len(s)
}
func (t rtRoomTable) randomSelect(appID []byte, mux []byte, scoreCenter float32) *rtRoom {
	var roomList = make(rtRoomSortSlice, 0, rtRoomTableSize)
	for i := uint16(0); i < rtRoomTableSize; i++ {
		if t[i] == nil {
			continue
		}
		var loopRoom = t[i]
		if loopRoom.status.Status != rtRoomStatusOpening {
			continue
		}
		if loopRoom.userNum() >= loopRoom.status.MaxUser {
			continue
		}
		if !bytes.Equal(appID, loopRoom.owner.AppID()) {
			continue
		}
		if !loopRoom.status.isMatchMux(mux) {
			continue
		}
		var weight = 0.5 * float32(loopRoom.userNum()+1)
		weight = weight * weight
		var diff = (loopRoom.status.ScoreCenter - scoreCenter) / loopRoom.status.ScoreDiff
		if diff < 0 {
			diff = -diff
		}
		weight = weight / float32(math.Exp(float64(0.5*diff)))
		var r = float32(math.Pow(rand.Float64(), float64(1.0/weight)))
		roomList = append(roomList, &rtRoomSortInfo{
			room:   loopRoom,
			weight: weight,
			r:      r,
		})
	}
	roomList = append(roomList, &rtRoomSortInfo{
		room:   nil,
		weight: 0.1,
		r:      0.001,
	})
	sort.Sort(roomList)
	return roomList[0].room
}

type RTRoomStatus struct {
	ID          uint16
	RoomType    uint8
	Status      uint8
	CreatedAt   Time
	UntilAt     Time
	MinUser     uint16
	MaxUser     uint16
	ScoreCenter float32
	ScoreDiff   float32
	MatchMux    []byte
}

func (r *RTRoomStatus) writeToPacket(pkt *rtPacket) {
	pkt.WriteUint16(r.ID).WriteUint8(r.RoomType).WriteUint8(r.Status)
	pkt.WriteUint16(r.MinUser).WriteUint16(r.MaxUser)
	pkt.WriteFloat32(r.ScoreCenter).WriteFloat32(r.ScoreDiff)
}
func (r *RTRoomStatus) isMatchMux(mux []byte) bool {
	if mux == nil || r.MatchMux == nil {
		return false
	}
	return bytes.Equal(r.MatchMux, mux)
}

type rtRoom struct {
	id         uint16
	mu         sync.RWMutex
	owner      *rtRoomUser
	status     *RTRoomStatus
	userTable  []*rtRoomUser
	valueTable rtRoomValueTable
	server     *hybsRealtimeServer
	entering   func(user *rtRoomUser)
	entered    func(user *rtRoomUser)
	exiting    func(user *rtRoomUser)
	exited     func(user *rtRoomUser)
}

func rtRoomInitCreate() *rtRoom {
	var roomStatus = &RTRoomStatus{
		RoomType:    rtRoomTypeMatching,
		Status:      rtRoomStatusOpening,
		CreatedAt:   Now(),
		UntilAt:     Now(),
		MinUser:     0,
		MaxUser:     0,
		ScoreCenter: 0,
		ScoreDiff:   math.MaxFloat32,
		MatchMux:    make([]byte, 0),
	}
	return &rtRoom{
		status:     roomStatus,
		mu:         sync.RWMutex{},
		userTable:  make([]*rtRoomUser, 0, 0x10),
		valueTable: rtRoomValueTable{},
		entering:   func(user *rtRoomUser) {},
		entered:    func(user *rtRoomUser) {},
		exiting:    func(user *rtRoomUser) {},
		exited:     func(user *rtRoomUser) {},
	}
}

type rtRoomUser struct {
	*rtSession
	room      *rtRoom
	status    uint8
	enteredAt Time
	untilAt   Time
}

func (u *rtRoomUser) writeToPacket(pkt *rtPacket) {
	pkt.WriteUint16(u.id).WriteBytes(u.UserID()).WriteUint8(u.status)
}

const (
	rtRoomTypeNormal   = 0 // ノーマルルーム
	rtRoomTypeMatching = 1 // 自動マッチング

	rtRoomStatusOpening = 0 // ユーザーを待っている状態
	rtRoomStatusLocking = 1 // ゲーム最中の状態

	rtRoomUserStatusWaiting = 0
	rtRoomUserStatusReady   = 1
	rtRoomUserStatusGaming  = 2
	rtRoomUserStatusExiting = 3

	rtRoomUserNumMax uint16 = 1 << 12
)

func newRTRoom(roomID uint16, server *hybsRealtimeServer) *rtRoom {
	var defaultRoomLife = time.Hour * 36
	var newRoom = server.roomPool.Get().(*rtRoom)
	newRoom.id = roomID
	newRoom.status.ID = roomID
	newRoom.status.RoomType = rtRoomTypeMatching
	newRoom.status.Status = rtRoomStatusOpening
	newRoom.status.CreatedAt = Now()
	newRoom.status.UntilAt = Now().Add(defaultRoomLife)
	newRoom.userTable = make([]*rtRoomUser, 0, 0x10)
	return newRoom
}

func (r *rtRoom) UserIDList() [][]byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var ret = make([][]byte, 0, len(r.userTable))
	for _, u := range r.userTable {
		if u == nil {
			continue
		}
		ret = append(ret, u.UserID())
	}
	return ret
}

func (r *rtRoom) userAdd(session *rtSession) bool {
	var defaultRoomUserLife = time.Hour * 72
	var newUser = &rtRoomUser{
		room:      r,
		rtSession: session,
		status:    rtRoomUserStatusWaiting,
		enteredAt: Now(),
		untilAt:   Now().Add(defaultRoomUserLife),
	}
	r.entering(newUser)
	r.mu.Lock()
	defer r.mu.Unlock()
	var (
		position uint16 = 0
		ok              = false
	)
	for i, loopUser := range r.userTable {
		// over max user num
		if uint16(i) >= r.status.MaxUser {
			return false
		}
		// position not empty
		if loopUser != nil {
			continue
		}
		position, ok = uint16(i), true
		r.userTable[position] = newUser
		break
	}
	if !ok {
		// append user to list
		if len(r.userTable) < int(r.status.MaxUser) {
			r.userTable = append(r.userTable, newUser)
			position = uint16(len(r.userTable)&0xffff) - 1
		} else {
			return false
		}
	}
	// set room owner
	if len(r.userTable) == 1 {
		r.owner = newUser
	}
	// set session parameters
	session.roomID = &r.id
	session.roomPos = &position
	r.entered(newUser)
	// log
	var entry = r.server.logger.WithField("session", fmt.Sprintf("%x", newUser.id))
	entry.WithField("room", fmt.Sprintf("%x", r.id)).Infof("entered room")
	return true
}
func (r *rtRoom) userAddByID(sessionID uint16) bool {
	var session = r.server.session(sessionID)
	if session == nil {
		return false
	}
	return r.userAdd(session)
}

func (r *rtRoom) userRemove(session *rtSession) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < len(r.userTable); i++ {
		if r.userTable[i] == nil {
			continue
		}
		if r.userTable[i].rtSession.id != session.id {
			continue
		}
		var roomUser = r.userTable[i]
		r.exiting(roomUser)
		// r.userTable = append(r.userTable[:i], r.userTable[i+1:]...)
		r.userTable[i] = nil
		r.exited(roomUser)
		// set session parameters
		session.roomID = nil
		session.roomPos = nil
		break
	}
	// log
	var entry = r.server.logger.WithField("session", fmt.Sprintf("%x", session.id))
	entry.WithField("room", fmt.Sprintf("%x", r.id)).Infof("exited room")
}

func (r *rtRoom) userRemoveByPosition(pos int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var roomUser = r.userTable[pos]
	if roomUser == nil {
		return
	}
	r.exiting(roomUser)
	// r.userTable = append(r.userTable[:index], r.userTable[index+1:]...)
	r.userTable[pos] = nil
	r.exited(roomUser)
	// set session parameters
	roomUser.roomID = nil
	roomUser.roomPos = nil
	// log
	var entry = r.server.logger.WithField("session", fmt.Sprintf("%x", roomUser.id))
	entry.WithField("room", fmt.Sprintf("%x", r.id)).Infof("exited room")
}

func (r *rtRoom) userNum() uint16 {
	// return uint16(len(r.userTable))
	var count uint16 = 0
	for _, u := range r.userTable {
		if u == nil {
			continue
		}
		count++
	}
	return count
}

func (r *rtRoom) userList() []uint16 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var ret = make([]uint16, 0, len(r.userTable))
	for _, u := range r.userTable {
		if u == nil {
			continue
		}
		ret = append(ret, u.id)
	}
	return ret
}

func (r *rtRoom) isCanLock() bool {
	if r.status.Status != rtRoomStatusOpening {
		return false
	}
	if !(r.userNum() >= r.status.MinUser && r.userNum() <= r.status.MaxUser) {
		return false
	}
	return true
}

func (r *rtRoom) lock() {
	if !r.isCanLock() {
		var entry = r.server.logger.WithField("room", fmt.Sprintf("%x", r.id))
		entry.Errorf("cannot lock room")
		return
	}
	r.status.Status = rtRoomStatusLocking
	var entry = r.server.logger.WithField("room", fmt.Sprintf("%x", r.id))
	entry.Infof("room locked")
}
func (r *rtRoom) unlock() {
	r.status.Status = rtRoomStatusOpening
	var entry = r.server.logger.WithField("room", fmt.Sprintf("%x", r.id))
	entry.Infof("room unlocked")
}

func (r *rtRoom) check() (ok bool) {
	if r.server.Now().After(r.status.UntilAt) {
		r.mu.RLock()
		for i := 0; i < len(r.userTable); i++ {
			r.exiting(r.userTable[i])
		}
		for i := 0; i < len(r.userTable); i++ {
			r.exited(r.userTable[i])
		}
		r.mu.RUnlock()
		return false
	}
	for i := 0; i < len(r.userTable); i++ {
		if r.server.Now().After(r.userTable[i].untilAt) {
			r.userRemoveByPosition(i)
		}
	}
	return true
}

func (r *rtRoom) writeToPacket(pkt *rtPacket) {
	r.status.writeToPacket(pkt) // room status
	if r.owner != nil {
		pkt.WriteUint16(r.owner.id) // room owner session id
	} else {
		pkt.WriteUint16(0)
	}
	for _, u := range r.userTable {
		if u == nil {
			continue
		}
		pkt.WriteUint16(u.id).WriteUint16(*u.roomPos)
	}
	// end of stream
	pkt.WriteUint16(0xffff).WriteUint16(0xffff)
}

const rtRoomValueTableSize = 1 << 8

type rtRoomValueTable [rtRoomValueTableSize]interface{}

func (t rtRoomValueTable) Set(key byte, val []byte) {
	t[key] = val
}
func (t rtRoomValueTable) Get(key byte) (ret []byte) {
	var val = t[key]
	if val == nil {
		return nil
	}
	return val.([]byte)
}

const rtMessageQueueSize = 1 << 12

type rtMessageQueue struct {
	sync.RWMutex
	q           [rtMessageQueueSize]*rtCtx
	front, back uint16
}

func (mq *rtMessageQueue) isEmpty() bool {
	return mq.front == mq.back
}
func (mq *rtMessageQueue) isFull() bool {
	mq.RLock()
	defer mq.RUnlock()
	return (mq.back+1)&(rtMessageQueueSize-1) == mq.front
}
func (mq *rtMessageQueue) push(ctx *rtCtx) {
	mq.Lock()
	defer mq.Unlock()
	mq.q[mq.back] = ctx
	mq.back = (mq.back + 1) & (rtMessageQueueSize - 1)
}
func (mq *rtMessageQueue) pop() (ctx *rtCtx) {
	ctx = mq.q[mq.front]
	mq.front = (mq.front + 1) & (rtMessageQueueSize - 1)
	return ctx
}
func (mq *rtMessageQueue) exec(ht rtHandlerTable) {
	mq.Lock()
	defer mq.Unlock()
	for !mq.isEmpty() {
		var ctx = mq.pop()
		if ctx == nil {
			continue
		}
		if ctx.server.session(ctx.sessionID) == nil {
			continue
		}
		ht[ctx.in.code](ctx)
		ctx.dispose()
	}
}
