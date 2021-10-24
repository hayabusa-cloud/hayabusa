package realtime

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type processor chan *ctx

func newProcessor() processor {
	return make(chan *ctx)
}

func (p processor) start(server *serverImpl) {
	for c := range p {
		var cmd, code, h = c.request.header & 0xf, c.request.code, Handler(nil)
		if cmd == headerCmdBuiltin {
			h = server.builtinHandlerTable[code]
		} else if cmd == headerCmdOriginal {
			h = server.handlerTable[code]
		}
		if h != nil {
			h(c)
		}
		c.dispose()
	}
}

//lint:ignore U1000 ignore unused
type serializable interface {
	serialize(m *message)
}

//lint:ignore U1000 ignore unused
type deserializable interface {
	deserialize(m *message)
}

const (
	roomTableSizeBit = 14
	roomTableSize    = 1 << roomTableSizeBit
	roomNumMask      = roomTableSize | (roomTableSize << 1)
)

type roomTable [roomTableSize]*room

func (t roomTable) room(roomID uint16) *room {
	if roomID&roomNumMask != 0 {
		return nil
	}
	return t[roomID]
}

type roomSortInfo struct {
	room   *room
	weight float32
	r      float32
}
type roomSortSlice []*roomSortInfo

func (s roomSortSlice) Less(i, j int) bool {
	return s[i].r > s[j].r
}
func (s roomSortSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s roomSortSlice) Len() int {
	return len(s)
}
func (t roomTable) randomSelect(appID []byte, mux []byte, scoreCenter float32) *room {
	var roomList = make(roomSortSlice, 0, roomTableSize)
	for i := uint16(0); i < roomTableSize; i++ {
		if t[i] == nil {
			continue
		}
		var loopRoom = t[i]
		if loopRoom.status.Status != roomStatusOpening {
			continue
		}
		if loopRoom.userNum() >= loopRoom.status.UserNum {
			continue
		}
		if !bytes.Equal(appID, loopRoom.owner.conn.AppID()) {
			continue
		}
		if !loopRoom.status.isMatchMux(mux) {
			continue
		}
		var weight = float32(loopRoom.userNum() + 1)
		weight = weight * weight
		var diff = (loopRoom.status.ScoreCenter - scoreCenter) / loopRoom.status.ScoreVariance
		if diff < 0 {
			diff = -diff
		}
		weight = weight / float32(math.Exp(float64(diff)))
		var r = float32(math.Pow(rand.Float64(), float64(1.0/weight)))
		roomList = append(roomList, &roomSortInfo{
			room:   loopRoom,
			weight: weight,
			r:      r,
		})
	}
	roomList = append(roomList, &roomSortInfo{
		room:   nil,
		weight: 0.1,
		r:      0.001,
	})
	sort.Sort(roomList)
	return roomList[0].room
}

type RoomStatus struct {
	ID            uint16
	RoomType      uint8
	Status        uint8
	CreatedAt     time.Time
	UntilAt       time.Time
	UserNum       uint16
	ScoreCenter   float32
	ScoreVariance float32
	Mux           []byte
}

func (r *RoomStatus) writeToMessage(msg *message) {
	msg.WriteUint16(r.ID).WriteUint8(r.RoomType).WriteUint8(r.Status).WriteUint16(r.UserNum)
	msg.WriteFloat32(r.ScoreCenter).WriteFloat32(r.ScoreVariance)
}
func (r *RoomStatus) isMatchMux(mux []byte) bool {
	if mux == nil || r.Mux == nil {
		return false
	}
	return bytes.Equal(r.Mux, mux)
}

type room struct {
	id         uint16
	mu         sync.RWMutex
	owner      *roomUser
	status     *RoomStatus
	userTable  []*roomUser
	valueTable roomValueTable
	server     *serverImpl
	entering   func(user *roomUser)
	entered    func(user *roomUser)
	exiting    func(user *roomUser)
	exited     func(user *roomUser)
}

func createRoom(server *serverImpl) *room {
	var roomStatus = &RoomStatus{
		RoomType:      roomTypeMatching,
		Status:        roomStatusOpening,
		CreatedAt:     server.Now(),
		UntilAt:       server.Now(),
		UserNum:       0,
		ScoreCenter:   0,
		ScoreVariance: math.MaxFloat32,
		Mux:           make([]byte, 0),
	}
	return &room{
		status:     roomStatus,
		mu:         sync.RWMutex{},
		userTable:  make([]*roomUser, 0, 0x10),
		valueTable: roomValueTable{},
		entering:   func(user *roomUser) {},
		entered:    func(user *roomUser) {},
		exiting:    func(user *roomUser) {},
		exited:     func(user *roomUser) {},
	}
}

type roomUser struct {
	conn      *connection
	room      *room
	status    uint8
	enteredAt time.Time
	untilAt   time.Time
}

func (u *roomUser) serialize(resp *message) {
	resp.WriteUint16(u.conn.id).WriteBytes(u.conn.UserID()).WriteUint8(u.status)
}

const (
	roomTypeNormal   = 0 // normal room
	roomTypeMatching = 1 // auto matching room

	roomStatusOpening = 0 // wait for players
	roomStatusLocking = 1 // gaming

	roomUserStatusWaiting = 0
	roomUserStatusReady   = 1
	roomUserStatusGaming  = 2
	roomUserStatusExiting = 3
)

func newRealtimeRoom(roomID uint16, server *serverImpl) *room {
	var defaultRoomLife = time.Hour * 36
	var newRoom = server.roomPool.Get().(*room)
	newRoom.id = roomID
	newRoom.status.ID = roomID
	newRoom.status.RoomType = roomTypeMatching
	newRoom.status.Status = roomStatusOpening
	newRoom.status.CreatedAt = server.Now()
	newRoom.status.UntilAt = server.Now().Add(defaultRoomLife)
	newRoom.userTable = make([]*roomUser, 0, 0x10)
	return newRoom
}

func (r *room) UserIDList() [][]byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var ret = make([][]byte, 0, len(r.userTable))
	for _, u := range r.userTable {
		if u == nil {
			continue
		}
		ret = append(ret, u.conn.UserID())
	}
	return ret
}

func (r *room) userAdd(connection *connection) bool {
	var defaultRoomUserLife = time.Hour * 72
	var newUser = &roomUser{
		room:      r,
		conn:      connection,
		status:    roomUserStatusWaiting,
		enteredAt: r.server.Now(),
		untilAt:   r.server.Now().Add(defaultRoomUserLife),
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
		if uint16(i) >= r.status.UserNum {
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
		if len(r.userTable) < int(r.status.UserNum) {
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
	// set connection parameters
	connection.roomID = &r.id
	connection.roomPos = &position
	r.entered(newUser)
	// log
	var entry = r.server.logEntry.WithField("connection", fmt.Sprintf("%x", newUser.conn.id))
	entry.WithField("room", fmt.Sprintf("%x", r.id)).Infof("entered room")
	return true
}

//lint:ignore U1000 ignore unused
func (r *room) userAddByID(connectionID uint16) bool {
	var conn = r.server.connection(connectionID)
	if conn == nil {
		return false
	}
	return r.userAdd(conn)
}

func (r *room) userRemove(connection *connection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < len(r.userTable); i++ {
		if r.userTable[i] == nil {
			continue
		}
		if r.userTable[i].conn.id != connection.id {
			continue
		}
		var u = r.userTable[i]
		r.exiting(u)
		// r.userTable = append(r.userTable[:i], r.userTable[i+1:]...)
		r.userTable[i] = nil
		r.exited(u)
		// set connection parameters
		connection.roomID = nil
		connection.roomPos = nil
		break
	}
	// log
	var entry = r.server.logEntry.WithField("connection", fmt.Sprintf("%x", connection.id))
	entry.WithField("room", fmt.Sprintf("%x", r.id)).Infof("exited room")
}

func (r *room) userRemoveByPosition(pos int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var u = r.userTable[pos]
	if u == nil {
		return
	}
	r.exiting(u)
	// r.userTable = append(r.userTable[:index], r.userTable[index+1:]...)
	r.userTable[pos] = nil
	r.exited(u)
	// set connection parameters
	u.conn.roomID = nil
	u.conn.roomPos = nil
	// log
	var entry = r.server.logEntry.WithField("connection", fmt.Sprintf("%x", u.conn.id))
	entry.WithField("room", fmt.Sprintf("%x", r.id)).Infof("exited room")
}

func (r *room) userNum() uint16 {
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

//lint:ignore U1000 ignore unused
func (r *room) userList() []uint16 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var ret = make([]uint16, 0, len(r.userTable))
	for _, u := range r.userTable {
		if u == nil {
			continue
		}
		ret = append(ret, u.conn.id)
	}
	return ret
}

func (r *room) isCanLock() bool {
	if r.status.Status != roomStatusOpening {
		return false
	}
	if r.userNum() != r.status.UserNum {
		return false
	}
	return true
}

func (r *room) lock() {
	if !r.isCanLock() {
		var entry = r.server.logEntry.WithField("room", fmt.Sprintf("%x", r.id))
		entry.Errorf("cannot lock room")
		return
	}
	r.status.Status = roomStatusLocking
	var entry = r.server.logEntry.WithField("room", fmt.Sprintf("%x", r.id))
	entry.Infof("room locked")
}
func (r *room) unlock() {
	r.status.Status = roomStatusOpening
	var entry = r.server.logEntry.WithField("room", fmt.Sprintf("%x", r.id))
	entry.Infof("room unlocked")
}

func (r *room) check() (ok bool) {
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

//lint:ignore U1000 ignore unused
func (r *room) serialize(msg *message) {
	r.status.writeToMessage(msg) // room status
	if r.owner != nil {
		msg.WriteUint16(r.owner.conn.id) // room owner connection id
	} else {
		msg.WriteUint16(0)
	}
	for _, u := range r.userTable {
		if u == nil {
			continue
		}
		msg.WriteUint16(u.conn.id).WriteUint16(*u.conn.roomPos)
	}
	// end of stream
	msg.WriteUint16(0xffff).WriteUint16(0xffff)
}

const rtRoomValueTableSize = 1 << 8

type roomValueTable [rtRoomValueTableSize]interface{}

func (t roomValueTable) Set(key byte, val []byte) {
	t[key] = val
}
func (t roomValueTable) Get(key byte) (ret []byte) {
	var val = t[key]
	if val == nil {
		return nil
	}
	return val.([]byte)
}

const messageQueueSize = 1 << 12

//lint:ignore U1000 ignore unused
type messageQueue struct {
	sync.RWMutex
	q           [messageQueueSize]*ctx
	front, back uint16
}

//lint:ignore U1000 ignore unused
func (mq *messageQueue) isEmpty() bool {
	return mq.front == mq.back
}

//lint:ignore U1000 ignore unused
func (mq *messageQueue) isFull() bool {
	mq.RLock()
	defer mq.RUnlock()
	return (mq.back+1)&(messageQueueSize-1) == mq.front
}

//lint:ignore U1000 ignore unused
func (mq *messageQueue) push(ctx *ctx) {
	mq.Lock()
	defer mq.Unlock()
	mq.q[mq.back] = ctx
	mq.back = (mq.back + 1) & (messageQueueSize - 1)
}

//lint:ignore U1000 ignore unused
func (mq *messageQueue) pop() (ctx *ctx) {
	ctx = mq.q[mq.front]
	mq.front = (mq.front + 1) & (messageQueueSize - 1)
	return ctx
}

//lint:ignore U1000 ignore unused
func (mq *messageQueue) exec(ht handlerTable) {
	mq.Lock()
	defer mq.Unlock()
	for !mq.isEmpty() {
		var c = mq.pop()
		if c == nil {
			continue
		}
		if c.server.connection(c.connID) == nil {
			continue
		}
		ht[c.request.code](c)
		c.dispose()
	}
}
