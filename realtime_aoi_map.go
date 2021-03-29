package hybs

import (
	"math"
)

// realtime_aoi_map.go implements area of interest algorithm
// NOT multi-goroutine safe, because of using single-loop model to
// process the game requests sent from clients
// when synchronize player location, ONLY broadcast location information
// to other players that is nearby the moving player

type rtAOIElem struct {
	sessionID        uint16
	userID           []byte
	offsetX, offsetY float64
	pred, next       *rtAOIElem

	aoiCell *rtAOICell
}

func (e *rtAOIElem) absolutePosition() (x float64, y float64) {
	x = e.offsetX + float64(e.aoiCell.x)*e.aoiCell.aoiMap.cellSizeW
	y = e.offsetY + float64(e.aoiCell.y)*e.aoiCell.aoiMap.cellSizeH
	return
}

type rtAOICell struct {
	x, y uint16
	list *rtAOIElem

	aoiMap *RTAreaOfInterestMap
}

// RTAreaOfInterestMap represents aoi map
type RTAreaOfInterestMap struct {
	mapID                uint16
	cellMatrix           [][]*rtAOICell
	cellNumW, cellNumH   uint16
	cellSizeW, cellSizeH float64

	mapBySessionID map[uint16]*rtAOIElem

	server         *hybsRealtimeServer
	insertPacketFn func(sessionID uint16, userID []byte, x float64, y float64) (pkt OutPacket)
	removePacketFn func(sessionID uint16, userID []byte) (pkt OutPacket)
	movePacketFn   func(sessionID uint16, userID []byte, x float64, y float64, speed float64) (pkt OutPacket)
}

func (m *RTAreaOfInterestMap) SetInsertFn(
	fn func(sessionID uint16, userID []byte, x float64, y float64) (pkt OutPacket)) (ret *RTAreaOfInterestMap) {
	m.insertPacketFn = fn
	return m
}
func (m *RTAreaOfInterestMap) SetRemoveFn(fn func(sessionID uint16, userID []byte) (pkt OutPacket)) (ret *RTAreaOfInterestMap) {
	m.removePacketFn = fn
	return m
}
func (m *RTAreaOfInterestMap) SetMoveFn(
	fn func(sessionID uint16, userID []byte, x float64, y float64, speed float64) (pkt OutPacket)) (ret *RTAreaOfInterestMap) {
	m.movePacketFn = fn
	return m
}

// NewRTAreaOfInterestMap creates and returns aoi map
func NewRTAreaOfInterestMap(
	server *hybsRealtimeServer,
	mapID uint16,
	sizeX float64, sizeY float64,
	cellNumW uint16, cellNumH uint16) (newMap *RTAreaOfInterestMap) {
	newMap = &RTAreaOfInterestMap{
		mapID:      mapID,
		cellMatrix: make([][]*rtAOICell, cellNumH),
		cellNumW:   cellNumW,
		cellNumH:   cellNumH,
		cellSizeW:  sizeX / float64(cellNumW),
		cellSizeH:  sizeY / float64(cellNumH),
		server:     server,
		insertPacketFn: func(sessionID uint16, userID []byte, x float64, y float64) (pkt OutPacket) {
			pkt = server.outPacketPool.Get().(OutPacket)
			defer server.outPacketPool.Put(pkt)
			pkt.Reset()
			pkt.SetEventCode(RTEventCodeUserEnteredAOIMap)
			pkt.WriteUint16(newMap.mapID).WriteUint16(sessionID).WriteFloat64(x).WriteFloat64(y)
			return pkt
		},
		removePacketFn: func(sessionID uint16, userID []byte) (pkt OutPacket) {
			pkt = server.outPacketPool.Get().(OutPacket)
			defer server.outPacketPool.Put(pkt)
			pkt.Reset()
			pkt.SetEventCode(RTEventCodeUserExitedAOIMap)
			pkt.WriteUint16(newMap.mapID).WriteUint16(sessionID)
			return pkt
		},
		movePacketFn: func(sessionID uint16, userID []byte, x float64, y float64, speed float64) (pkt OutPacket) {
			pkt = server.outPacketPool.Get().(OutPacket)
			defer server.outPacketPool.Put(pkt)
			pkt.Reset()
			pkt.SetEventCode(RTEventCodeUserMovedOnAOIMap)
			pkt.WriteUint16(newMap.mapID).WriteUint16(sessionID)
			pkt.WriteFloat64(x).WriteFloat64(y).WriteFloat64(speed)
			return pkt
		},
	}
	// init matrix
	for r := 0; r < len(newMap.cellMatrix); r++ {
		newMap.cellMatrix[r] = make([]*rtAOICell, cellNumW)
		for c := 0; r < len(newMap.cellMatrix[r]); c++ {
			newMap.cellMatrix[r][c] = &rtAOICell{
				x:      uint16(r),
				y:      uint16(c),
				list:   nil,
				aoiMap: newMap,
			}
		}
	}
	return newMap
}

func (m *RTAreaOfInterestMap) broadcastAdjacency(centralCell *rtAOICell, pkt OutPacket) {
	// broadcast insert
	var sendTarget = make([]uint16, 0)
	// add current cell
	var elem = centralCell.list
	for elem != nil {
		sendTarget = append(sendTarget, elem.sessionID)
	}
	// add left cell
	if centralCell.x > 0 {
		elem = m.cellMatrix[centralCell.x-1][centralCell.y].list
		for elem != nil {
			sendTarget = append(sendTarget, elem.sessionID)
		}
	}
	// add right cell
	if centralCell.x < m.cellNumW-1 {
		elem = m.cellMatrix[centralCell.x+1][centralCell.y].list
		for elem != nil {
			sendTarget = append(sendTarget, elem.sessionID)
		}
	}
	// add bottom cell
	if centralCell.y > 0 {
		elem = m.cellMatrix[centralCell.x][centralCell.y-1].list
		for elem != nil {
			sendTarget = append(sendTarget, elem.sessionID)
		}
	}
	// add top cell
	if centralCell.y < m.cellNumH-1 {
		elem = m.cellMatrix[centralCell.x][centralCell.y+1].list
		for elem != nil {
			sendTarget = append(sendTarget, elem.sessionID)
		}
	}
	m.server.SendPacketMultiple(sendTarget, pkt)
}
func (m *RTAreaOfInterestMap) Insert(sessionID uint16, userID []byte, x float64, y float64) {
	var cellX, cellY = uint16(x / m.cellSizeW), uint16(y / m.cellSizeH)
	if cellX >= m.cellNumW || cellY >= m.cellNumH {
		return
	}
	var cell = m.cellMatrix[cellX][cellY]
	var newElem = &rtAOIElem{
		sessionID: sessionID,
		userID:    userID,
		offsetX:   math.Mod(x, m.cellSizeW),
		offsetY:   math.Mod(y, m.cellSizeH),
		pred:      nil,
		next:      nil,
		aoiCell:   cell,
	}
	if cell.list == nil {
		cell.list = newElem
	} else {
		var elem = cell.list
		for elem.next != nil {
			elem = elem.next
		}
		elem.next = newElem
		newElem.pred = elem
	}
	// broadcast message
	m.broadcastAdjacency(cell, m.insertPacketFn(sessionID, userID, x, y))
}
func (m *RTAreaOfInterestMap) Remove(sessionID uint16, userID []byte) {
	var removingElem, ok = m.mapBySessionID[sessionID]
	if !ok {
		return
	}
	// broadcast remove
	m.broadcastAdjacency(removingElem.aoiCell, m.removePacketFn(sessionID, userID))
	// remove element
	if removingElem.pred != nil {
		removingElem.pred.next = removingElem.next
	}
	if removingElem.next != nil {
		removingElem.next.pred = removingElem.pred
	}
	if removingElem == removingElem.aoiCell.list {
		removingElem.aoiCell.list = nil
	}
}
func (m *RTAreaOfInterestMap) Move(sessionID uint16, userID []byte, posX float64, posY float64, speed float64) {
	var movingElem, ok = m.mapBySessionID[sessionID]
	if !ok {
		return
	}
	// broadcast move
	m.broadcastAdjacency(movingElem.aoiCell, m.movePacketFn(sessionID, userID, posX, posY, speed))
	// update position data
	var targetX = uint16(posX / movingElem.aoiCell.aoiMap.cellSizeW)
	var targetY = uint16(posY / movingElem.aoiCell.aoiMap.cellSizeH)
	if targetX == movingElem.aoiCell.x && targetY == movingElem.aoiCell.y {
		// case of not change cell
		movingElem.offsetX = posX - float64(targetX)*m.cellSizeW
		movingElem.offsetY = posY - float64(targetY)*m.cellSizeH
		return
	} else {
		// delete elem from list
		if movingElem.pred != nil {
			movingElem.pred.next = movingElem.next
		}
		if movingElem.next != nil {
			movingElem.next.pred = movingElem.pred
		}
		// cast of change cell
		if movingElem == movingElem.aoiCell.list {
			// only one elem in list
			movingElem.aoiCell.list = nil
		}
		// insert elem into elem list of adjacent cell
		var targetCell = m.cellMatrix[targetX][targetY]
		movingElem.aoiCell = targetCell
		movingElem.offsetX = posX - float64(targetX)*m.cellSizeW
		movingElem.offsetY = posY - float64(targetY)*m.cellSizeH
		if targetCell.list == nil {
			targetCell.list = movingElem
		} else {
			targetCell.list.pred = movingElem
			movingElem.next = targetCell.list
			targetCell.list = movingElem
		}
	}
}
