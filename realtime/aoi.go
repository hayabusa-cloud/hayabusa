package realtime

import (
	"math"
)

// realtime_aoi_map.go implements area of interest algorithm
// NOT multi-goroutine concurrency safe, because engine uses single-loop to
// process the game requests sent from clients
// when synchronize player location, ONLY broadcast location information
// to other players that is nearby the moving player

type realtimeAOICell struct {
	connID           uint16
	userID           []byte
	offsetX, offsetY float64
	pred, next       *realtimeAOICell

	aoiCell *aoiCell
}

//lint:ignore U1000 todo
func (e *realtimeAOICell) absolutePosition() (x float64, y float64) {
	x = e.offsetX + float64(e.aoiCell.x)*e.aoiCell.aoiMap.cellSizeW
	y = e.offsetY + float64(e.aoiCell.y)*e.aoiCell.aoiMap.cellSizeH
	return
}

type aoiCell struct {
	x, y uint16
	list *realtimeAOICell

	aoiMap *AreaOfInterestMap
}

// AreaOfInterestMap represents area of interest map data structure
type AreaOfInterestMap struct {
	mapID                uint16
	cellMatrix           [][]*aoiCell
	cellNumW, cellNumH   uint16
	cellSizeW, cellSizeH float64

	mapByConnID map[uint16]*realtimeAOICell

	sendMessageFn  func(targets []uint16, resp Response)
	insertPacketFn func(connID uint16, userID []byte, x float64, y float64) (resp Response)
	removePacketFn func(connID uint16, userID []byte) (resp Response)
	movePacketFn   func(connID uint16, userID []byte, x float64, y float64, speed float64) (resp Response)
}

// OnInsertUser sets the callback fn on user inserted into map
func (m *AreaOfInterestMap) OnInsertUser(
	fn func(connID uint16, userID []byte, x float64, y float64) (resp Response)) (ret *AreaOfInterestMap) {
	m.insertPacketFn = fn
	return m
}

// OnRemoveUser sets the callback fn on user removed from map
func (m *AreaOfInterestMap) OnRemoveUser(fn func(connID uint16, userID []byte) (resp Response)) (ret *AreaOfInterestMap) {
	m.removePacketFn = fn
	return m
}

// OnMoveUser sets the callback fn on user moved on the map
func (m *AreaOfInterestMap) OnMoveUser(
	fn func(connID uint16, userID []byte, x float64, y float64, speed float64) (resp Response)) (ret *AreaOfInterestMap) {
	m.movePacketFn = fn
	return m
}

// NewAreaOfInterestMap creates and returns a new area of interest map
func NewAreaOfInterestMap(
	server Server,
	mapID uint16,
	sizeX float64, sizeY float64,
	cellNumW uint16, cellNumH uint16) (newMap *AreaOfInterestMap) {
	newMap = &AreaOfInterestMap{
		mapID:       mapID,
		cellMatrix:  make([][]*aoiCell, cellNumH),
		cellNumW:    cellNumW,
		cellNumH:    cellNumH,
		cellSizeW:   sizeX / float64(cellNumW),
		cellSizeH:   sizeY / float64(cellNumH),
		mapByConnID: make(map[uint16]*realtimeAOICell),
		sendMessageFn: func(targets []uint16, resp Response) {
			server.SendMessageMultiple(targets, resp)
		},
		insertPacketFn: func(connID uint16, userID []byte, x float64, y float64) (resp Response) {
			resp = server.(*serverImpl).responsePool.Get().(Response)
			defer server.(*serverImpl).responsePool.Put(resp)
			resp.Reset()
			resp.SetEventCode(EventCodeUserEnteredAOIMap)
			resp.WriteUint16(newMap.mapID).WriteUint16(connID).WriteFloat64(x).WriteFloat64(y)
			return resp
		},
		removePacketFn: func(connID uint16, userID []byte) (resp Response) {
			resp = server.(*serverImpl).responsePool.Get().(Response)
			defer server.(*serverImpl).responsePool.Put(resp)
			resp.Reset()
			resp.SetEventCode(EventCodeUserExitedAOIMap)
			resp.WriteUint16(newMap.mapID).WriteUint16(connID)
			return resp
		},
		movePacketFn: func(connID uint16, userID []byte, x float64, y float64, speed float64) (resp Response) {
			resp = server.(*serverImpl).responsePool.Get().(Response)
			defer server.(*serverImpl).responsePool.Put(resp)
			resp.Reset()
			resp.SetEventCode(EventCodeUserMovedOnAOIMap)
			resp.WriteUint16(newMap.mapID).WriteUint16(connID)
			resp.WriteFloat64(x).WriteFloat64(y).WriteFloat64(speed)
			return resp
		},
	}
	// init matrix
	for r := 0; r < len(newMap.cellMatrix); r++ {
		newMap.cellMatrix[r] = make([]*aoiCell, cellNumW)
		for c := 0; c < len(newMap.cellMatrix[r]); c++ {
			newMap.cellMatrix[r][c] = &aoiCell{
				x:      uint16(r),
				y:      uint16(c),
				list:   nil,
				aoiMap: newMap,
			}
		}
	}
	return newMap
}

func (m *AreaOfInterestMap) broadcastAdjacency(centralCell *aoiCell, resp Response) {
	// broadcast insert
	var sendTarget = make([]uint16, 0)
	// add current cell
	var elem = centralCell.list
	for elem != nil {
		sendTarget = append(sendTarget, elem.connID)
		elem = elem.next
	}
	// add left cell
	if centralCell.x > 0 {
		elem = m.cellMatrix[centralCell.x-1][centralCell.y].list
		for elem != nil {
			sendTarget = append(sendTarget, elem.connID)
			elem = elem.next
		}
	}
	// add right cell
	if centralCell.x < m.cellNumW-1 {
		elem = m.cellMatrix[centralCell.x+1][centralCell.y].list
		for elem != nil {
			sendTarget = append(sendTarget, elem.connID)
			elem = elem.next
		}
	}
	// add bottom cells
	if centralCell.y > 0 {
		if centralCell.x > 0 {
			elem = m.cellMatrix[centralCell.x-1][centralCell.y-1].list
			for elem != nil {
				sendTarget = append(sendTarget, elem.connID)
				elem = elem.next
			}
		}
		elem = m.cellMatrix[centralCell.x][centralCell.y-1].list
		for elem != nil {
			sendTarget = append(sendTarget, elem.connID)
			elem = elem.next
		}
		if centralCell.x < m.cellNumW-1 {
			elem = m.cellMatrix[centralCell.x+1][centralCell.y-1].list
			for elem != nil {
				sendTarget = append(sendTarget, elem.connID)
				elem = elem.next
			}
		}
	}
	// add top cells
	if centralCell.y < m.cellNumH-1 {
		if centralCell.x > 0 {
			elem = m.cellMatrix[centralCell.x-1][centralCell.y+1].list
			for elem != nil {
				sendTarget = append(sendTarget, elem.connID)
				elem = elem.next
			}
		}
		elem = m.cellMatrix[centralCell.x][centralCell.y+1].list
		for elem != nil {
			sendTarget = append(sendTarget, elem.connID)
			elem = elem.next
		}
		if centralCell.x < m.cellNumW-1 {
			elem = m.cellMatrix[centralCell.x+1][centralCell.y+1].list
			for elem != nil {
				sendTarget = append(sendTarget, elem.connID)
				elem = elem.next
			}
		}
	}
	m.sendMessageFn(sendTarget, resp)
}

// Insert inserts a user into area of interest map
func (m *AreaOfInterestMap) Insert(connID uint16, userID []byte, x float64, y float64) {
	var cellX, cellY = uint16(x / m.cellSizeW), uint16(y / m.cellSizeH)
	if cellX >= m.cellNumW || cellY >= m.cellNumH {
		return
	}
	var cell = m.cellMatrix[cellX][cellY]
	var newElem = &realtimeAOICell{
		connID:  connID,
		userID:  userID,
		offsetX: math.Mod(x, m.cellSizeW),
		offsetY: math.Mod(y, m.cellSizeH),
		pred:    nil,
		next:    nil,
		aoiCell: cell,
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
	m.mapByConnID[connID] = newElem
	// broadcast message
	m.broadcastAdjacency(cell, m.insertPacketFn(connID, userID, x, y))
}

// Remove removes a user into area of interest map
func (m *AreaOfInterestMap) Remove(connID uint16, userID []byte) {
	var removingElem, ok = m.mapByConnID[connID]
	if !ok {
		return
	}
	// broadcast remove
	m.broadcastAdjacency(removingElem.aoiCell, m.removePacketFn(connID, userID))
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

// Move moves a user in the area of interest map
func (m *AreaOfInterestMap) Move(connID uint16, userID []byte, posX float64, posY float64, speed float64) {
	var movingElem, ok = m.mapByConnID[connID]
	if !ok {
		return
	}
	// broadcast move
	m.broadcastAdjacency(movingElem.aoiCell, m.movePacketFn(connID, userID, posX, posY, speed))
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
