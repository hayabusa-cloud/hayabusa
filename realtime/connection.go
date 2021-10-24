package realtime

import (
	"sync"
	"time"
)

const (
	connectionPoolSizeBit = 15
	// connectionPoolSize must be powers of 2
	connectionPoolSize = 1 << connectionPoolSizeBit
)
const minHeartbeatInterval = time.Second

type connectionManager struct {
	sync.RWMutex
	pool        [connectionPoolSize]connection
	active      map[uint16]*connection
	assigningID uint16
}

func (m *connectionManager) init(server *serverImpl) {
	for i := uint16(0); i < connectionPoolSize; i++ {
		m.pool[i].id = i
		m.pool[i].server = server
		m.pool[i].loadValue = 3
		m.pool[i].loadValueAt = server.Now()
		m.pool[i].auth = nil
		m.pool[i].message = &messageObject{}
		m.pool[i].active = false
	}
	m.active = make(map[uint16]*connection, connectionPoolSize>>1)
	m.assigningID = 1
}
func (m *connectionManager) connectionNum() int {
	return len(m.active)
}
func (m *connectionManager) isFull() bool {
	return m.connectionNum() >= connectionPoolSize>>1
}
func (m *connectionManager) connection(connectionID uint16) *connection {
	if connectionID >= connectionPoolSize {
		return nil
	}
	if !m.pool[connectionID].active {
		return nil
	}
	return &m.pool[connectionID]
}
func (m *connectionManager) create() (c *connection, ok bool) {
	if m.isFull() {
		return nil, false
	}
	m.Lock()
	defer m.Unlock()

	for m.pool[m.assigningID].active {
		// assert that connectionPoolSize is powers of 2
		m.assigningID = (m.assigningID + 1) & (connectionPoolSize - 1)
	}
	c = &m.pool[m.assigningID]
	c.active = true
	c.auth = nil
	c.loadValue = 3
	c.loadValueAt = c.server.Now()
	c.roomID = nil
	c.roomPos = nil
	c.sending = make(chan []byte, 200)
	m.active[c.id] = c
	return c, true
}
func (m *connectionManager) destroy(connectionID uint16, err ErrorCode) {
	var conn = m.connection(connectionID)
	conn.mu.Lock()
	conn.active = false
	close(conn.sending)
	conn.mu.Unlock()
	m.Lock()
	delete(m.active, conn.id)
	m.Unlock()
}
