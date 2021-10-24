package realtime

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAreaOfInterestMap_InsertRemoveMove(t *testing.T) {
	server := NewTestRealtimeServer(t, NewTestEngine())
	aoiMap := NewAreaOfInterestMap(server, 1, 50, 50, 5, 5)
	var messageTargets []uint16
	aoiMap.sendMessageFn = func(targets []uint16, resp Response) {
		messageTargets = targets
	}
	aoiMap.Insert(1, []byte("user01"), 5, 5)
	assert.Len(t, messageTargets, 1) // 1
	aoiMap.Insert(2, []byte("user02"), 6, 6)
	assert.Len(t, messageTargets, 2) // 1, 2
	aoiMap.Insert(3, []byte("user03"), 15, 15)
	assert.Len(t, messageTargets, 3) // 1, 2, 3
	aoiMap.Insert(4, []byte("user04"), 5, 15)
	assert.Len(t, messageTargets, 4) // 1, 2, 3, 4
	aoiMap.Move(1, []byte("user01"), 16, 16, 10)
	assert.Len(t, messageTargets, 4) // 1, 2, 3, 4
	aoiMap.Insert(5, []byte("user05"), 35, 35)
	assert.Len(t, messageTargets, 1) // 5
	aoiMap.Insert(6, []byte("user06"), 45, 25)
	assert.Len(t, messageTargets, 2) // 5, 6
	aoiMap.Move(2, []byte("user02"), 25, 25, 10)
	assert.Len(t, messageTargets, 3) // 1, 3, 4
	aoiMap.Insert(7, []byte("user07"), 15, 5)
	assert.Len(t, messageTargets, 4) // 1, 3, 4, 7
	aoiMap.Remove(1, []byte("user01"))
	assert.Len(t, messageTargets, 5) // 1, 2, 3, 4, 7
	aoiMap.Move(4, []byte("user04"), 45, 45, 10)
	assert.Len(t, messageTargets, 2) // 4, 7
}
