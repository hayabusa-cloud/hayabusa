package utils

import (
	"context"
	"time"
)

type ClockGenerator struct {
	ch     chan time.Time
	active bool
}

// NewClockGenerator returns a new channel type clock pulse generator with given duration and bufferSize
func NewClockGenerator(clock Clicker, d time.Duration, bufferSize ...uint16) (clockGenerator *ClockGenerator) {
	var (
		nextAt = clock.Now().Truncate(d)
	)
	clockGenerator = &ClockGenerator{active: true}
	if len(bufferSize) < 1 {
		clockGenerator.ch = make(chan time.Time)
	} else {
		clockGenerator.ch = make(chan time.Time, bufferSize[0])
	}
	go func() {
		for {
			var now = clock.Now()
			if nextAt.After(now) {
				var sleepDuration = nextAt.Sub(now)
				time.Sleep(sleepDuration)
				continue
			}
			var lastClockedAt = nextAt
			nextAt = nextAt.Add(d)
			if !clockGenerator.active {
				close(clockGenerator.ch)
				break
			}
			clockGenerator.ch <- lastClockedAt
		}
	}()
	return
}

func (clockGenerator *ClockGenerator) Receive(ctx context.Context) (t time.Time, ok bool) {
	select {
	case <-ctx.Done():
		return time.Now(), false
	case t, ok = <-clockGenerator.ch:
		return
	}
}

func (clockGenerator *ClockGenerator) Channel() <-chan time.Time {
	return clockGenerator.ch
}

func (clockGenerator *ClockGenerator) Stop() {
	clockGenerator.active = false
}

type Clicker interface {
	Now() time.Time
}
