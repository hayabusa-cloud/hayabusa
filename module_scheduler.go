package hybs

import (
	"time"
)

// EventCallback is function type of scheduled task callback
type EventCallback func(eng *hybsEngine, at Time) (err error)

// SchedledEvent is data structured of scheduled event
type ScheduledEvent struct {
	id         string
	callbackFn []EventCallback
	untilAt    *Time
	interval   uint16 // second
}

// NewScheduledEvent create and returns a new ScheduledEvent with given parameters
func NewScheduledEvent(id string, interval uint16, untilAt ...*Time) *ScheduledEvent {
	ret := &ScheduledEvent{
		id:         id,
		callbackFn: make([]EventCallback, 0),
		untilAt:    nil,
		interval:   interval,
	}
	if len(untilAt) > 0 {
		ret.untilAt = untilAt[0]
	}
	return ret
}

// AddCallback adds EventCallback to the ScheduledEvent
func (se *ScheduledEvent) AddCallback(fn EventCallback) *ScheduledEvent {
	if se.callbackFn == nil {
		se.callbackFn = make([]EventCallback, 0)
	}
	se.callbackFn = append(se.callbackFn, fn)
	return se
}
func (se *ScheduledEvent) copy() *ScheduledEvent {
	return &ScheduledEvent{
		id:         se.id,
		callbackFn: se.callbackFn,
		untilAt:    se.untilAt,
		interval:   se.interval,
	}
}

type eventList struct {
	*ScheduledEvent
	next *eventList
}

type schedulerWheel [1 << 16]*eventList

type scheduler struct {
	server *hybsEngine
	wheel  schedulerWheel

	pulser chan Time
}

func (s *scheduler) insert(at Time, evt *ScheduledEvent) {
	var atUnix16 = uint16(at.Unix() & 0xffff)
	s.wheel[atUnix16] = &eventList{
		ScheduledEvent: evt,
		next:           s.wheel[atUnix16],
	}
}
func (s *scheduler) do(at Time) {
	var atUnix16 = uint16(at.Unix() & 0xffff)
	for ; s.wheel[atUnix16] != nil; s.wheel[atUnix16] = s.wheel[atUnix16].next {
		evt := s.wheel[atUnix16].ScheduledEvent
		if evt.untilAt != nil && at.After(*evt.untilAt) {
			continue
		}
		for _, fn := range evt.callbackFn {
			if err := fn(s.server, at.Truncate(time.Second)); err != nil {
				s.server.LogErrorf("sys event ID=%s error:%s", evt.id, err)
			}
		}
		if evt.interval > 0 {
			if evt.untilAt == nil || !evt.untilAt.After(at.Add(time.Duration(evt.interval)*time.Second)) {
				s.insert(at.Add(time.Duration(evt.interval)*time.Second), evt)
			}
		}
	}
}
func (s *scheduler) run(ch chan Time) {
	s.pulser = ch
	for {
		at, ok := <-ch
		if !ok {
			break
		}
		s.do(at)
	}
}

// Pulser returns a new channel type timing pulse generator with given duration and bufferSize
func Pulser(d time.Duration, bufferSize ...uint16) (ch chan Time) {
	var nextPulseAt = Now().Truncate(d)
	if len(bufferSize) < 1 {
		ch = make(chan Time)
	} else {
		ch = make(chan Time, bufferSize[0])
	}
	go func() {
		for {
			if ch == nil {
				break
			}
			var now = Now()
			if nextPulseAt.After(now) {
				var sleepDuration = nextPulseAt.Sub(now)
				time.Sleep(sleepDuration)
				continue
			}
			var currPulseAt = nextPulseAt
			nextPulseAt = nextPulseAt.Add(d)
			if ch != nil {
				ch <- currPulseAt
			} else {
				break
			}
		}
	}()
	return ch
}

var defaultSchedulerEvents = make([]*ScheduledEvent, 0)

// RegisterScheduledEvent register evt to engine
func RegisterScheduledEvent(evt *ScheduledEvent) {
	defaultSchedulerEvents = append(defaultSchedulerEvents, evt)
}
