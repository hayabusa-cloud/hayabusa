package hybs

import (
	"time"
)

type EventCallback func(eng *hybsEngine, at Time) (err error)

type SchedulerEvent struct {
	id         string
	callbackFn []EventCallback
	untilAt    *Time
	interval   uint16 // second
}

func newSchedulerEvent(id string, interval uint16, untilAt ...*Time) *SchedulerEvent {
	ret := &SchedulerEvent{
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
func (se *SchedulerEvent) AddCallback(fn EventCallback) *SchedulerEvent {
	if se.callbackFn == nil {
		se.callbackFn = make([]EventCallback, 0)
	}
	se.callbackFn = append(se.callbackFn, fn)
	return se
}
func (se *SchedulerEvent) copy() *SchedulerEvent {
	return &SchedulerEvent{
		id:         se.id,
		callbackFn: se.callbackFn,
		untilAt:    se.untilAt,
		interval:   se.interval,
	}
}

type eventList struct {
	*SchedulerEvent
	next *eventList
}

type schedulerWheel [1 << 16]*eventList

type scheduler struct {
	server *hybsEngine
	wheel  schedulerWheel

	pulser chan Time
}

func (s *scheduler) insert(at Time, evt *SchedulerEvent) {
	var atUnix16 = uint16(at.Unix() & 0xffff)
	s.wheel[atUnix16] = &eventList{
		SchedulerEvent: evt,
		next:           s.wheel[atUnix16],
	}
}
func (s *scheduler) do(at Time) {
	var atUnix16 = uint16(at.Unix() & 0xffff)
	for ; s.wheel[atUnix16] != nil; s.wheel[atUnix16] = s.wheel[atUnix16].next {
		evt := s.wheel[atUnix16].SchedulerEvent
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

// Pulser returns a pulse generator
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

var defaultSchedulerEvents = make([]*SchedulerEvent, 0)

func RegisterSchedulerEvent(evt *SchedulerEvent) {
	defaultSchedulerEvents = append(defaultSchedulerEvents, evt)
}
