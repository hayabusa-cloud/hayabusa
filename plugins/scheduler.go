package plugins

import (
	"context"
	"time"

	"github.com/hayabusa-cloud/hayabusa/utils"
)

// EventCallback is function type of scheduled task callback
type EventCallback func(plugins Interface, at time.Time) (err error)

// ScheduledEvent is data structured of scheduled event
type ScheduledEvent struct {
	id         string
	callbackFn []EventCallback
	untilAt    *time.Time
	interval   uint16 // second
}

// NewScheduledEvent create and returns a new ScheduledEvent with given parameters
func NewScheduledEvent(id string, interval uint16, untilAt ...*time.Time) *ScheduledEvent {
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

type eventList struct {
	*ScheduledEvent
	next *eventList
}

type timeWheel [1 << 16]*eventList

// Scheduler represents task Scheduler
type Scheduler struct {
	plugins Interface
	wheel   timeWheel

	clock *utils.ClockGenerator
}

// DefaultScheduler returns an new created scheduler with default registered events
func DefaultScheduler(engine Interface) (s *Scheduler) {
	s = &Scheduler{
		plugins: engine,
		clock:   utils.NewClockGenerator(engine.(utils.Clicker), time.Second),
	}
	for i := 0; i < len(scheduledEvents); i++ {
		s.insert(engine.(utils.Clicker).Now().Add(time.Second), scheduledEvents[i])
	}
	return
}

func (s *Scheduler) insert(at time.Time, evt *ScheduledEvent) {
	var atUnix16 = uint16(at.Unix() & 0xffff)
	s.wheel[atUnix16] = &eventList{
		ScheduledEvent: evt,
		next:           s.wheel[atUnix16],
	}
}
func (s *Scheduler) do(at time.Time) {
	var atUnix16 = uint16(at.Unix() & 0xffff)
	for ; s.wheel[atUnix16] != nil; s.wheel[atUnix16] = s.wheel[atUnix16].next {
		evt := s.wheel[atUnix16].ScheduledEvent
		if evt.untilAt != nil && at.After(*evt.untilAt) {
			continue
		}
		for _, fn := range evt.callbackFn {
			if err := fn(s.plugins, at.Truncate(time.Second)); err != nil {
				s.plugins.Errorf("callback event [%s] error:%s", evt.id, err)
			}
		}
		if evt.interval > 0 {
			if evt.untilAt == nil || !evt.untilAt.After(at.Add(time.Duration(evt.interval)*time.Second)) {
				s.insert(at.Add(time.Duration(evt.interval)*time.Second), evt)
			}
		}
	}
}

// Start blocks until the scheduler stopped
func (s *Scheduler) Start(ctx context.Context) {
	for {
		at, ok := s.clock.Receive(ctx)
		if !ok {
			break
		}
		s.do(at)
	}
}
func (s *Scheduler) Stop() {
	s.clock.Stop()
}

var scheduledEvents = make([]*ScheduledEvent, 0)

// RegisterScheduledEvent register evt to engine
func RegisterScheduledEvent(evt *ScheduledEvent) {
	scheduledEvents = append(scheduledEvents, evt)
}
