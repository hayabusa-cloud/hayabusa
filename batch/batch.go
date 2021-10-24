package batch

import (
	"context"
	"regexp"
	"strconv"
	"time"

	"github.com/hayabusa-cloud/hayabusa/engine"
	"github.com/hayabusa-cloud/hayabusa/utils"
)

type Batch struct {
	config          *Config
	engine          engine.Interface
	tasks           []func(ctx Ctx)
	launchedAt      time.Time
	lastExecutingAt *time.Time
	nextExecutingAt *time.Time
	lastCheckedAt   *time.Time
	executedTimes   int
	userValue       utils.KVPairs

	timeOffset time.Duration

	runAt     *time.Time
	startAt   *time.Time
	stopUntil *time.Time
	onMinute  *regexp.Regexp
	onHour    *regexp.Regexp
	onDay     *regexp.Regexp
	onMonth   *regexp.Regexp
	onWeekday *regexp.Regexp

	clock *utils.ClockGenerator
}

func NewBatch(engine engine.Interface, config Config) *Batch {
	var newBatch = &Batch{
		config:     &config,
		engine:     engine,
		tasks:      make([]func(ctx Ctx), 0),
		launchedAt: engine.Now(),
		userValue:  make(utils.KVPairs, 0),
	}
	// unmarshal time_specified
	if u, err := strconv.Atoi(config.TimeSpecified); err == nil {
		newBatch.timeOffset = time.Unix(int64(u), 0).Sub(engine.Now())
	} else if t, err := utils.TimeParseAny(config.TimeSpecified); err == nil {
		newBatch.timeOffset = t.Sub(engine.Now())
	}
	if t, err := utils.TimeParseAny(config.RunAt); err == nil {
		newBatch.runAt = t
	}
	var now = utils.TimeSecondAt(newBatch.launchedAt)
	newBatch.nextExecutingAt = &now
	if t, err := utils.TimeParseAny(config.StartFrom); err == nil {
		newBatch.startAt = t
		newBatch.nextExecutingAt = newBatch.startAt
	}
	if t, err := utils.TimeParseAny(config.StopUntil); err == nil {
		newBatch.stopUntil = t
	}
	if config.OnMinute != "" {
		newBatch.onMinute = regexp.MustCompile(config.OnMinute)
	}
	if config.OnHour != "" {
		newBatch.onHour = regexp.MustCompile(config.OnHour)
	}
	if config.OnDay != "" {
		newBatch.onDay = regexp.MustCompile(config.OnDay)
	}
	if config.OnMonth != "" {
		newBatch.onMonth = regexp.MustCompile(config.OnMonth)
	}
	if config.OnWeekday != "" {
		newBatch.onWeekday = regexp.MustCompile(config.OnWeekday)
	}
	if newBatch.config.Interval < 0 {
		newBatch.config.Interval = 0
	}
	// task list
	for _, task := range config.Tasks {
		var s, ok = defaultBatchTaskTable[task]
		if !ok {
			continue
		}
		newBatch.tasks = append(newBatch.tasks, s...)
	}
	return newBatch
}

func (batch *Batch) ID() string {
	return batch.config.ID
}

func (batch *Batch) UserValue(key string) (value interface{}, ok bool) {
	return batch.userValue.Get(key)
}

func (batch *Batch) SetUserValue(key string, value interface{}) {
	batch.userValue = batch.userValue.Set(key, value)
}

func (batch *Batch) Now() time.Time {
	return batch.engine.Now().Add(batch.timeOffset)
}

// force execute tasks without condition check
func (batch *Batch) doExecute(t time.Time) {
	var start = batch.Now()
	batch.lastExecutingAt = &start
	var ctx = newBatchCtx(batch, t)
	for _, taskFn := range batch.tasks {
		taskFn(ctx)
	}
	var end = batch.Now()
	ctx.executedAt = &end
	batch.executedTimes++
	batch.engine.Infof("batch [%s] executed", batch.config.ID)
}
func (batch *Batch) touch() (ok bool) {
	switch batch.config.Event {
	case "immediately":
		return batch.touchImmediately(batch.Now())
	case "once":
		return batch.touchOnce(batch.Now())
	case "repeat":
		return batch.touchRepeat(batch.Now())
	case "timer":
		return batch.touchTimer(batch.Now())
	}
	return false
}
func (batch *Batch) touchImmediately(t time.Time) (ok bool) {
	batch.doExecute(t)
	return false
}
func (batch *Batch) touchOnce(t time.Time) (ok bool) {
	if batch.runAt != nil && !t.Before(*batch.runAt) {
		batch.doExecute(*batch.runAt)
		return false
	}
	return true
}
func (batch *Batch) touchRepeat(t time.Time) (ok bool) {
	if batch.stopUntil != nil && batch.stopUntil.Before(t) {
		return false
	}
	if batch.config.RepeatTimes > 0 && batch.executedTimes >= batch.config.RepeatTimes {
		return false
	}
	if batch.nextExecutingAt == nil {
		return false
	}
	if batch.startAt != nil && batch.startAt.After(t) {
		return true
	}
	if batch.nextExecutingAt.After(t) {
		return true
	}
	var current = utils.TimeSecondAt(*batch.nextExecutingAt)
	var next = current.Add(batch.config.Interval)
	batch.nextExecutingAt = &next
	batch.doExecute(current)
	return true
}
func (batch *Batch) touchTimer(t time.Time) (ok bool) {
	if batch.stopUntil != nil && batch.stopUntil.Before(t) {
		return false
	}
	if batch.config.RepeatTimes > 0 && batch.executedTimes >= batch.config.RepeatTimes {
		return false
	}
	if batch.startAt != nil && batch.startAt.After(t) {
		return true
	}
	if batch.lastExecutingAt != nil && batch.lastExecutingAt.Unix()/60 == t.Unix()/60 {
		return true
	}
	var from = batch.launchedAt
	if batch.lastCheckedAt != nil {
		from = *batch.lastCheckedAt
	}
	for checkpoint := from; checkpoint.Before(t); checkpoint = utils.TimeMinuteAt(checkpoint.Add(time.Minute)) {
		batch.lastCheckedAt = &checkpoint
		if batch.onMinute != nil && !batch.onMinute.MatchString(checkpoint.Format("04")) {
			continue
		}
		if batch.onHour != nil && !batch.onHour.MatchString(checkpoint.Format("15")) {
			continue
		}
		if batch.onDay != nil && !batch.onDay.MatchString(checkpoint.Format("02")) {
			continue
		}
		if batch.onMonth != nil && !batch.onMonth.MatchString(checkpoint.Format("01")) {
			continue
		}
		if batch.onWeekday != nil && !batch.onWeekday.MatchString(checkpoint.Format("Mon")) {
			continue
		}
		batch.doExecute(checkpoint)
	}
	return true
}

// Serve blocks until the batch stopped
func (batch *Batch) Serve(ctx context.Context) (err error) {
	batch.clock = utils.NewClockGenerator(batch, time.Second)
	for {
		var _, ok = batch.clock.Receive(ctx)
		if !ok {
			break
		}
		if !batch.touch() {
			break
		}
	}
	return nil
}
func (batch *Batch) Stop() {
	batch.clock.Stop()
}

var defaultBatchTaskTable = make(map[string][]func(ctx Ctx))

// RegisterBatchTask register batch task with a id
func RegisterBatchTask(id string, fn func(ctx Ctx)) {
	if s, ok := defaultBatchTaskTable[id]; ok {
		defaultBatchTaskTable[id] = append(s, fn)
		return
	}
	defaultBatchTaskTable[id] = []func(ctx Ctx){fn}
}
