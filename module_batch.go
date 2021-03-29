package hybs

import (
	"context"
	"regexp"
	"strconv"
	"time"

	"github.com/patrickmn/go-cache"
	"go.mongodb.org/mongo-driver/mongo"
	"gorm.io/gorm"
)

type batchConfig struct {
	ID            string        `yaml:"id" required:"true"`
	TimeSpecified string        `yaml:"time_specified" default:""`
	TimeOffset    time.Duration `yaml:"time_offset" default:"0"`
	Event         string        `yaml:"event" default:"immediately"`
	RunAt         string        `yaml:"run_at" default:"run_at"`
	StartFrom     string        `yaml:"start_from" default:""`
	StopUntil     string        `yaml:"stop_until" default:""`
	RepeatTimes   int           `yaml:"repeat_times" default:"-1"`
	Interval      time.Duration `yaml:"interval" default:"-1"`
	OnMinute      string        `yaml:"on_minute" default:""`
	OnHour        string        `yaml:"on_hour" default:""`
	OnDay         string        `yaml:"on_day" default:""`
	OnMonth       string        `yaml:"on_month" default:""`
	OnWeekday     string        `yaml:"on_weekday" default:""`
	Tasks         []string      `yaml:"tasks"`
}

type hybsBatch struct {
	config          *batchConfig
	engine          *hybsEngine
	tasks           []func(ctx BatchCtx)
	launchedAt      Time
	lastExecutingAt *Time
	lastExecutedAt  *Time
	nextExecutingAt *Time
	lastCheckedAt   *Time
	executedTimes   int
	userValue       kvPairs

	timeOffset time.Duration

	runAt     *Time
	startFrom *Time
	stopUntil *Time
	onMinute  *regexp.Regexp
	onHour    *regexp.Regexp
	onDay     *regexp.Regexp
	onMonth   *regexp.Regexp
	onWeekday *regexp.Regexp

	pulser chan Time
}

func newHybsBatch(config batchConfig, engine *hybsEngine) *hybsBatch {
	var newBatch = &hybsBatch{
		config:     &config,
		engine:     engine,
		tasks:      make([]func(ctx BatchCtx), 0),
		launchedAt: engine.Now(),
		userValue:  make(kvPairs, 0),
	}
	for {
		// unmarshal time_specified
		if u, err := strconv.Atoi(config.TimeSpecified); err == nil {
			newBatch.timeOffset = TimeUnix(int64(u)).Sub(engine.Now())
			break
		}
		if t, err := TimeParseAll(config.TimeSpecified); err == nil {
			newBatch.timeOffset = t.Sub(engine.Now())
			break
		}
		break
	}
	if t, err := TimeParseAll(config.RunAt); err == nil {
		newBatch.runAt = &t
	}
	var now = newBatch.launchedAt.SecondAt()
	newBatch.nextExecutingAt = &now
	if t, err := TimeParseAll(config.StartFrom); err == nil {
		newBatch.startFrom = &t
		newBatch.nextExecutingAt = newBatch.startFrom
	}
	if t, err := TimeParseAll(config.StopUntil); err == nil {
		newBatch.stopUntil = &t
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
		for _, fn := range s {
			newBatch.tasks = append(newBatch.tasks, fn)
		}
	}
	return newBatch
}

func (batch *hybsBatch) UserValue(key string) (value interface{}, ok bool) {
	return batch.userValue.Get(key)
}

func (batch *hybsBatch) SetUserValue(key string, value interface{}) {
	batch.userValue = batch.userValue.Set(key, value)
}

func (batch *hybsBatch) Now() Time {
	return batch.engine.Now().Add(batch.timeOffset)
}

// force execute tasks without condition check
func (batch *hybsBatch) doExecute(t Time) {
	var start = batch.Now()
	batch.lastExecutingAt = &start
	var ctx = newBatchCtx(batch, t)
	for _, taskFn := range batch.tasks {
		taskFn(ctx)
	}
	var end = batch.Now()
	ctx.executedAt = &end
	batch.executedTimes++
	batch.engine.LogInfof("batch [%s] executed", batch.config.ID)
}
func (batch *hybsBatch) touch(Time) (ok bool) {
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
func (batch *hybsBatch) touchImmediately(t Time) (ok bool) {
	batch.doExecute(t)
	return false
}
func (batch *hybsBatch) touchOnce(t Time) (ok bool) {
	if batch.runAt != nil && !t.Before(*batch.runAt) {
		batch.doExecute(*batch.runAt)
		return false
	}
	return true
}
func (batch *hybsBatch) touchRepeat(t Time) (ok bool) {
	if batch.stopUntil != nil && batch.stopUntil.Before(t) {
		return false
	}
	if batch.config.RepeatTimes > 0 && batch.executedTimes >= batch.config.RepeatTimes {
		return false
	}
	if batch.nextExecutingAt == nil {
		return false
	}
	if batch.startFrom != nil && batch.startFrom.After(t) {
		return true
	}
	if batch.nextExecutingAt.After(t) {
		return true
	}
	var current = batch.nextExecutingAt.SecondAt()
	var next = current.Add(batch.config.Interval)
	batch.nextExecutingAt = &next
	batch.doExecute(current)
	return true
}
func (batch *hybsBatch) touchTimer(t Time) (ok bool) {
	if batch.stopUntil != nil && batch.stopUntil.Before(t) {
		return false
	}
	if batch.config.RepeatTimes > 0 && batch.executedTimes >= batch.config.RepeatTimes {
		return false
	}
	if batch.startFrom != nil && batch.startFrom.After(t) {
		return true
	}
	if batch.lastExecutingAt != nil && batch.lastExecutingAt.Unix()/60 == t.Unix()/60 {
		return true
	}
	var from = batch.launchedAt
	if batch.lastCheckedAt != nil {
		from = *batch.lastCheckedAt
	}
	for checkpoint := from; checkpoint.Before(t); checkpoint = checkpoint.MinuteAt().Add(time.Minute) {
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
func (batch *hybsBatch) start() {
	batch.pulser = make(chan Time)
	for t := range batch.pulser {
		if !batch.touch(t) {
			break
		}
	}
}

// BatchCtx is interface hybsBatchCtx implements
type BatchCtx interface {
	Context() context.Context
	UserValue(key string) (val interface{}, ok bool)
	SetUserValue(key string, value interface{})
	Cache(id string) *cache.Cache
	Redis(id string) RedisClient
	Mongo(id string) *mongo.Database
	MySQL(id string) *gorm.DB
	Sqlite(id string) *gorm.DB
	LogInfof(format string, args ...interface{})
	LogWarnf(format string, args ...interface{})
	LogErrorf(format string, args ...interface{})
	StartedAt() Time
	Now() Time
}

type hybsBatchCtx struct {
	context    context.Context
	batch      *hybsBatch
	startedAt  Time
	executedAt *Time

	at Time
}

func newBatchCtx(batch *hybsBatch, t Time) (ctx *hybsBatchCtx) {
	return &hybsBatchCtx{
		context:    batch.engine.context,
		batch:      batch,
		startedAt:  batch.engine.Now(),
		executedAt: nil,
		at:         t,
	}
}

func (ctx *hybsBatchCtx) Context() context.Context {
	return ctx.context
}
func (ctx *hybsBatchCtx) UserValue(key string) (val interface{}, ok bool) {
	return ctx.batch.UserValue(key)
}
func (ctx *hybsBatchCtx) SetUserValue(key string, value interface{}) {
	ctx.batch.SetUserValue(key, value)
}
func (ctx *hybsBatchCtx) Cache(id string) *cache.Cache {
	return ctx.batch.engine.Cache(id)
}
func (ctx *hybsBatchCtx) Redis(id string) RedisClient {
	return ctx.batch.engine.Redis(id)
}
func (ctx *hybsBatchCtx) Mongo(id string) *mongo.Database {
	return ctx.batch.engine.Mongo(id)
}
func (ctx *hybsBatchCtx) MySQL(id string) *gorm.DB {
	return ctx.batch.engine.MySQL(id)
}
func (ctx *hybsBatchCtx) Sqlite(id string) *gorm.DB {
	return ctx.batch.engine.Sqlite(id)
}
func (ctx *hybsBatchCtx) LogInfof(format string, args ...interface{}) {
	ctx.batch.engine.sysLogger.Infof(format, args...)
}
func (ctx *hybsBatchCtx) LogWarnf(format string, args ...interface{}) {
	ctx.batch.engine.sysLogger.Warnf(format, args...)
}
func (ctx *hybsBatchCtx) LogErrorf(format string, args ...interface{}) {
	ctx.batch.engine.sysLogger.Errorf(format, args...)
}
func (ctx *hybsBatchCtx) StartedAt() Time {
	return ctx.startedAt
}
func (ctx *hybsBatchCtx) Now() Time {
	return ctx.batch.Now()
}

var defaultBatchTaskTable = make(map[string][]func(ctx BatchCtx))

// RegisterBatchTask register batch task with a id
func RegisterBatchTask(id string, fn func(ctx BatchCtx)) {
	if s, ok := defaultBatchTaskTable[id]; ok {
		defaultBatchTaskTable[id] = append(s, fn)
		return
	}
	defaultBatchTaskTable[id] = []func(ctx BatchCtx){fn}
}
