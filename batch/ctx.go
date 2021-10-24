package batch

import (
	"time"

	"github.com/hayabusa-cloud/hayabusa/engine"
	"github.com/hayabusa-cloud/hayabusa/plugins"
)

// Ctx is interface batchCtx implements
type Ctx interface {
	// UserValue returns the value stored via SetUserValue under the given key.
	UserValue(key string) (val interface{}, ok bool)
	// SetUserValue stores the given value under the given key in BatchCtx.
	SetUserValue(key string, value interface{})
	// Cache returns the cache instance under the given id
	Cache(id string) *plugins.Cache
	// Redis returns the RedisClient instance under the given id
	Redis(id string) *plugins.Redis
	// Mongo returns the mongodb client under the given id
	Mongo(id string) *plugins.Mongo
	// MySQL returns the mysql client under the given id
	MySQL(id string) *plugins.MySQL
	// Sqlite returns the sqlite instance under the given id
	Sqlite(id string) *plugins.Sqlite
	// Infof writes info level log
	Infof(format string, args ...interface{})
	// Warnf writes warning level log
	Warnf(format string, args ...interface{})
	// Errorf writes error level log
	Errorf(format string, args ...interface{})
	// StartedAt returns the batch started executing time
	StartedAt() time.Time
	// Now returns the logical current time
	Now() time.Time
}

type batchCtx struct {
	engine     engine.Interface
	batch      *Batch
	startedAt  time.Time
	executedAt *time.Time

	at time.Time
}

func newBatchCtx(batch *Batch, t time.Time) (ctx *batchCtx) {
	return &batchCtx{
		engine:     batch.engine,
		batch:      batch,
		startedAt:  batch.engine.Now(),
		executedAt: nil,
		at:         t,
	}
}

func (ctx *batchCtx) UserValue(key string) (val interface{}, ok bool) {
	return ctx.batch.UserValue(key)
}
func (ctx *batchCtx) SetUserValue(key string, value interface{}) {
	ctx.batch.SetUserValue(key, value)
}
func (ctx *batchCtx) Cache(id string) *plugins.Cache {
	return ctx.engine.Cache(id)
}
func (ctx *batchCtx) Redis(id string) *plugins.Redis {
	return ctx.engine.Redis(id)
}
func (ctx *batchCtx) Mongo(id string) *plugins.Mongo {
	return ctx.engine.Mongo(id)
}
func (ctx *batchCtx) MySQL(id string) *plugins.MySQL {
	return ctx.engine.MySQL(id)
}
func (ctx *batchCtx) Sqlite(id string) *plugins.Sqlite {
	return ctx.engine.Sqlite(id)
}
func (ctx *batchCtx) Infof(format string, args ...interface{}) {
	ctx.engine.Infof(format, args...)
}
func (ctx *batchCtx) Warnf(format string, args ...interface{}) {
	ctx.engine.Warnf(format, args...)
}
func (ctx *batchCtx) Errorf(format string, args ...interface{}) {
	ctx.engine.Errorf(format, args...)
}
func (ctx *batchCtx) StartedAt() time.Time {
	return ctx.startedAt
}
func (ctx *batchCtx) Now() time.Time {
	return ctx.batch.Now()
}
