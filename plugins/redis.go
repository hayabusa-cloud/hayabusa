package plugins

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

type RedisConfig struct {
	ID           string        `yaml:"id" required:"true"`
	Network      string        `yaml:"network" default:"tcp"`
	Address      string        `yaml:"address" default:"localhost:6379"`
	Username     string        `yaml:"username" default:""`
	Password     string        `yaml:"password" default:""`
	DB           int           `yaml:"db" default:"0"`
	DialTimeout  time.Duration `yaml:"dial_timeout" default:"0"`
	ReadTimeout  time.Duration `yaml:"read_timeout" default:"0"`
	WriteTimeout time.Duration `yaml:"write_timeout" default:"0"`
	PoolSize     int           `yaml:"pool_size" default:"0"`
	MinIdleConns int           `yaml:"min_idle_conns" default:"0"`
	MaxConnAge   time.Duration `yaml:"max_conn_age" default:"0"`
	PoolTimeout  time.Duration `yaml:"pool_timeout" default:"0"`
	IdleTimeout  time.Duration `yaml:"idle_timeout" default:"0"`
}

type Redis struct {
	Config *RedisConfig
	redis.UniversalClient
	locker   *redsync.Redsync
	mutexMap map[string]*redsync.Mutex
	rw       sync.RWMutex
}

func NewRedis(config RedisConfig) (plugin *Redis, err error) {
	var client = redis.NewClient(&redis.Options{
		Network:      config.Network,
		Addr:         config.Address,
		Username:     config.Username,
		Password:     config.Password,
		DB:           config.DB,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		PoolSize:     config.PoolSize,
		IdleTimeout:  config.IdleTimeout,
	})
	var pool = goredis.NewPool(client)
	var locker = redsync.New(pool)
	plugin = &Redis{
		Config:          &config,
		UniversalClient: client,
		locker:          locker,
		mutexMap:        make(map[string]*redsync.Mutex),
		rw:              sync.RWMutex{},
	}
	return plugin, nil
}

func (r *Redis) Locker() *redsync.Redsync {
	return r.locker
}
func (r *Redis) Mutex(name string, options ...redsync.Option) (mu *redsync.Mutex) {
	r.rw.RLock()
	var val, ok = r.mutexMap[name]
	r.rw.RUnlock()
	if ok {
		return val
	}
	mu = r.locker.NewMutex(name, options...)
	r.rw.Lock()
	r.mutexMap[name] = mu
	r.rw.Unlock()
	return mu
}
func (r *Redis) Lock(key string, options ...redsync.Option) (err error) {
	return r.Mutex(key, options...).Lock()
}
func (r *Redis) Unlock(key string) (status bool, err error) {
	r.rw.RLock()
	var val, ok = r.mutexMap[key]
	r.rw.RUnlock()
	if !ok {
		return false, fmt.Errorf("no lock")
	}
	return val.Unlock()
}
