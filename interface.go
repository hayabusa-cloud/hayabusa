package hayabusa

import (
	"context"
	"time"

	"github.com/hayabusa-cloud/hayabusa/plugins"
)

// Engine is interface of engine implements
type Engine interface {
	plugins.Logger
	Stop() (err error)
	MasterData() plugins.MasterDataManager
	LBClient(id string) *plugins.LBClient
	Cache(id string) *plugins.Cache
	Redis(id string) *plugins.Redis
	Mongo(id string) *plugins.Mongo
	MySQL(id string) *plugins.MySQL
	Sqlite(id string) *plugins.Sqlite
	Now() time.Time
	AppConfig() string
	Context() context.Context
	UserValue(key string) (value interface{}, ok bool)
	SetUserValue(key string, value interface{})
}

type service interface {
	ID() string
	Serve(ctx context.Context) (err error)
	Stop()
}
type scheduler interface {
	Start(ctx context.Context)
	Stop()
}
