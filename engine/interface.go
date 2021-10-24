package engine

import (
	"context"
	"time"

	"github.com/hayabusa-cloud/hayabusa/plugins"
)

type Interface interface {
	plugins.Interface
	plugins.Logger
	AppName() string
	Version() string
	Env() string
	Context() context.Context
	UserValue(key string) (value interface{}, ok bool)
	SetUserValue(key string, value interface{})
	Now() time.Time
	DebugMode() bool
}
