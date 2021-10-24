package plugins

import (
	"fmt"
	"time"

	"github.com/valyala/fasthttp"
)

type LBClientConfig struct {
	ID                  string        `yaml:"id" required:"true"`
	Scheme              string        `yaml:"scheme" default:"http"`
	Host                string        `yaml:"host" required:"true"`
	Port                uint16        `yaml:"port" required:"true"`
	MaxConns            int           `yaml:"max_conns" default:"0"`
	MaxPendingRequests  int           `yaml:"max_pending_requests" default:"0"`
	MaxBatchDelay       time.Duration `yaml:"max_batch_delay" default:"0"`
	DialDualStack       bool          `yaml:"dial_dual_stack" default:"true"`
	IsTLS               bool          `yaml:"is_tls" default:"false"`
	MaxIdleConnDuration time.Duration `yaml:"max_idle_conn_duration" default:"0"`
	ReadBufferSize      int           `yaml:"read_buffer_size" default:"0"`
	WriteBufferSize     int           `yaml:"write_buffer_size" default:"0"`
	ReadTimeout         time.Duration `yaml:"read_timeout" default:"0"`
	WriteTimeout        time.Duration `yaml:"write_timeout" default:"0"`
}

type LBClient struct {
	Config *LBClientConfig
	*fasthttp.PipelineClient
}

func NewLBClient(logger Logger, config LBClientConfig) (plugin *LBClient, err error) {
	return &LBClient{
		PipelineClient: &fasthttp.PipelineClient{
			Addr:                fmt.Sprintf("%s:%d", config.Host, config.Port),
			MaxConns:            config.MaxConns,
			MaxPendingRequests:  config.MaxPendingRequests,
			MaxBatchDelay:       config.MaxBatchDelay,
			DialDualStack:       config.DialDualStack,
			IsTLS:               config.IsTLS,
			MaxIdleConnDuration: config.MaxIdleConnDuration,
			ReadBufferSize:      config.ReadBufferSize,
			WriteBufferSize:     config.WriteBufferSize,
			ReadTimeout:         config.ReadTimeout,
			WriteTimeout:        config.WriteTimeout,
			Logger:              logger,
		},
		Config: &config,
	}, nil
}
