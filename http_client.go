package hybs

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
	"time"
)

// HttpClient represents http client for server to server communication
type HttpClient struct {
	Timeout   time.Duration
	userValue kvPairs
}

func (c *HttpClient) Request(url string, method string, args *fasthttp.Args, respBody interface{}) (statusCode int, err error) {
	var req = fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	var resp = fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	req.Header.SetMethod(method)
	if args != nil {
		req.SetBody(args.QueryString())
	}
	if c.Timeout > 0 {
		if err = fasthttp.DoTimeout(req, resp, c.Timeout); err != nil {
			return 0, err
		}
	} else {
		if err = fasthttp.Do(req, resp); err != nil {
			return 0, err
		}
	}
	statusCode = resp.StatusCode()
	if respBody == nil {
		return
	}
	if err = jsoniter.Unmarshal(resp.Body(), respBody); err != nil {
		return 0, err
	}
	return
}
func (c *HttpClient) Get(url string, args *fasthttp.Args, respBody interface{}) (statusCode int, err error) {
	return c.Request(url, fasthttp.MethodGet, args, respBody)
}
func (c *HttpClient) Post(url string, args *fasthttp.Args, respBody interface{}) (statusCode int, err error) {
	return c.Request(url, fasthttp.MethodPost, args, respBody)
}
func (c *HttpClient) Put(url string, args *fasthttp.Args, respBody interface{}) (statusCode int, err error) {
	return c.Request(url, fasthttp.MethodPut, args, respBody)
}
func (c *HttpClient) Delete(url string, respBody interface{}) (statusCode int, err error) {
	return c.Request(url, fasthttp.MethodDelete, nil, respBody)
}
func (c *HttpClient) Do(req *fasthttp.Request, resp *fasthttp.Response) (err error) {
	if c.Timeout > 0 {
		return fasthttp.DoTimeout(req, resp, c.Timeout)
	}
	return fasthttp.Do(req, resp)
}

// DefaultHttpClient is the default http client for server to server communication
var DefaultHttpClient = &HttpClient{
	Timeout:   time.Second * 2,
	userValue: make(kvPairs, 0),
}

type lbClientConfig struct {
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

type hybsBalancingClient struct {
	config *lbClientConfig
	*fasthttp.PipelineClient
}

func newLBClient(engine *hybsEngine, config lbClientConfig) (newClient *hybsBalancingClient, err error) {
	return &hybsBalancingClient{
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
			Logger:              engine.sysLogger,
		},
		config: &config,
	}, nil
}
