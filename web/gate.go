package web

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/hayabusa-cloud/hayabusa/engine"
	"github.com/hayabusa-cloud/hayabusa/plugins"
	"github.com/valyala/fasthttp"
)

type GateConfig struct {
	ServerConfig `yaml:",inline"`
	Gateways     []struct {
		ServerName string   `yaml:"server_name" default:""`
		LBClients  []string `yaml:"lb_clients"`
	} `yaml:"gateways"`
	Timeout time.Duration `yaml:"timeout" default:"2s"`
}

type httpGate interface {
	httpServer
}

func newHttp11Gate(engine engine.Interface, config *GateConfig) (httpGate, error) {
	var server, err = newHttp11Server(&config.ServerConfig)
	if err != nil {
		return nil, err
	}
	var lbMap = make(map[string]*fasthttp.LBClient)
	for _, gateway := range config.Gateways {
		// load balancing
		var clients = make([]*fasthttp.PipelineClient, 0, len(gateway.LBClients))
		for _, clientID := range gateway.LBClients {
			var lbClient = engine.LBClient(clientID)
			if lbClient == nil {
				return nil, fmt.Errorf("load balancing client [%s] not exists", clientID)
			}
			clients = append(clients, lbClient.PipelineClient)
		}
		var lb = &fasthttp.LBClient{
			Clients: make([]fasthttp.BalancingClient, 0, len(clients)),
			HealthCheck: func(req *fasthttp.Request, resp *fasthttp.Response, err error) bool {
				return err == nil
			},
			Timeout: fasthttp.DefaultLBClientTimeout,
		}
		if config.Timeout >= time.Millisecond {
			lb.Timeout = config.Timeout
		}
		for i := 0; i < len(clients); i++ {
			lb.Clients = append(lb.Clients, clients[i])
		}
		if gateway.ServerName == "" {
			lbMap["*"] = lb
		} else {
			lbMap[gateway.ServerName] = lb
		}
	}
	var defaultLB *fasthttp.LBClient = nil
	var defaultServerName []byte
	for k, v := range lbMap {
		defaultLB = v
		if k == "" {
			defaultServerName = []byte("*")
		} else {
			defaultServerName = []byte(k)
		}
		break
	}
	if defaultLB == nil {
		return nil, fmt.Errorf("http gate server [%s] has no gateway rules", config.ID)
	}
	// when has just only one gateway rule
	if len(config.Gateways) == 1 {
		server.(*http11Server).httpServer.Handler = func(ctx *fasthttp.RequestCtx) {
			ctx.Request.Header.Set("X-Real-IP", ctx.RemoteIP().String())
			if string(defaultServerName) != "*" && !bytes.EqualFold(ctx.Request.Host(), defaultServerName) {
				ctx.Response.SetStatusCode(fasthttp.StatusNotFound)
				return
			}
			if err := defaultLB.DoTimeout(&ctx.Request, &ctx.Response, defaultLB.Timeout); err != nil {
				ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
			}
		}
		return server.(httpGate), err
	}
	// when has several gateway rules
	server.(*http11Server).httpServer.Handler = func(ctx *fasthttp.RequestCtx) {
		var reqServerName = "*"
		ctx.Request.Header.Set("X-Real-IP", ctx.RemoteIP().String())
		if ctx.Request.Host() != nil && len(ctx.Request.Host()) > 0 {
			reqServerName = string(ctx.Request.Host())
		}
		var lb, ok = lbMap[reqServerName]
		if !ok {
			ctx.Response.SetStatusCode(fasthttp.StatusNotFound)
			return
		}
		if err := lb.DoTimeout(&ctx.Request, &ctx.Response, defaultLB.Timeout); err != nil {
			ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		}
	}
	return server.(httpGate), err
}

type http3GateHandlerImpl struct {
	gateConfig   *GateConfig
	currentIndex map[string]int
	lb           map[string][]*plugins.LBClientConfig

	mu sync.Mutex
}

func (h *http3GateHandlerImpl) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var reqServerName = "*"
	req.Header.Set("X-Real-IP", req.RemoteAddr)
	if req.Host != "" {
		reqServerName = req.Host
	}
	h.mu.Lock()
	var s, ok = h.lb[reqServerName]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		h.mu.Unlock()
		return
	}
	var idx, ok2 = h.currentIndex[reqServerName]
	if !ok2 {
		w.WriteHeader(http.StatusNotFound)
		h.mu.Unlock()
		return
	}
	var c = s[idx]
	h.currentIndex[reqServerName] = (idx + 1) % len(s)
	h.mu.Unlock()
	// proxy request
	var newUrl = fmt.Sprintf("%s://%s:%d%s", c.Scheme, c.Host, c.Port, req.URL.RequestURI())
	var newRequest, err = http.NewRequest(req.Method, newUrl, req.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	newRequest.Header.Set("X-Real-IP", req.RemoteAddr)
	for header, values := range req.Header {
		for _, value := range values {
			newRequest.Header.Add(header, value)
		}
	}
	if res, err := http.DefaultClient.Do(newRequest); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	} else {
		if err = res.Write(w); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}

func newHttp3Gate(engine engine.Interface, config *GateConfig) (httpGate, error) {
	var server, err = newHttp3Server(&config.ServerConfig)
	if err != nil {
		return nil, err
	}
	var rawServer = server.(*http3Server).http3Server

	var h = &http3GateHandlerImpl{
		gateConfig:   config,
		currentIndex: make(map[string]int),
		lb:           make(map[string][]*plugins.LBClientConfig),

		mu: sync.Mutex{},
	}
	for _, gateway := range config.Gateways {
		var serverName = "*"
		if gateway.ServerName == "" {
			serverName = gateway.ServerName
		}
		h.currentIndex[serverName] = 0
		h.lb[serverName] = make([]*plugins.LBClientConfig, 0, len(gateway.LBClients))
		for _, clientID := range gateway.LBClients {
			var lbClient = engine.LBClient(clientID)
			if lbClient == nil {
				return nil, fmt.Errorf("load balancing client [%s] not exists", clientID)
			}
			h.lb[serverName] = append(h.lb[serverName], lbClient.Config)
		}
	}

	rawServer.Handler = h
	return server.(httpGate), err
}

// Gate represents web API proxy server
type Gate struct {
	engine engine.Interface
	httpGate
	Config *GateConfig
}

func NewGate(engine engine.Interface, config GateConfig) (*Gate, error) {
	if config.isHttp1() {
		var gate, err = newHttp11Gate(engine, &config)
		if err != nil {
			return nil, err
		}
		return &Gate{
			httpGate: gate,
			Config:   &config,
			engine:   engine,
		}, nil
	} else if config.isHttp3() {
		var g, err = newHttp3Gate(engine, &config)
		if err != nil {
			return nil, err
		}
		return &Gate{
			httpGate: g,
			Config:   &config,
			engine:   engine,
		}, nil
	}
	return nil, fmt.Errorf("unsupprted HTTP protocol version")
}

func (g *Gate) ID() string {
	return g.Config.ID
}

// Serve is non-blocking
func (g *Gate) Serve(ctx context.Context) (err error) {
	go func() {
		if g.httpGate == nil {
			return
		}
		if err = g.httpGate.serve(); err != nil {
			panic(fmt.Errorf("start http(s) gate error:%s", err))
		}
	}()
	fmt.Printf("web API gate [%s] started, listening on %s://%s\n", g.Config.ID, g.Config.Network, g.Config.Address)
	return
}

func (g *Gate) Stop() {
	if err := g.httpGate.stop(); err != nil {
		fmt.Printf("stop web API gate error:%s\n", err)
	}
}
