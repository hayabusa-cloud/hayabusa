package hybs

import (
	"bytes"
	"fmt"
	"github.com/valyala/fasthttp"
	"net/http"
	"sync"
	"time"
)

type httpGateConfig struct {
	httpServerConfig `yaml:",inline"`
	Gateways         []struct {
		ServerName string   `yaml:"server_name" default:""`
		LBClients  []string `yaml:"lb_clients"`
	} `yaml:"gateways"`
}

type httpGate interface {
	httpServer
}

func newFasthttpGate(config *httpGateConfig, lbClients []*hybsBalancingClient) (httpGate, error) {
	var server, err = newFasthttpServer(&config.httpServerConfig)
	if err != nil {
		return nil, err
	}
	var lbMap = make(map[string]*fasthttp.LBClient)
	for _, gateway := range config.Gateways {
		// load balancing
		var clients = make([]*fasthttp.PipelineClient, 0, len(gateway.LBClients))
		for _, clientID := range gateway.LBClients {
			var ok = false
			for _, client := range lbClients {
				if client.config.ID == clientID {
					clients = append(clients, client.PipelineClient)
					ok = true
					break
				}
			}
			if !ok {
				return nil, fmt.Errorf("load balancing client [%s] not registered", clientID)
			}
		}
		var lb = &fasthttp.LBClient{
			Clients: make([]fasthttp.BalancingClient, 0, len(clients)),
			HealthCheck: func(req *fasthttp.Request, resp *fasthttp.Response, err error) bool {
				return err == nil
			},
			Timeout: time.Second * 2,
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
		server.(*fasthttpServer).httpServer.Handler = func(ctx *fasthttp.RequestCtx) {
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
	server.(*fasthttpServer).httpServer.Handler = func(ctx *fasthttp.RequestCtx) {
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
	gateConfig   *httpGateConfig
	currentIndex map[string]int
	lb           map[string][]*lbClientConfig

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

func newHttp3Gate(config *httpGateConfig, lbClients []*hybsBalancingClient) (httpGate, error) {
	var server, err = newHttp3Server(&config.httpServerConfig)
	if err != nil {
		return nil, err
	}
	var rawServer = server.(*http3Server).http3Server

	var h = &http3GateHandlerImpl{
		gateConfig:   config,
		currentIndex: make(map[string]int),
		lb:           make(map[string][]*lbClientConfig),

		mu: sync.Mutex{},
	}
	for _, gateway := range config.Gateways {
		var serverName = "*"
		if gateway.ServerName == "" {
			serverName = gateway.ServerName
		}
		h.currentIndex[serverName] = 0
		h.lb[serverName] = make([]*lbClientConfig, 0, len(gateway.LBClients))
		for _, clientID := range gateway.LBClients {
			var ok = false
			for _, client := range lbClients {
				if clientID == client.config.ID {
					h.lb[serverName] = append(h.lb[serverName], client.config)
					ok = true
					break
				}
			}
			if !ok {
				return nil, fmt.Errorf("lb_client [%s] not exists", clientID)
			}
		}
	}

	rawServer.Handler = h
	return server.(httpGate), err
}

type hybsHttpGate struct {
	httpGate
	config *httpGateConfig
	engine *hybsEngine
}

func newHttpGate(engine *hybsEngine, config httpGateConfig) (*hybsHttpGate, error) {
	if config.isHttp1() {
		var g, err = newFasthttpGate(&config, engine.lbClients)
		if err != nil {
			return nil, err
		}
		return &hybsHttpGate{
			httpGate: g,
			config:   &config,
			engine:   engine,
		}, nil
	} else if config.isHttp3() {
		var g, err = newHttp3Gate(&config, engine.lbClients)
		if err != nil {
			return nil, err
		}
		return &hybsHttpGate{
			httpGate: g,
			config:   &config,
			engine:   engine,
		}, nil
	}
	return nil, fmt.Errorf("unsupprted HTTP protocol version")
}

func (gate *hybsHttpGate) serve() (err error) {
	// listen and serve
	go func() {
		if gate.httpGate == nil {
			return
		}
		if err := gate.httpGate.serve(); err != nil {
			panic(fmt.Errorf("start http(s) gate failed:%s", err))
		}
	}()
	return
}
