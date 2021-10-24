package web

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hayabusa-cloud/hayabusa/engine"
	"github.com/hayabusa-cloud/hayabusa/plugins"
	"github.com/lucas-clemente/quic-go"
	"github.com/lucas-clemente/quic-go/http3"

	"github.com/NYTimes/gziphandler"
	"github.com/go-playground/pure/v5"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttprouter"
	"gopkg.in/yaml.v2"
)

type ServerConfig struct {
	ID                 string        `yaml:"id" required:"true"`
	HttpVersion        string        `yaml:"http_version" default:"1.1"`
	Network            string        `yaml:"network" default:"tcp"`
	Address            string        `yaml:"address" default:":8088"`
	UseTLS             bool          `yaml:"use_tls" default:"false"`
	CertFile           string        `yaml:"cert_file"`
	KeyFile            string        `yaml:"key_file"`
	Concurrency        int           `yaml:"concurrency" default:"8192"`
	MaxConnectionPerIP int           `yaml:"max_connection_per_ip" default:"1024"`
	ReadTimeout        time.Duration `yaml:"read_timeout" default:"0"`
	WriteTimeout       time.Duration `yaml:"write_timeout" default:"0"`
	MaxRequestBodySize int           `yaml:"max_request_body_size" default:"1024"`
	CompressLevel      int           `yaml:"compress_level" default:"0"`
	HandleTimeout      time.Duration `yaml:"handle_timeout" default:"0"`
	ControllerFilepath string        `yaml:"controller_filepath" default:"controller/"`
	LogLevel           string        `yaml:"log_level" default:"info"`
}

func (c *ServerConfig) isHttp1() bool {
	if strings.EqualFold(c.HttpVersion, "1.1") {
		return true
	}
	if strings.HasPrefix(c.HttpVersion, "1.") {
		return true
	}
	return false
}
func (c *ServerConfig) isHttp2() bool {
	// todo: adapt HTTP/2
	return false
}
func (c *ServerConfig) isHttp3() bool {
	if strings.EqualFold(c.HttpVersion, "3") {
		return true
	}
	if strings.HasPrefix(c.HttpVersion, "3.") {
		return true
	}
	return false
}

// Server represents web API server
type Server struct {
	config               *ServerConfig
	engine               engine.Interface
	httpServer           httpServer
	controllerConfig     controllerConfig
	controllerConfigTree *controllerConfigTree
	serviceMap           map[string]Handler
	middlewareMap        map[string]Middleware
	routerMultiplexers   []routerMultiplexer
	middlewareTree       *middlewareTree
	sysLogger            plugins.Logger
	gameLogger           plugins.Logger

	apiLock *sync.RWMutex
}

// NewServer returns a new hayabusa web server pointer
func NewServer(engine engine.Interface, config ServerConfig) (newServer *Server, err error) {
	// init server structure
	newServer = &Server{
		engine:           engine,
		config:           &config,
		controllerConfig: make(controllerConfig, 0),
		controllerConfigTree: &controllerConfigTree{
			children:   [0x80]*controllerConfigTree{},
			configList: nil,
		},
		routerMultiplexers: make([]routerMultiplexer, 1),
		serviceMap:         make(map[string]Handler),
		middlewareMap:      make(map[string]Middleware),

		apiLock: &sync.RWMutex{},
	}
	// virtual server multiplexed by domain name(todo)
	newServer.routerMultiplexers[0] = routerMultiplexer{
		http11Router: fasthttprouter.New(),
		http3Mux:     pure.New(),
		serverName:   "",
	}
	// create http protocol engine
	if newServer.config.isHttp3() {
		newServer.httpServer, err = newHttp3Server(newServer.config)
		if err != nil {
			return nil, fmt.Errorf("init http3 server error:%s", err)
		}
	} else if newServer.config.isHttp1() {
		newServer.httpServer, err = newHttp11Server(newServer.config)
		if err != nil {
			return nil, fmt.Errorf("init http1.1 server error:%s", err)
		}
	} else {
		return nil, fmt.Errorf("bad http version code:%s", newServer.config.HttpVersion)
	}
	// load controller config files
	var controllerConfigFiles []string
	controllerConfigFiles, err = newServer.loadControllerFiles(config.ControllerFilepath)
	if err != nil {
		return nil, fmt.Errorf("load controller config files error:%s", err)
	}
	var controllerConfigBytes = make([]byte, 0)
	for _, configFile := range controllerConfigFiles {
		var fileBytes []byte
		if fileBytes, err = ioutil.ReadFile(configFile); err != nil {
			return nil, fmt.Errorf("load controller config file %s error:%s", configFile, err)
		} else {
			controllerConfigBytes = append(controllerConfigBytes, fileBytes...)
		}
	}
	if err = yaml.Unmarshal(controllerConfigBytes, &newServer.controllerConfig); err != nil {
		return nil, fmt.Errorf("load controller config files error:%s", err)
	}
	// register builtin and user original middlewares and services
	newServer.registerBuiltinMiddlewares()
	newServer.registerBuiltinServices()
	newServer.registerOriginalMiddlewares()
	newServer.registerOriginalServices()
	return newServer, err
}

func (server *Server) ID() string {
	return server.config.ID
}

// Serve is non-blocking
func (server *Server) Serve(ctx context.Context) (err error) {
	// register controller groups
	if err = server.registerGroups(); err != nil {
		return fmt.Errorf("register controller groups error:%s", err)
	}
	// register controller services
	if err = server.registerServices(); err != nil {
		return fmt.Errorf("register controller services error:%s", err)
	}
	go func() {
		if server.httpServer == nil {
			return
		}
		if err = server.httpServer.serve(); err != nil {
			panic(fmt.Errorf("start web service error:%s", err))
		}
	}()
	fmt.Printf("web service [%s] started, listening on %s://%s\n", server.config.ID, server.config.Network, server.config.Address)
	return
}

func (server *Server) Stop() {
	server.apiLock.Lock()
	defer server.apiLock.Unlock()
	// stop http server
	if server.httpServer != nil {
		if err := server.httpServer.stop(); err != nil {
			server.sysLogger.Errorf("stop web service error:%s", err)
		}
	}
}

func (server *Server) registerBuiltinMiddlewares() {
	server.middlewareMap["Builtin:ResponseJSON"] = middlewareBuiltinResponseJSON(server)
	server.middlewareMap["Builtin:ResponseFound"] = middlewareBuiltinResponseFound(server)
	server.middlewareMap["Builtin:Authentication"] = middlewareBuiltinAuthentication(server)
	server.middlewareMap["Builtin:SysLog"] = middlewareBuiltinSysLog(server)
	server.middlewareMap["Builtin:GameLog"] = middlewareBuiltinGameLog(server)
}

func (server *Server) registerBuiltinServices() {
	server.serviceMap["Builtin:Document"] = builtinServiceDocument(server)
}

func (server *Server) SetSysLogger(l plugins.Logger) *Server {
	server.sysLogger = l
	return server
}

func (server *Server) SetGameLogger(l plugins.Logger) *Server {
	server.gameLogger = l
	return server
}

func (server *Server) RegisterPluginCacheMiddleware(id string) {
	server.middlewareMap["Plugin:"+id] = middlewarePluginCache(server.engine, id)
}

func (server *Server) RegisterPluginRedisMiddleware(id string) {
	server.middlewareMap["Plugin:"+id] = middlewarePluginRedis(server.engine, id)
}

func (server *Server) RegisterPluginMongoMiddleware(id string) {
	server.middlewareMap["Plugin:"+id] = middlewarePluginMongo(server.engine, id)
}

func (server *Server) RegisterPluginMySQLMiddleware(id string) {
	server.middlewareMap["Plugin:"+id] = middlewarePluginMySQL(server.engine, id)
}

func (server *Server) RegisterPluginSqliteMiddleware(id string) {
	server.middlewareMap["Plugin:"+id] = middlewarePluginSqlite3(server.engine, id)
}

func (server *Server) registerOriginalMiddlewares() {
	for _, loopMiddleware := range middlewareTupleList {
		server.middlewareMap[loopMiddleware.middlewareID] = loopMiddleware.m
	}
}

func (server *Server) registerOriginalServices() {
	for _, loopService := range serviceTupleList {
		server.serviceMap[loopService.serviceID] = loopService.handler
	}
}

func (server *Server) loadControllerFiles(path string) (files []string, err error) {
	fileInfos, err := ioutil.ReadDir(path)
	if err != nil {
		return []string{}, fmt.Errorf("load controller config directory %s error:%s", path, err)
	}
	files = make([]string, 0)
	for _, fileInfo := range fileInfos {
		var fileName = filepath.Join(path, fileInfo.Name())
		if fileInfo.IsDir() {
			appendingFiles, err := server.loadControllerFiles(filepath.Join(fileName, ""))
			if err != nil {
				return files, err
			}
			files = append(files, appendingFiles...)
			continue
		}
		if !strings.HasSuffix(fileInfo.Name(), ".yml") && !strings.HasSuffix(fileInfo.Name(), ".toml") {
			continue
		}
		files = append(files, fileName)
	}
	return files, nil
}

func (server *Server) pathMiddleware(controller controllerConfigBase) (middleware Middleware, err error) {
	middleware = middleware1()
	// ip control
	if len(controller.Allow) > 0 || len(controller.Deny) > 0 {
		middleware = middleware.Right(middlewareBuiltinIPControl(controller.Allow, controller.Deny))
	}
	// slow query
	middleware = middleware.Left(middlewareBuiltinSlowQuery(server, controller.SlowQueryWarn, controller.SlowQueryError))
	// path params
	for _, pathParamsConfig := range controller.PathParams {
		middleware = middleware.Left(middlewareBuiltinPathParamsAllow(pathParamsConfig.Name, pathParamsConfig.Allow))
		middleware = middleware.Left(middlewareBuiltinPathParamsDeny(pathParamsConfig.Name, pathParamsConfig.Deny))
	}
	// query params
	for _, queryParamsConfig := range controller.QueryArgs {
		middleware = middleware.Left(middlewareBuiltinFormValueAllow(queryParamsConfig.Name, queryParamsConfig.Allow))
		middleware = middleware.Left(middlewareBuiltinFormValueDeny(queryParamsConfig.Name, queryParamsConfig.Deny))
	}
	// set const value
	for _, constParamsConfig := range controller.ConstParams {
		middleware = middleware.Left(middlewareBuiltinConstParam(constParamsConfig.Name, constParamsConfig.Value))
	}
	// set response links
	middleware = middleware.Left(middlewareBuiltinResponseLinks(controller.Links))
	// set default status code
	middleware = middleware.Left(middlewareBuiltinDefaultStatusCode())
	// use controller middlewares
	for _, middlewareName := range controller.Middlewares {
		m, ok := server.middlewareMap[middlewareName]
		if !ok {
			return middleware0(), fmt.Errorf("middleware %s not found", middlewareName)
		}
		if middleware == nil {
			middleware = m
		} else {
			middleware = middleware.Left(m)
		}
	}
	return middleware, nil
}

func (server *Server) registerGroups() (err error) {
	server.middlewareTree = &middlewareTree{}
	for _, controller := range server.controllerConfig {
		var middleware Middleware = nil
		if middleware, err = server.pathMiddleware(controller.Base); err != nil {
			return err
		}
		if middleware != nil {
			server.middlewareTree.add([]byte(controller.Location), middleware)
		}
		var insertController = controller // copy instance
		server.controllerConfigTree.add([]byte(controller.Location), &insertController)
	}
	return
}

func (server *Server) registerServices() (err error) {
	var middlewareSystemInfo = middlewareBuiltinSystemInfo(server)
	for _, controller := range server.controllerConfig {
		hostName := []byte(controller.ServerName)
		if len(controller.Services) < 1 {
			continue
		}
		var middlewareWithPath = server.middlewareTree.combine([]byte(controller.Location))
		for _, serviceConfig := range controller.Services {
			var middlewareWithMethod, err = server.pathMiddleware(serviceConfig.Base)
			if err != nil {
				return err
			}
			if middlewareWithMethod != nil {
				middlewareWithMethod = middlewareWithPath.Left(middlewareWithMethod)
			} else {
				middlewareWithMethod = middlewareWithPath
			}
			middlewareWithMethod = middlewareWithMethod.Right(middlewareSystemInfo)
			if serviceConfig.ServiceID == "" {
				continue
			}
			handler, ok := server.serviceMap[serviceConfig.ServiceID]
			if !ok {
				return fmt.Errorf("service function %s not found", serviceConfig.ServiceID)
			}
			handler = middlewareWithMethod.Apply(handler)
			// HTTP/1.x
			virtualHandler1 := func(ctx *fasthttp.RequestCtx, params fasthttprouter.Params) {
				if len(hostName) > 0 && !bytes.EqualFold(ctx.Host(), hostName) {
					ctx.NotFound()
					return
				}
				rawHttp11Handler(server, handler)(ctx, params)
			}
			server.routerMultiplexers[0].http11Router.Handle(serviceConfig.Method, controller.Location, virtualHandler1)
			// HTTP/3
			notFoundBytes := []byte("not found")
			virtualHandler3 := func(rw http.ResponseWriter, req *http.Request) {
				if len(hostName) > 0 && !bytes.EqualFold([]byte(req.Host), hostName) {
					rw.WriteHeader(http.StatusNotFound)
					_, _ = rw.Write(notFoundBytes)
					return
				}
				rawHttp3Handler(server, handler).ServeHTTP(rw, req)
			}
			server.routerMultiplexers[0].http3Mux.Handle(serviceConfig.Method, controller.Location, virtualHandler3)
		}
	}
	// builtin health check API
	server.routerMultiplexers[0].http11Router.GET("/__health_check__", func(ctx *fasthttp.RequestCtx, params fasthttprouter.Params) {
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	server.routerMultiplexers[0].http3Mux.Get("/__health_check__", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	var handleTimeout, compressLevel = server.config.HandleTimeout, server.config.CompressLevel
	if server.config.isHttp1() {
		// HTTP/1.x
		var routerRawHandler1 = server.routerMultiplexers[0].http11Router.Handler
		if handleTimeout > 0 {
			if handleTimeout < minimumTimeout {
				handleTimeout = minimumTimeout
			}
			routerRawHandler1 = fasthttp.TimeoutHandler(routerRawHandler1, handleTimeout, "internal timeout")
		}
		if compressLevel > fasthttp.CompressNoCompression && compressLevel <= fasthttp.CompressBestCompression {
			routerRawHandler1 = fasthttp.CompressHandlerLevel(routerRawHandler1, compressLevel)
		}
		server.httpServer.setHandler(fasthttp.RequestHandler(routerRawHandler1))
	} else if server.config.isHttp2() {
		// todo
	} else if server.config.isHttp3() {
		// HTTP/3
		var routerRawHandler3 = server.routerMultiplexers[0].http3Mux.Serve()
		if handleTimeout > 0 {
			routerRawHandler3 = http.TimeoutHandler(routerRawHandler3, handleTimeout, "internal timeout")
		}
		if compressLevel > fasthttp.CompressNoCompression && compressLevel <= fasthttp.CompressBestCompression {
			if http3GzipHandler, err := gziphandler.NewGzipLevelHandler(compressLevel); err != nil {
				return err
			} else {
				routerRawHandler3 = http3GzipHandler(routerRawHandler3)
			}
		}
		server.httpServer.setHandler(routerRawHandler3)
	}
	return nil
}

type httpServer interface {
	serve() (err error)
	setListener(l net.Listener)
	stop() (err error)
	setHandler(routerHandler interface{})
}

// for HTTP/1.1
type http11Server struct {
	*ServerConfig
	netListener net.Listener
	httpServer  *fasthttp.Server
}

func newHttp11Server(config *ServerConfig) (newServer httpServer, err error) {
	// create new http11Server
	newServer = &http11Server{ServerConfig: config}
	// init netListener
	newServer.(*http11Server).netListener, err = net.Listen(config.Network, config.Address)
	if err != nil {
		return nil, fmt.Errorf("cannot listen at %s:%s", config.Address, err)
	}
	newServer.(*http11Server).httpServer = &fasthttp.Server{
		Name:               "hayabusa",
		Concurrency:        config.Concurrency,
		MaxConnsPerIP:      config.MaxConnectionPerIP,
		ReadTimeout:        config.ReadTimeout,
		WriteTimeout:       config.WriteTimeout,
		MaxRequestBodySize: config.MaxRequestBodySize * 1024,
	}
	return newServer, err
}

func (server *http11Server) serve() (err error) {
	var config = server.ServerConfig
	if !config.UseTLS || server.CertFile == "" || server.KeyFile == "" {
		return server.httpServer.Serve(server.netListener)
	}
	return server.httpServer.ServeTLS(server.netListener, server.CertFile, server.KeyFile)
}
func (server *http11Server) setListener(l net.Listener) {
	server.netListener = l
}
func (server *http11Server) stop() (err error) {
	return server.httpServer.Shutdown()
}
func (server *http11Server) setHandler(routerHandler interface{}) {
	server.httpServer.Handler = routerHandler.(fasthttp.RequestHandler)
}

// for HTTP/3
type http3Server struct {
	*ServerConfig
	http3Server *http3.Server
}

func newHttp3Server(config *ServerConfig) (newServer httpServer, err error) {
	// create new http3 server from quic-go server
	newServer = &http3Server{ServerConfig: config}
	newServer.(*http3Server).http3Server = &http3.Server{
		Server: &http.Server{
			Addr:         config.Address,
			ReadTimeout:  config.ReadTimeout,
			WriteTimeout: config.WriteTimeout,
			TLSConfig:    &tls.Config{},
		},
		QuicConfig: &quic.Config{},
	}
	return newServer, err
}

func (server *http3Server) serve() (err error) {
	var cfg = server.ServerConfig
	if !cfg.UseTLS || server.CertFile == "" || server.KeyFile == "" {
		return server.http3Server.ListenAndServe()
	}
	return server.http3Server.ListenAndServeTLS(server.CertFile, server.KeyFile)
}
func (server *http3Server) setListener(l net.Listener) {
}
func (server *http3Server) stop() (err error) {
	return server.http3Server.Shutdown(context.Background())
}
func (server *http3Server) setHandler(routerHandler interface{}) {
	server.http3Server.Handler = routerHandler.(http.Handler)
}

const (
	minimumTimeout = time.Millisecond
)
