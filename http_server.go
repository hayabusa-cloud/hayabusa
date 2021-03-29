package hybs

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/lucas-clemente/quic-go"
	"github.com/lucas-clemente/quic-go/http3"
	"io/ioutil"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/go-playground/pure/v5"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttprouter"
	"gopkg.in/yaml.v2"
)

type hybsHttpServer struct {
	engine               *hybsEngine
	httpConfig           *httpServerConfig
	httpServer           httpServer
	controllerConfig     controllerConfig
	controllerConfigTree *controllerConfigTree
	serviceMap           map[string]ServiceHandler
	middlewareMap        map[string]Middleware
	virtualRouters       []virtualRouter
	middlewareTree       *middlewareTree

	apiLock *sync.RWMutex
}

// newHttpServer returns a new hayabusa server pointer
func newHttpServer(engine *hybsEngine, config httpServerConfig) (server *hybsHttpServer, err error) {
	// init server structure
	var newServer = &hybsHttpServer{
		engine:           engine,
		httpConfig:       &config,
		controllerConfig: make(controllerConfig, 0),
		controllerConfigTree: &controllerConfigTree{
			children:   [128]*controllerConfigTree{},
			configList: nil,
		},
		virtualRouters: make([]virtualRouter, 1),
		serviceMap:     make(map[string]ServiceHandler),
		middlewareMap:  make(map[string]Middleware),

		apiLock: &sync.RWMutex{},
	}
	// virtual server by domain name
	newServer.virtualRouters[0] = virtualRouter{
		rawRouter:  fasthttprouter.New(),
		http3Mux:   pure.New(),
		serverName: "",
	}
	// create http protocol engine
	if newServer.httpConfig.isHttp3() {
		newServer.httpServer, err = newHttp3Server(newServer.httpConfig)
		if err != nil {
			return nil, fmt.Errorf("init http3 server failed:%s", err)
		}
	} else if newServer.httpConfig.isHttp1() {
		newServer.httpServer, err = newFasthttpServer(newServer.httpConfig)
		if err != nil {
			return nil, fmt.Errorf("init http1.1 server failed:%s", err)
		}
	} else {
		return nil, fmt.Errorf("bad http version code:%s", newServer.httpConfig.HttpVersion)
	}
	// load controller config files
	var controllerConfigFiles []string
	controllerConfigFiles, err = newServer.loadControllerFiles(config.ControllerFilepath)
	if err != nil {
		return nil, fmt.Errorf("load controller config files failed:%s", err)
	}
	var controllerConfigBytes = make([]byte, 0)
	for _, configFile := range controllerConfigFiles {
		var fileBytes []byte
		if fileBytes, err = ioutil.ReadFile(configFile); err != nil {
			return nil, fmt.Errorf("load controller config file %s failed:%s", configFile, err)
		} else {
			controllerConfigBytes = append(controllerConfigBytes, fileBytes...)
		}
	}
	if err = yaml.Unmarshal(controllerConfigBytes, &newServer.controllerConfig); err != nil {
		return nil, fmt.Errorf("load controller config files failed:%s", err)
	}
	return newServer, err
}

func (server *hybsHttpServer) serve() (err error) {
	// register controller groups
	if err := server.registerGroup(); err != nil {
		return fmt.Errorf("register controller groups failed:%s\n", err)
	}
	// register API
	if err := server.registerAPI(); err != nil {
		return fmt.Errorf("register API failed:%s", err)
	}
	// listen and serve
	go func() {
		if server.httpServer == nil {
			return
		}
		if err := server.httpServer.serve(); err != nil {
			panic(fmt.Errorf("start http(s) service failed:%s", err))
		}
	}()
	return
}

func (server *hybsHttpServer) stop() (err error) {
	server.apiLock.Lock()
	defer server.apiLock.Unlock()
	// stop http server
	if server.httpServer != nil {
		if err = server.httpServer.stop(); err != nil {
			server.engine.sysLogger.Errorf("stop http server failed:%s", err)
		}
	}
	return err
}

func (server *hybsHttpServer) loadControllerFiles(path string) (files []string, err error) {
	fileInfos, err := ioutil.ReadDir(path)
	if err != nil {
		return []string{}, fmt.Errorf("load controller config directory %s failed:%s", path, err)
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

func (server *hybsHttpServer) pathMiddleware(controller controllerConfigBase) (middleware Middleware, err error) {
	middleware = middleware1()
	// ip control check
	if len(controller.Allow) > 0 || len(controller.Deny) > 0 {
		middleware = middleware.Right(middlewareBuiltinIPControl(controller.Allow, controller.Deny))
	}
	// slow query check
	middleware = middleware.Left(middlewareBuiltinSlowQuery(server, controller.SlowQueryWarn, controller.SlowQueryError))
	// path params check
	for _, pathParamsConfig := range controller.PathParams {
		middleware = middleware.Left(middlewareBuiltinPathParamsAllow(pathParamsConfig.Name, pathParamsConfig.Allow))
		middleware = middleware.Left(middlewareBuiltinPathParamsDeny(pathParamsConfig.Name, pathParamsConfig.Deny))
	}
	// query params check
	for _, queryParamsConfig := range controller.QueryArgs {
		middleware = middleware.Left(middlewareBuiltinFormValueAllow(queryParamsConfig.Name, queryParamsConfig.Allow))
		middleware = middleware.Left(middlewareBuiltinFormValueDeny(queryParamsConfig.Name, queryParamsConfig.Deny))
	}
	// set const value
	for _, constParamsConfig := range controller.ConstParams {
		middleware = middleware.Left(middlewareBuiltinConstParam(constParamsConfig.Name, constParamsConfig.Value))
	}
	// set response links
	middleware = middleware.Left(middlewareBuiltinResponseLinks(server, controller.Links))
	// set default status code
	middleware = middleware.Left(middlewareBuiltinDefaultStatusCode(server))
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

func (server *hybsHttpServer) registerGroup() (err error) {
	// register controller groups
	server.middlewareTree = &middlewareTree{}
	for _, controller := range server.controllerConfig {
		var middleware Middleware = nil
		if middleware, err = server.pathMiddleware(controller.Base); err != nil {
			return err
		}
		// insert middleware into tree
		if middleware != nil {
			server.middlewareTree.add([]byte(controller.Location), middleware)
		}
		// append to controller define tree
		var insertController = controller // copy instance
		server.controllerConfigTree.add([]byte(controller.Location), &insertController)
	}
	return
}

func (server *hybsHttpServer) registerAPI() (err error) {
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
			// apply path middleware
			if middlewareWithMethod != nil {
				middlewareWithMethod = middlewareWithPath.Left(middlewareWithMethod)
			} else {
				middlewareWithMethod = middlewareWithPath
			}
			// apply middleware system info
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
				rawFastHttpHandler(server, handler)(ctx, params)
			}
			server.virtualRouters[0].rawRouter.Handle(serviceConfig.Method, controller.Location, virtualHandler1)
			// HTTP/3
			notFoundBytes := []byte("not found")
			virtualHandler3 := func(rw http.ResponseWriter, req *http.Request) {
				if len(hostName) > 0 && !bytes.EqualFold([]byte(req.Host), hostName) {
					rw.WriteHeader(http.StatusNotFound)
					rw.Write(notFoundBytes)
					return
				}
				rawHttp3Handler(server, handler).ServeHTTP(rw, req)
			}
			server.virtualRouters[0].http3Mux.Handle(serviceConfig.Method, controller.Location, virtualHandler3)
		}
	}
	// builtin health check API
	server.virtualRouters[0].rawRouter.GET("/__hybs_health_check__", func(ctx *fasthttp.RequestCtx, params fasthttprouter.Params) {
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	server.virtualRouters[0].http3Mux.Get("/__hybs_health_check__", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	var handleTimeout, compressLevel = server.httpConfig.HandleTimeout, server.httpConfig.CompressLevel
	if server.httpConfig.isHttp1() {
		// HTTP/1.x
		var routerRawHandler1 = server.virtualRouters[0].rawRouter.Handler
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
	} else if server.httpConfig.isHttp3() {
		// HTTP/3
		var routerRawHandler3 = server.virtualRouters[0].http3Mux.Serve()
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

type controllerConfigList struct {
	configItem *controllerConfigItem
	next       *controllerConfigList
	depth      uint16
}

func (list *controllerConfigList) add(configItem *controllerConfigItem) *controllerConfigList {
	parent, current := &controllerConfigList{next: list, depth: list.depth}, list
	for current != nil {
		parent = current
		current = current.next
	}
	current = &controllerConfigList{configItem: configItem, next: nil, depth: list.depth + 1}
	parent.next = current
	return current
}

type controllerConfigTree struct {
	children   [0x80]*controllerConfigTree
	configList *controllerConfigList
}

func (tree *controllerConfigTree) seekPath(path []byte) (node *controllerConfigTree, err error) {
	if path == nil || len(path) < 1 {
		return nil, fmt.Errorf("path cannot be nil or empty")
	}
	node = tree
	for i := 0; i < len(path); i++ {
		if node == nil {
			return nil, fmt.Errorf("already seek to leaf:%s", path[:i])
		}
		if path[i] > 0x7F {
			return nil, fmt.Errorf("character used in path must be 0~07f:%s", path)
		}
		node = node.children[path[i]]
	}
	return node, nil
}

func (tree *controllerConfigTree) add(path []byte, configItem *controllerConfigItem) {
	if path == nil || configItem == nil {
		return
	}
	current := tree
	for i := 0; i < len(path); i++ {
		if path[i] > 0x7F {
			// 不正キャラクターを無視
			continue
		}
		if current.children[path[i]] == nil {
			current.children[path[i]] = &controllerConfigTree{
				children:   [0x80]*controllerConfigTree{nil},
				configList: nil,
			}
		}
		current = current.children[path[i]]
	}
	if current.configList == nil {
		current.configList = &controllerConfigList{configItem: configItem, next: nil, depth: 0}
		return
	}
	current.configList.add(configItem)
}

func (tree *controllerConfigTree) combine(path string) (ret *controllerConfigList) {
	if path == "" || len(path) < 1 {
		return
	}
	var node = tree
	for i := 0; i < len(path); i++ {
		if path[i] > 0x7F {
			return ret
		}
		node = node.children[path[i]]
		if node == nil {
			return ret
		}
		var list = node.configList
		for ; list != nil; list = list.next {
			if list.configItem == nil {
				continue
			}
			if ret != nil {
				ret.add(list.configItem)
				continue
			}
			ret = &controllerConfigList{
				configItem: list.configItem,
				next:       nil,
			}
		}
	}
	return ret
}

// Now returns server side time
var Now = func() Time {
	return Time(time.Now())
}

type httpServer interface {
	serve() (err error)
	setListener(l net.Listener)
	stop() (err error)
	setHandler(routerHandler interface{})
}

type httpServerConfig struct {
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
	ControllerFilepath string        `yaml:"controller_filepath" default:"config/controllers/"`
}

func (c *httpServerConfig) isHttp1() bool {
	if strings.EqualFold(c.HttpVersion, "1.1") {
		return true
	}
	if strings.HasPrefix(c.HttpVersion, "1.") {
		return true
	}
	return false
}
func (c *httpServerConfig) isHttp2() bool {
	return false
}
func (c *httpServerConfig) isHttp3() bool {
	if strings.EqualFold(c.HttpVersion, "3") {
		return true
	}
	if strings.HasPrefix(c.HttpVersion, "3.") {
		return true
	}
	return false
}

// for HTTP/1.1
type fasthttpServer struct {
	*hybsEngine
	*httpServerConfig
	netListener net.Listener
	httpServer  *fasthttp.Server
}

func newFasthttpServer(config *httpServerConfig) (newServer httpServer, err error) {
	// create new fasthttpServer
	newServer = &fasthttpServer{httpServerConfig: config}
	// init netListener
	newServer.(*fasthttpServer).netListener, err = net.Listen(config.Network, config.Address)
	if err != nil {
		return nil, fmt.Errorf("cannot listen at %s:%s", config.Address, err)
	}
	newServer.(*fasthttpServer).httpServer = &fasthttp.Server{
		Name:               "hayabusa",
		Concurrency:        config.Concurrency,
		MaxConnsPerIP:      config.MaxConnectionPerIP,
		ReadTimeout:        config.ReadTimeout,
		WriteTimeout:       config.WriteTimeout,
		MaxRequestBodySize: config.MaxRequestBodySize * 1024,
	}
	return newServer, err
}

func (server *fasthttpServer) serve() (err error) {
	var cfg = server.httpServerConfig
	if !cfg.UseTLS || server.CertFile == "" || server.KeyFile == "" {
		return server.httpServer.Serve(server.netListener)
	}
	return server.httpServer.ServeTLS(server.netListener, server.CertFile, server.KeyFile)
}
func (server *fasthttpServer) setListener(l net.Listener) {
	server.netListener = l
}
func (server *fasthttpServer) stop() (err error) {
	return server.httpServer.Shutdown()
}
func (server *fasthttpServer) setHandler(routerHandler interface{}) {
	server.httpServer.Handler = routerHandler.(fasthttp.RequestHandler)
}

// for HTTP/3
type http3Server struct {
	*hybsEngine
	*httpServerConfig
	http3Server *http3.Server
}

func newHttp3Server(config *httpServerConfig) (newServer httpServer, err error) {
	// create new http3 server from quic-go
	newServer = &http3Server{httpServerConfig: config}
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
	var cfg = server.httpServerConfig
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
