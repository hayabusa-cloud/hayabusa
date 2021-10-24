package web

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hayabusa-cloud/hayabusa/plugins"

	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

// Ctx is interface of ctx implements
type Ctx interface {
	// Context returns the internal context of this API processing
	Context() context.Context
	// ConstStringValue returns const string parameter value defined in controller
	ConstStringValue(name string) string
	// ConstIntValue returns const int parameter value defined in controller
	ConstIntValue(name string) int
	// StatusOk sets status code to 200
	StatusOk() Ctx
	// StatusCreated sets status code to 201
	StatusCreated(location string) Ctx
	// StatusAccepted sets status code to 202
	StatusAccepted() Ctx
	// StatusNonAuthoritativeInfo  sets status code to 203
	StatusNonAuthoritativeInfo() Ctx
	// StatusNoContent sets status code to 204
	StatusNoContent() Ctx
	// Error sets status code with error message (do not set 2xx/3xx)
	Error(statusCode int, msg ...interface{}) Ctx
	// StatusBadRequest sets status code to 400
	StatusBadRequest(param ...interface{}) Ctx
	// StatusUnauthorized sets status code to 401
	StatusUnauthorized() Ctx
	// StatusForbidden sets status code to 403
	StatusForbidden(msg ...interface{}) Ctx
	// StatusNotFound sets status code to 404
	StatusNotFound() Ctx
	// StatusInternalServerError sets status code to 500
	StatusInternalServerError(msg ...interface{}) Ctx
	// StatusServiceUnavailable sets status code to 503
	StatusServiceUnavailable(msg ...interface{}) Ctx
	// SetResponseValue sets/adds a pair of key-value append to response body
	SetResponseValue(key string, value interface{})
	// SysLogf writes system log
	// system logs are used for technicians to investigate warns and errors
	// no need to consider log level because log level will be automatically set
	// if no errors occurred and status code is set to 2xx/3xx, log level is info
	// if no errors occurred and status code is set to 4xx, log level is warn
	// if any errors occurred or status code is set to 5xx, log level is error
	SysLogf(format string, args ...interface{}) Ctx
	// GameLog writes game log
	// game logs are used to track user behavior history
	// game logs are for KPI analysis, customer service, etc.
	GameLog(action string, fields map[string]interface{}, args ...interface{}) Ctx
	// Assert throw a fatal error if the value of ok is not true
	Assert(ok bool, format string, args ...interface{}) Ctx
	// Cache returns reference of cache instance by id
	// cache instance with an id is defined in controllers
	Cache(id ...string) (plugin *plugins.Cache)
	// Redis returns reference of redis client by id
	// redis client with an id is defined in controllers
	Redis(id ...string) (plugin *plugins.Redis)
	// Mongo returns reference of mongodb client with default database
	// mongodb client with an id is defined in controllers
	Mongo(database ...string) (plugin *plugins.Mongo)
	// MySQL returns reference of mysql client with default database
	// mysql client with an id is defined in controllers
	MySQL(database ...string) (plugin *plugins.MySQL)
	// Sqlite returns reference of sqlite
	// sqlite with an id is defined in controllers
	Sqlite(database ...string) (plugin *plugins.Sqlite)
	// MasterTable find and returns master table by table name
	MasterTable(tableName string, mux ...string) plugins.MasterTable
	// HasError returns true if no errors occurred and status code is 2xx/3xx
	HasError() bool
	// HttpClient returns the default http client that used for server-server communication
	HttpClient() *Client
	// Now returns the time when request came
	Now() time.Time

	context.Context
	httpCtx
}

type httpArgs interface {
	VisitAll(f func(key, value []byte))
	Len() int
	String() string
	Peek(key string) []byte
	PeekBytes(key []byte) []byte
}

type requestHeader interface {
	Peek(key string) []byte
	PeekBytes(key []byte) []byte
	Set(key, value string)
	RawHeaders() []byte
}
type responseHeader interface {
	Peek(key string) []byte
	PeekBytes(key []byte) []byte
	Set(key, value string)
}

type httpCtx interface {
	ID() uint64
	Authentication() *Authentication
	RequestHeader() requestHeader
	RequestBody() []byte
	RequestURI() string
	Method() []byte
	MethodString() string
	MethodIdempotent() bool
	Path() []byte
	PathString() string
	SetCtxValue(name string, value interface{})
	CtxValue(name string) interface{}
	CtxString(name string) string
	CtxInt(name string) int
	CtxUint(name string) uint
	CtxInt16(name string) int16
	CtxUint16(name string) uint16
	CtxBool(name string) bool
	// QueryArgs returns query args behind the URL
	// For example, /v1/sample-resource/?id=5
	QueryArgs() httpArgs
	// PostArgs returns form args in body
	PostArgs() httpArgs
	// RouteParams returns params of route
	// For example, /v1/sample-items/:id
	RouteParams() httpParams
	FormBytes(name string) []byte
	FormString(name string) string
	FormInt(name string) int
	FormUint(name string) uint
	FormInt16(name string) int16
	FormUint16(name string) uint16
	FormBool(name string) bool
	FormTime(name string) time.Time
	RouteParamString(name string) string
	RouteParamInt(name string) int
	RouteParamUint(name string) uint
	RouteParamInt16(name string) int16
	RouteParamUint16(name string) uint16
	RouteParamBool(name string) bool
	RouteParamTime(name string) time.Time
	ResponseHeader() responseHeader
	StatusCode() int
	SetStatusCode(code int)
	SetContentType(content string)
	SetContentTypeBytes(content []byte)
	SetResponseBody(body []byte)
	RemoteIP() net.IP
	RealIP() net.IP
	SetConnectionClose()
}

type httpParams interface {
	ByName(string) string
}

type ctx struct {
	httpCtx
	context          context.Context
	sysLogCollector  []interface{}
	gameLogCollector []*plugins.GameLog
	sysLoggerEntry   *logrus.Entry
	gameLoggerEntry  *logrus.Entry
	cache            map[string]*plugins.Cache
	redis            map[string]*plugins.Redis
	mongodb          map[string]*plugins.Mongo
	mysql            map[string]*plugins.MySQL
	sqlite           map[string]*plugins.Sqlite
	response         map[string]interface{}
	handleAt         time.Time
	timeOffset       time.Duration
}

func createNewCtx(server *Server) *ctx {
	return &ctx{
		context:          server.engine.Context(),
		sysLogCollector:  make([]interface{}, 0, 16),
		gameLogCollector: make([]*plugins.GameLog, 0, 8),
		cache:            make(map[string]*plugins.Cache),
		redis:            make(map[string]*plugins.Redis),
		mongodb:          make(map[string]*plugins.Mongo),
		mysql:            make(map[string]*plugins.MySQL),
		sqlite:           make(map[string]*plugins.Sqlite),
		response:         make(map[string]interface{}),
		timeOffset:       0,
	}
}

func (c *ctx) Context() context.Context {
	return c.context
}
func (c *ctx) Deadline() (deadline time.Time, ok bool) {
	return c.context.Deadline()
}
func (c *ctx) Done() <-chan struct{} {
	return c.context.Done()
}
func (c *ctx) Err() error {
	return c.context.Err()
}
func (c *ctx) Value(key interface{}) interface{} {
	return c.context.Value(key)
}
func (c *ctx) ConstStringValue(name string) string {
	return c.CtxString(fmt.Sprintf("__CONST_%s", name))
}
func (c *ctx) ConstIntValue(name string) int {
	return c.CtxInt(fmt.Sprintf("__CONST_%s", name))
}
func (c *ctx) StatusOk() Ctx {
	c.SetStatusCode(fasthttp.StatusOK)
	return c
}
func (c *ctx) StatusAccepted() Ctx {
	c.SetStatusCode(fasthttp.StatusAccepted)
	return c
}
func (c *ctx) StatusNonAuthoritativeInfo() Ctx {
	c.SetStatusCode(fasthttp.StatusNonAuthoritativeInfo)
	return c
}
func (c *ctx) StatusCreated(location string) Ctx {
	c.SetStatusCode(fasthttp.StatusCreated)
	c.ResponseHeader().Set("Location", location)
	return c
}
func (c *ctx) StatusNoContent() Ctx {
	c.SetStatusCode(fasthttp.StatusNoContent)
	return c
}
func (c *ctx) Error(statusCode int, msg ...interface{}) Ctx {
	c.httpCtx.SetStatusCode(statusCode)
	if len(msg) > 0 {
		c.SetResponseValue("error", msg[0])
		c.sysLogCollector = append(c.sysLogCollector, fmt.Sprintf("set status code:%d msg:%s", statusCode, msg[0]))
	} else {
		c.SetResponseValue("error", fasthttp.StatusMessage(statusCode))
		c.sysLogCollector = append(c.sysLogCollector, fmt.Sprintf("set Status code:%d", statusCode))
	}
	return c
}
func (c *ctx) StatusBadRequest(param ...interface{}) Ctx {
	if len(param) < 1 {
		c.Error(fasthttp.StatusBadRequest)
		return c
	}
	c.Error(fasthttp.StatusBadRequest, fmt.Sprintf("Bad Param:%s", param[0]))
	return c
}
func (c *ctx) StatusUnauthorized() Ctx {
	return c.Error(fasthttp.StatusUnauthorized)
}
func (c *ctx) StatusForbidden(msg ...interface{}) Ctx {
	return c.Error(fasthttp.StatusForbidden, msg...)
}
func (c *ctx) StatusNotFound() Ctx {
	c.SetResponseValue("errorQueryArgs", c.QueryArgs().String())
	c.SetResponseValue("errorRequestURI", c.RequestURI())
	return c.Error(fasthttp.StatusNotFound)
}
func (c *ctx) StatusInternalServerError(msg ...interface{}) Ctx {
	return c.Error(fasthttp.StatusInternalServerError, msg...)
}
func (c *ctx) StatusServiceUnavailable(msg ...interface{}) Ctx {
	return c.Error(fasthttp.StatusServiceUnavailable, msg...)
}

func (c *ctx) SysLogf(format string, args ...interface{}) Ctx {
	c.sysLogCollector = append(c.sysLogCollector, fmt.Sprintf(format, args...))
	return c
}
func (c *ctx) GameLog(action string, fields map[string]interface{}, args ...interface{}) Ctx {
	var comment = "ok"
	if len(args) > 0 && args[0] != nil {
		format, ok := args[0].(string)
		if ok {
			comment = fmt.Sprintf(format, args[1:])
		}
	}
	c.SysLogf("[game-log] action=%s comment=%s", action, comment)
	c.gameLogCollector = append(c.gameLogCollector, &plugins.GameLog{
		Action:  action,
		Fields:  fields,
		Comment: comment,
	})
	return c
}
func (c *ctx) Assert(ok bool, format string, args ...interface{}) Ctx {
	if !ok {
		c.CtxValue("Server").(*Server).sysLogger.Fatalf(format, args...)
		c.SetConnectionClose()
	}
	return c
}
func (c *ctx) SetResponseValue(key string, value interface{}) {
	c.response[key] = value
}
func (c *ctx) Cache(cacheID ...string) *plugins.Cache {
	// take default reference
	if len(c.cache) == 1 {
		for _, v := range c.cache {
			return v
		}
	}
	// take reference by id
	var id string
	if len(cacheID) > 0 {
		id = cacheID[0]
	} else {
		id = c.ConstStringValue("cache_id")
	}
	cache, ok := c.cache[id]
	if !ok {
		server := c.CtxValue("Server").(*Server)
		server.sysLogger.Errorf("cache %s not exists", id)
		c.StatusInternalServerError()
	}
	return cache
}
func (c *ctx) Redis(instanceID ...string) *plugins.Redis {
	if len(c.redis) == 1 {
		for _, v := range c.redis {
			return v
		}
	}
	// take reference by id
	var id string
	if len(instanceID) > 0 {
		id = instanceID[0]
	} else {
		id = c.ConstStringValue("redis_id")
	}
	r, ok := c.redis[id]
	if !ok {
		server := c.CtxValue("Server").(*Server)
		server.sysLogger.Errorf("redis %s not exists", id)
		c.StatusInternalServerError()
	}
	return r
}
func (c *ctx) Mongo(database ...string) *plugins.Mongo {
	// take default reference
	if len(c.mongodb) == 1 {
		for _, v := range c.mongodb {
			return v
		}
	}
	// take reference by id
	var dbID string
	if len(database) > 0 {
		dbID = database[0]
	} else {
		dbID = c.ConstStringValue("mongo_id")
	}
	var db, ok = c.mongodb[dbID]
	if !ok {
		server := c.CtxValue("Server").(*Server)
		server.sysLogger.Errorf("mongo db %s not exists", dbID)
		c.StatusInternalServerError()
		return nil
	}
	return db
}
func (c *ctx) MySQL(database ...string) *plugins.MySQL {
	// take default reference
	if len(c.mysql) == 1 {
		for _, v := range c.mysql {
			return v
		}
	}
	// take reference by id
	var dbID string
	if len(database) > 0 {
		dbID = database[0]
	} else {
		dbID = c.ConstStringValue("mysql_id")
	}
	db, ok := c.mysql[dbID]
	if !ok {
		server := c.CtxValue("Server").(*Server)
		server.sysLogger.Errorf("mysql %s not exists", dbID)
		c.StatusInternalServerError()
	}
	return db
}
func (c *ctx) Sqlite(database ...string) *plugins.Sqlite {
	// take default reference
	if len(c.sqlite) == 1 {
		for _, v := range c.sqlite {
			return v
		}
	}
	// take reference by id
	var dbID string
	if len(database) > 0 {
		dbID = database[0]
	} else {
		dbID = c.ConstStringValue("sqlite_id")
	}
	db, ok := c.sqlite[dbID]
	if !ok {
		server := c.CtxValue("Server").(*Server)
		server.sysLogger.Errorf("sqlite %s not exists", dbID)
		c.StatusInternalServerError()
	}
	return db
}
func (c *ctx) MasterTable(tableName string, args ...string) plugins.MasterTable {
	var mux string
	if len(args) > 0 {
		mux = args[0]
	} else {
		mux = c.ConstStringValue("csv_mux")
	}
	var (
		svr = c.httpCtx.CtxValue("Server")
		val *sync.Map
		ok  bool
	)
	if svr != nil {
		val, ok = svr.(*Server).engine.MasterData().Table(tableName, mux)
		if ok {
			if svr.(*Server).engine.DebugMode() {
				c.SysLogf("loaded master table:%s", tableName)
			}
		}
	}
	return plugins.MasterTableWithLogger(c.sysLoggerEntry, mux, tableName, val)
}
func (c *ctx) HasError() bool {
	return !c.isSuccess()
}
func (c *ctx) Now() time.Time {
	if c.handleAt.IsZero() {
		return c.Now().Add(c.timeOffset)
	}
	return c.handleAt.Add(c.timeOffset)
}
func (c *ctx) HttpClient() *Client {
	return DefaultHttpClient
}

func (c *ctx) DebugMode() bool {
	return c.CtxValue("Server").(*Server).engine.DebugMode()
}
func (c *ctx) isSuccess() bool {
	return c.httpCtx.StatusCode() >= 200 && c.httpCtx.StatusCode() < 400
}
func (c *ctx) statusCodeType() int {
	return c.httpCtx.StatusCode() / 100
}
