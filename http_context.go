package hybs

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"

	"github.com/labstack/gommon/random"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttprouter"
	"go.mongodb.org/mongo-driver/mongo"
	"gorm.io/gorm"
)

const (
	// ResultCodeSuccess mains success
	ResultCodeSuccess = 0
)

// Ctx is interface of hybsCtx implements
type Ctx interface {
	// Context returns the internal context of this API processing
	Context() context.Context
	// ConstStringValue returns const string parameter value defined in controller
	ConstStringValue(name string) string
	// ConstStringValue returns const int parameter value defined in controller
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
	// StatusInternalServerError sets status code to 503
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
	// Asserts throw a fatal error if ok is not true
	Assert(ok bool, format string, args ...interface{}) Ctx
	// Cache returns reference of cache instance by id
	// cache instance with an id is defined in controllers
	Cache(id ...string) *cache.Cache
	// Redis returns reference of redis client by id
	// redis client with an id is defined in controllers
	Redis(id ...string) RedisClient
	// Mongo returns reference of mongodb client with default database
	// mongodb client with an id is defined in controllers
	Mongo(database ...string) (db *mongo.Database)
	// MySQL returns reference of mysql client with default database
	// mysql client with an id is defined in controllers
	MySQL(database ...string) (db *gorm.DB)
	// Sqlite returns reference of sqlite
	// sqlite with an id is defined in controllers
	Sqlite(database ...string) (db *gorm.DB)
	// MasterTable find and returns master table by table name
	MasterTable(tableName string, mux ...string) MasterTable
	// HasError returns true if no errors occurred and status code is 2xx/3xx
	HasError() bool
	// HttpClient returns the default http client that used for server-server communication
	HttpClient() *HttpClient
	// Now returns the time when request came
	Now() Time

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
type http3Args struct {
	url.Values
}

func (a *http3Args) VisitAll(f func(key, value []byte)) {
	for keyStr, valSlice := range a.Values {
		if len(valSlice) < 1 {
			continue
		}
		f([]byte(keyStr), []byte(valSlice[0]))
	}
}
func (a *http3Args) Len() int {
	return len(a.Values)
}
func (a *http3Args) String() string {
	return a.Values.Encode()
}
func (a *http3Args) Peek(key string) []byte {
	return []byte(a.Values.Get(key))
}
func (a *http3Args) PeekBytes(key []byte) []byte {
	return []byte(a.Values.Get(string(key)))
}

type http3Header struct {
	http.Header
}

func (h *http3Header) Peek(key string) []byte {
	return []byte(h.Header.Get(key))
}
func (h *http3Header) PeekBytes(key []byte) []byte {
	return []byte(h.Header.Get(string(key)))
}
func (h *http3Header) Set(key, value string) {
	h.Header.Set(key, value)
}
func (h *http3Header) SetContentType(content string) {
	h.Header.Set(fasthttp.HeaderContentType, content)
}
func (h *http3Header) SetContentTypeBytes(content []byte) {
	h.Header.Set(fasthttp.HeaderContentType, string(content))
}
func (h *http3Header) RawHeaders() []byte {
	var buffer = &bytes.Buffer{}
	for k, vSlice := range h.Header {
		buffer.WriteString(fmt.Sprintf("%s:", k))
		for _, v := range vSlice {
			buffer.WriteString(fmt.Sprintf(" %s", v))
		}
		buffer.WriteString("\n")
	}
	return buffer.Bytes()
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
	User() User
	RequestHeader() requestHeader
	RequestBody() []byte
	RequestURI() string
	Method() []byte
	MethodString() string
	MethodIdempotent() bool
	Path() []byte
	PathString() string
	// user original param
	SetCtxValue(name string, value interface{})
	CtxValue(name string) interface{}
	CtxString(name string) string
	CtxInt(name string) int
	CtxUint(name string) uint
	CtxInt16(name string) int16
	CtxUint16(name string) uint16
	CtxBool(name string) bool
	// Query Args returns query args behind the URL
	// For example, /v1/sample-resource/?id=5
	QueryArgs() httpArgs
	// Post Args returns form args in body
	PostArgs() httpArgs
	// Route Params returns params of route
	// For example, /v1/sample-items/:id
	RouteParams() httpParams
	FormBytes(name string) []byte
	FormString(name string) string
	FormInt(name string) int
	FormUint(name string) uint
	FormInt16(name string) int16
	FormUint16(name string) uint16
	FormBool(name string) bool
	FormTime(name string) Time
	RouteParamString(name string) string
	RouteParamInt(name string) int
	RouteParamUint(name string) uint
	RouteParamInt16(name string) int16
	RouteParamUint16(name string) uint16
	RouteParamBool(name string) bool
	RouteParamTime(name string) Time
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
type http3Params struct {
	url.Values
}

func (v *http3Params) ByName(name string) string {
	return v.Get(name)
}

type fasthttpCtx struct {
	*fasthttp.RequestCtx
	fasthttprouter.Params
}
type http3Ctx struct {
	*http.Request
	*http.Response
	http.ResponseWriter
	userValue map[string]interface{}
}

type hybsCtx struct {
	httpCtx
	context          context.Context
	sysLogCollector  []interface{}
	gameLogCollector []*gameLog
	sysLoggerEntry   *logrus.Entry
	gameLoggerEntry  *logrus.Entry
	cache            map[string]*cache.Cache
	redis            map[string]RedisClient
	mongodb          map[string]*mongo.Database
	mysql            map[string]*gorm.DB
	sqlite           map[string]*gorm.DB
	response         map[string]interface{}
	handleAt         Time
	timeOffset       time.Duration
}

func hybsCtxCreateNew(server *hybsHttpServer) *hybsCtx {
	return &hybsCtx{
		context:          server.engine.context,
		sysLogCollector:  make([]interface{}, 0, 10),
		gameLogCollector: make([]*gameLog, 0, 4),
		cache:            make(map[string]*cache.Cache),
		redis:            make(map[string]RedisClient),
		mongodb:          make(map[string]*mongo.Database),
		mysql:            make(map[string]*gorm.DB),
		sqlite:           make(map[string]*gorm.DB),
		response:         make(map[string]interface{}),
		timeOffset:       0,
	}
}

// fasthttp implement
func makeFasthttpCtx(server *hybsHttpServer, ctx *fasthttp.RequestCtx, params fasthttprouter.Params) *hybsCtx {
	var newCtx = hybsCtxCreateNew(server)
	newCtx.httpCtx = &fasthttpCtx{
		RequestCtx: ctx,
		Params:     params,
	}
	return newCtx
}
func (c *fasthttpCtx) User() User {
	var ret = c.UserValue("User")
	if ret != nil {
		return ret.(User)
	}
	ret = c.UserValue("UserBase")
	if ret != nil {
		return ret.(User)
	}
	return nil
}
func (c *fasthttpCtx) RequestHeader() requestHeader {
	return &c.RequestCtx.Request.Header
}
func (c *fasthttpCtx) ResponseHeader() responseHeader {
	return &c.RequestCtx.Response.Header
}
func (c *fasthttpCtx) RequestBody() []byte {
	return c.RequestCtx.Request.Body()
}
func (c *fasthttpCtx) RequestURI() string {
	return c.RequestCtx.Request.URI().String()
}
func (c *fasthttpCtx) MethodString() string {
	return string(c.RequestCtx.Method())
}
func (c *fasthttpCtx) MethodIdempotent() bool {
	var head, get, put, del = []byte("HEAD"), []byte("GET"), []byte("PUT"), []byte("DELETE")
	if bytes.Equal(c.Method(), head) {
		return true
	}
	if bytes.Equal(c.Method(), get) {
		return true
	}
	if bytes.Equal(c.Method(), put) {
		return true
	}
	if bytes.Equal(c.Method(), del) {
		return true
	}
	return false
}
func (c *fasthttpCtx) PathString() string {
	return string(c.RequestCtx.Path())
}
func (c *fasthttpCtx) SetCtxValue(name string, value interface{}) {
	c.RequestCtx.SetUserValue(name, value)
}
func (c *fasthttpCtx) CtxValue(name string) interface{} {
	// try get from http context
	if val := c.RequestCtx.UserValue(name); val != nil {
		return val
	}
	// try get from engine user value
	var val, ok = c.UserValue("HttpServer").(*hybsHttpServer).engine.UserValue(name)
	if !ok {
		return nil
	}
	return val
}
func (c *fasthttpCtx) CtxString(name string) string {
	ret, ok := c.CtxValue(name).(string)
	if !ok {
		return ""
	}
	return ret
}
func (c *fasthttpCtx) CtxInt(name string) int {
	ret, ok := c.CtxValue(name).(int)
	if !ok {
		return 0
	}
	return ret
}
func (c *fasthttpCtx) CtxUint(name string) uint {
	ret, ok := c.CtxValue(name).(uint)
	if !ok {
		return 0
	}
	return ret
}
func (c *fasthttpCtx) CtxInt16(name string) int16 {
	ret, ok := c.CtxValue(name).(int16)
	if !ok {
		return 0
	}
	return ret
}
func (c *fasthttpCtx) CtxUint16(name string) uint16 {
	ret, ok := c.CtxValue(name).(uint16)
	if !ok {
		return 0
	}
	return ret
}
func (c *fasthttpCtx) CtxBool(name string) bool {
	ret, ok := c.CtxValue(name).(bool)
	if !ok {
		return false
	}
	return ret
}
func (c *fasthttpCtx) QueryArgs() httpArgs {
	return c.RequestCtx.QueryArgs()
}
func (c *fasthttpCtx) PostArgs() httpArgs {
	return c.RequestCtx.PostArgs()
}
func (c *fasthttpCtx) FormBytes(name string) []byte {
	return c.RequestCtx.FormValue(name)
}
func (c *fasthttpCtx) FormString(name string) string {
	return string(c.RequestCtx.FormValue(name))
}
func (c *fasthttpCtx) FormInt(name string) int {
	var str = c.FormString(name)
	if str == "" {
		return 0
	}
	ret, err := strconv.Atoi(str)
	if err != nil {
		return 0
	}
	return ret
}
func (c *fasthttpCtx) FormUint(name string) uint {
	return uint(c.FormInt(name))
}
func (c *fasthttpCtx) FormInt16(name string) int16 {
	return int16(c.FormInt(name))
}
func (c *fasthttpCtx) FormUint16(name string) uint16 {
	return uint16(c.FormInt(name))
}
func (c *fasthttpCtx) FormBool(name string) bool {
	return strings.EqualFold(c.FormString(name), "true")
}
func (c *fasthttpCtx) FormTime(name string) Time {
	var str = c.FormString(name)
	if str == "" {
		return TimeNil()
	}
	var i64, err = strconv.ParseInt(str, 10, 64)
	if err != nil {
		return TimeNil()
	}
	return TimeUnix(i64)
}
func (c *fasthttpCtx) RouteParams() httpParams {
	return c.Params
}
func (c *fasthttpCtx) RouteParamString(name string) string {
	return c.Params.ByName(name)
}
func (c *fasthttpCtx) RouteParamInt(name string) int {
	var str = c.RouteParamString(name)
	if str == "" {
		return 0
	}
	ret, err := strconv.Atoi(str)
	if err != nil {
		return 0
	}
	return ret
}
func (c *fasthttpCtx) RouteParamUint(name string) uint {
	return uint(c.RouteParamInt(name))
}
func (c *fasthttpCtx) RouteParamInt16(name string) int16 {
	return int16(c.RouteParamInt(name))
}
func (c *fasthttpCtx) RouteParamUint16(name string) uint16 {
	return uint16(c.RouteParamInt(name))
}
func (c *fasthttpCtx) RouteParamBool(name string) bool {
	return strings.EqualFold(c.RouteParamString(name), "true")
}
func (c *fasthttpCtx) RouteParamTime(name string) Time {
	var str = c.RouteParamString(name)
	if str == "" {
		return TimeNil()
	}
	var i64, err = strconv.ParseInt(str, 10, 64)
	if err != nil {
		return TimeNil()
	}
	return TimeUnix(i64)
}
func (c *fasthttpCtx) StatusCode() int {
	return c.Response.Header.StatusCode()
}
func (c *fasthttpCtx) SetResponseBody(body []byte) {
	c.Response.SetBody(body)
}
func (c *fasthttpCtx) RealIP() (ip net.IP) {
	var xRealIP = c.Request.Header.Peek("X-Real-IP")
	for {
		if xRealIP == nil || len(xRealIP) < 1 {
			break
		}
		if ip = net.ParseIP(string(xRealIP)); ip == nil {
			break
		}
		return ip
	}
	return c.RequestCtx.RemoteIP()
}

// http3 implement
func makeHttp3Ctx(server *hybsHttpServer, rw http.ResponseWriter, req *http.Request) *hybsCtx {
	var newCtx = hybsCtxCreateNew(server)
	newCtx.httpCtx = &http3Ctx{
		Request:        req,
		Response:       req.Response,
		ResponseWriter: rw,
		userValue:      make(map[string]interface{}),
	}
	return newCtx
}
func (c *http3Ctx) ID() uint64 {
	reqID, ok := c.userValue["RequestID"]
	if !ok {
		return 0
	}
	return reqID.(uint64)
}
func (c *http3Ctx) User() User {
	val, ok := c.userValue["User"]
	if ok && val != nil {
		return val.(User)
	}
	val, ok = c.userValue["UserBase"]
	if ok && val != nil {
		return val.(User)
	}
	return nil
}
func (c *http3Ctx) RequestHeader() requestHeader {
	return &http3Header{c.Request.Header}
}
func (c *http3Ctx) ResponseHeader() responseHeader {
	return &http3Header{c.Request.Response.Header}
}
func (c *http3Ctx) RequestBody() []byte {
	buffer := &bytes.Buffer{}
	_, err := buffer.ReadFrom(c.Request.Body)
	if err != nil {
		return []byte{}
	}
	return buffer.Bytes()
}
func (c *http3Ctx) RequestURI() string {
	return c.Request.RequestURI
}
func (c *http3Ctx) Method() []byte {
	return []byte(c.Request.Method)
}
func (c *http3Ctx) MethodString() string {
	return c.Request.Method
}
func (c *http3Ctx) MethodIdempotent() bool {
	var head, get, put, del = []byte("HEAD"), []byte("GET"), []byte("PUT"), []byte("DELETE")
	if bytes.Equal(c.Method(), head) {
		return true
	}
	if bytes.Equal(c.Method(), get) {
		return true
	}
	if bytes.Equal(c.Method(), put) {
		return true
	}
	if bytes.Equal(c.Method(), del) {
		return true
	}
	return false
}
func (c *http3Ctx) Path() []byte {
	return []byte(c.URL.Path)
}
func (c *http3Ctx) PathString() string {
	return c.URL.Path
}
func (c *http3Ctx) SetCtxValue(name string, value interface{}) {
	c.userValue[name] = value
}
func (c *http3Ctx) CtxValue(name string) interface{} {
	// try get from http context
	if val, ok := c.userValue[name]; ok {
		return val
	}
	// try get from engine user value
	if val, ok := c.userValue["HttpServer"].(*hybsHttpServer).engine.UserValue(name); ok {
		return val
	}
	return nil
}
func (c *http3Ctx) CtxString(name string) string {
	ret, ok := c.CtxValue(name).(string)
	if !ok {
		return ""
	}
	return ret
}
func (c *http3Ctx) CtxInt(name string) int {
	ret, ok := c.CtxValue(name).(int)
	if !ok {
		return 0
	}
	return ret
}
func (c *http3Ctx) CtxUint(name string) uint {
	ret, ok := c.CtxValue(name).(uint)
	if !ok {
		return 0
	}
	return ret
}
func (c *http3Ctx) CtxInt16(name string) int16 {
	ret, ok := c.CtxValue(name).(int16)
	if !ok {
		return 0
	}
	return ret
}
func (c *http3Ctx) CtxUint16(name string) uint16 {
	ret, ok := c.CtxValue(name).(uint16)
	if !ok {
		return 0
	}
	return ret
}
func (c *http3Ctx) CtxBool(name string) bool {
	ret, ok := c.CtxValue(name).(bool)
	if !ok {
		return false
	}
	return ret
}
func (c *http3Ctx) QueryArgs() httpArgs {
	return &http3Args{c.URL.Query()}
}
func (c *http3Ctx) PostArgs() httpArgs {
	return &http3Args{c.PostForm}
}
func (c *http3Ctx) FormBytes(name string) []byte {
	return []byte(c.Form.Get(name))
}
func (c *http3Ctx) FormString(name string) string {
	return c.Form.Get(name)
}
func (c *http3Ctx) FormInt(name string) int {
	var str = c.FormString(name)
	if str == "" {
		return 0
	}
	ret, err := strconv.Atoi(str)
	if err != nil {
		return 0
	}
	return ret
}
func (c *http3Ctx) FormUint(name string) uint {
	return uint(c.FormInt(name))
}
func (c *http3Ctx) FormInt16(name string) int16 {
	return int16(c.FormInt(name))
}
func (c *http3Ctx) FormUint16(name string) uint16 {
	return uint16(c.FormInt(name))
}
func (c *http3Ctx) FormBool(name string) bool {
	return strings.EqualFold(c.FormString(name), "true")
}
func (c *http3Ctx) FormTime(name string) Time {
	var str = c.FormString(name)
	if str == "" {
		return TimeNil()
	}
	var i64, err = strconv.ParseInt(str, 10, 64)
	if err != nil {
		return TimeNil()
	}
	return TimeUnix(i64)
}
func (c *http3Ctx) RouteParams() httpParams {
	return &http3Params{c.Request.URL.Query()}
}
func (c *http3Ctx) RouteParamString(name string) string {
	return c.Request.URL.Query().Get(name)
}
func (c *http3Ctx) RouteParamInt(name string) int {
	var str = c.RouteParamString(name)
	if str == "" {
		return 0
	}
	ret, err := strconv.Atoi(str)
	if err != nil {
		return 0
	}
	return ret
}
func (c *http3Ctx) RouteParamUint(name string) uint {
	return uint(c.RouteParamInt(name))
}
func (c *http3Ctx) RouteParamInt16(name string) int16 {
	return int16(c.RouteParamInt(name))
}
func (c *http3Ctx) RouteParamUint16(name string) uint16 {
	return uint16(c.RouteParamInt(name))
}
func (c *http3Ctx) RouteParamBool(name string) bool {
	return strings.EqualFold(c.RouteParamString(name), "true")
}
func (c *http3Ctx) RouteParamTime(name string) Time {
	var str = c.RouteParamString(name)
	if str == "" {
		return TimeNil()
	}
	var i64, err = strconv.ParseInt(str, 10, 64)
	if err != nil {
		return TimeNil()
	}
	return TimeUnix(i64)
}
func (c *http3Ctx) StatusCode() int {
	return c.Response.StatusCode
}
func (c *http3Ctx) SetContentType(val string) {
	c.ResponseWriter.Header().Set("Content-Type", val)
}
func (c *http3Ctx) SetContentTypeBytes(val []byte) {
	c.ResponseWriter.Header().Set("Content-Type", string(val))
}
func (c *http3Ctx) SetStatusCode(code int) {
	c.ResponseWriter.WriteHeader(code)
}
func (c *http3Ctx) SetResponseBody(body []byte) {
	c.ResponseWriter.Write(body)
}
func (c *http3Ctx) RemoteIP() net.IP {
	return net.ParseIP(c.Request.RemoteAddr)
}
func (c *http3Ctx) RealIP() (ip net.IP) {
	var xRealIP = c.Request.Header.Get("X-Real-IP")
	for {
		if xRealIP == "" {
			break
		}
		if ip = net.ParseIP(xRealIP); ip == nil {
			break
		}
		return ip
	}
	return net.ParseIP(c.Request.RemoteAddr)
}
func (c *http3Ctx) SetConnectionClose() {
	c.Request.Close = true
}

// application layer context
func (c *hybsCtx) Context() context.Context {
	return c.context
}
func (c *hybsCtx) Deadline() (deadline time.Time, ok bool) {
	return c.context.Deadline()
}
func (c *hybsCtx) Done() <-chan struct{} {
	return c.context.Done()
}
func (c *hybsCtx) Err() error {
	return c.context.Err()
}
func (c *hybsCtx) Value(key interface{}) interface{} {
	return c.context.Value(key)
}
func (c *hybsCtx) ConstStringValue(name string) string {
	return c.CtxString(fmt.Sprintf("__CONST_%s", name))
}
func (c *hybsCtx) ConstIntValue(name string) int {
	return c.CtxInt(fmt.Sprintf("__CONST_%s", name))
}
func (c *hybsCtx) StatusOk() Ctx {
	c.SetStatusCode(fasthttp.StatusOK)
	return c
}
func (c *hybsCtx) StatusAccepted() Ctx {
	c.SetStatusCode(fasthttp.StatusAccepted)
	return c
}
func (c *hybsCtx) StatusNonAuthoritativeInfo() Ctx {
	c.SetStatusCode(fasthttp.StatusNonAuthoritativeInfo)
	return c
}
func (c *hybsCtx) StatusCreated(location string) Ctx {
	c.SetStatusCode(fasthttp.StatusCreated)
	c.ResponseHeader().Set("Location", location)
	return c
}
func (c *hybsCtx) StatusNoContent() Ctx {
	c.SetStatusCode(fasthttp.StatusNoContent)
	return c
}
func (c *hybsCtx) Error(statusCode int, msg ...interface{}) Ctx {
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
func (c *hybsCtx) StatusBadRequest(param ...interface{}) Ctx {
	if len(param) < 1 {
		c.Error(fasthttp.StatusBadRequest)
		return c
	}
	c.Error(fasthttp.StatusBadRequest, fmt.Sprintf("Bad Param:%s", param[0]))
	return c
}
func (c *hybsCtx) StatusUnauthorized() Ctx {
	return c.Error(fasthttp.StatusUnauthorized)
}
func (c *hybsCtx) StatusForbidden(msg ...interface{}) Ctx {
	return c.Error(fasthttp.StatusForbidden, msg...)
}
func (c *hybsCtx) StatusNotFound() Ctx {
	c.SetResponseValue("errorQueryArgs", c.QueryArgs().String())
	c.SetResponseValue("errorRequestURI", c.RequestURI())
	return c.Error(fasthttp.StatusNotFound)
}
func (c *hybsCtx) StatusInternalServerError(msg ...interface{}) Ctx {
	return c.Error(fasthttp.StatusInternalServerError, msg...)
}
func (c *hybsCtx) StatusServiceUnavailable(msg ...interface{}) Ctx {
	return c.Error(fasthttp.StatusServiceUnavailable, msg...)
}

func (c *hybsCtx) SysLogf(format string, args ...interface{}) Ctx {
	c.sysLogCollector = append(c.sysLogCollector, fmt.Sprintf(format, args...))
	return c
}
func (c *hybsCtx) GameLog(action string, fields map[string]interface{}, args ...interface{}) Ctx {
	var comment = "ok"
	if len(args) > 0 && args[0] != nil {
		format, ok := args[0].(string)
		if ok {
			comment = fmt.Sprintf(format, args[1:])
		}
	}
	c.SysLogf("[game-log] action=%s comment=%s", action, comment)
	c.gameLogCollector = append(c.gameLogCollector, &gameLog{
		action:   action,
		fieldMap: fields,
		comment:  comment,
	})
	return c
}
func (c *hybsCtx) Assert(ok bool, format string, args ...interface{}) Ctx {
	if !ok {
		c.CtxValue("HttpServer").(*hybsHttpServer).engine.sysLogger.Fatalf(format, args...)
		c.SetConnectionClose()
	}
	return c
}
func (c *hybsCtx) SetResponseValue(key string, value interface{}) {
	c.response[key] = value
}
func (c *hybsCtx) Cache(cacheID ...string) *cache.Cache {
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
		server := c.CtxValue("HttpServer").(*hybsHttpServer)
		server.engine.sysLogger.Errorf("cache %s not exists", id)
		c.StatusInternalServerError()
	}
	return cache
}
func (c *hybsCtx) Redis(instanceID ...string) RedisClient {
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
		server := c.CtxValue("HttpServer").(*hybsHttpServer)
		server.engine.sysLogger.Errorf("redis %s not exists", id)
		c.StatusInternalServerError()
	}
	return r
}
func (c *hybsCtx) Mongo(database ...string) *mongo.Database {
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
		server := c.CtxValue("HttpServer").(*hybsHttpServer)
		server.engine.sysLogger.Errorf("mongo db %s not exists", dbID)
		c.StatusInternalServerError()
		return nil
	}
	return db
}
func (c *hybsCtx) MySQL(database ...string) *gorm.DB {
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
		server := c.CtxValue("HttpServer").(*hybsHttpServer)
		server.engine.sysLogger.Errorf("mysql %s not exists", dbID)
		c.StatusInternalServerError()
	}
	return db
}
func (c *hybsCtx) Sqlite(database ...string) *gorm.DB {
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
		server := c.CtxValue("HttpServer").(*hybsHttpServer)
		server.engine.sysLogger.Errorf("sqlite %s not exists", dbID)
		c.StatusInternalServerError()
	}
	return db
}
func (c *hybsCtx) MasterTable(tableName string, args ...string) MasterTable {
	var mux string
	if len(args) > 0 {
		mux = args[0]
	} else {
		mux = c.ConstStringValue("csv_mux")
	}
	var (
		svr = c.httpCtx.CtxValue("HttpServer")
		val *sync.Map
		ok  bool
	)
	if svr != nil {
		val, ok = svr.(*hybsHttpServer).engine.masterDataMgr.loadTable(tableName, mux)
		if ok {
			if svr.(*hybsHttpServer).engine.debugMode() {
				c.SysLogf("loaded master table:%s", tableName)
			}
		}
	}
	return &masterDataTable{
		l:         c,
		mux:       mux,
		tableName: tableName,
		container: val,
	}
}
func (c *hybsCtx) HasError() bool {
	return !c.isSuccess()
}
func (c *hybsCtx) Now() Time {
	if c.handleAt.IsZero() {
		return Now().Add(c.timeOffset)
	}
	return c.handleAt.Add(c.timeOffset)
}
func (c *hybsCtx) HttpClient() *HttpClient {
	return DefaultHttpClient
}

func (c *hybsCtx) isDebug() bool {
	return c.CtxValue("HttpServer").(*hybsHttpServer).engine.debugMode()
}
func (c *hybsCtx) isSuccess() bool {
	return c.httpCtx.StatusCode() >= 200 && c.httpCtx.StatusCode() < 400
}
func (c *hybsCtx) statusCodeType() int {
	return c.httpCtx.StatusCode() / 100
}
func (c *hybsCtx) logf(format string, args ...interface{}) {
	c.SysLogf(format, args...)
}

// ServiceHandler does process http api business logic
type ServiceHandler func(ctx Ctx)

func rawFastHttpHandler(server *hybsHttpServer, f ServiceHandler) fasthttprouter.Handle {
	return func(reqCtx *fasthttp.RequestCtx, params fasthttprouter.Params) {
		handleAt := server.engine.Now()
		ctx := makeFasthttpCtx(server, reqCtx, params)
		ctx.handleAt = handleAt
		ctx.sysLoggerEntry = server.engine.sysLogger.(*logrus.Logger).WithTime(handleAt.Raw())
		ctx.gameLoggerEntry = server.engine.gameLogger.(*logrus.Logger).WithTime(handleAt.Raw())
		// default context values
		ctx.SetCtxValue("HttpServer", server)
		ctx.SetCtxValue("HybsEngine", server.engine)
		ctx.SetCtxValue("RequestID", reqCtx.ID())
		server.apiLock.RLock()
		defer server.apiLock.RUnlock()
		f(ctx)
	}
}

type http3HandlerImpl struct {
	server *hybsHttpServer
	f      ServiceHandler
}

func (h *http3HandlerImpl) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	handleAt := h.server.engine.Now()
	ctx := makeHttp3Ctx(h.server, rw, req)
	ctx.handleAt = handleAt
	ctx.sysLoggerEntry = h.server.engine.sysLogger.(*logrus.Logger).WithTime(handleAt.Raw())
	ctx.gameLoggerEntry = h.server.engine.gameLogger.(*logrus.Logger).WithTime(handleAt.Raw())
	// default context values
	ctx.SetCtxValue("HttpServer", h.server)
	ctx.SetCtxValue("HybsEngine", h.server.engine)
	ctx.SetCtxValue("RequestID", uint64(handleAt.Unix()))
	h.server.apiLock.RLock()
	defer h.server.apiLock.RUnlock()
	h.f(ctx)
}
func rawHttp3Handler(svr *hybsHttpServer, f ServiceHandler) http.Handler {
	return &http3HandlerImpl{server: svr, f: f}
}

// User is interface of UserBase implements
type User interface {
	ID() string
	HasPermission(perm uint8) bool
}

const (
	// UserPermissionGuest is user permission level of guest
	UserPermissionGuest = 0
	// UserPermissionNormal is user permission level of normal
	UserPermissionNormal = 1
)

// UserBase defines basic fields of users
type UserBase struct {
	UserID      string `json:"userId"      bson:"user_id"`
	password    string `json:"-"    bson:"password"`
	AccessToken string `json:"accessToken" bson:"access_token"`
	Permission  uint8  `json:"permission"  bson:"permission"`
}

const userIDCharset = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvexyz0123456789"

// NewUser makes new UserBase
func NewUser() *UserBase {
	u := &UserBase{
		UserID:     random.String(12, userIDCharset),
		Permission: UserPermissionGuest,
	}
	u.randomize()
	return u
}

// NewUserFromAccessToken makes new UserBase from given AccessToken
func newUserFromAccessToken(token string) (u *UserBase) {
	bytes, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil
	}
	decodedToken := string(bytes)
	args := strings.Split(decodedToken, "#")
	if len(args) != 2 {
		return nil
	}
	u = &UserBase{
		UserID:      args[0],
		password:    args[1],
		AccessToken: token,
		Permission:  UserPermissionGuest,
	}
	return u
}

func (u *UserBase) randomize() {
	u.password = random.String(53, userIDCharset)
	token := fmt.Sprintf("%s#%s", u.UserID, u.password)
	u.AccessToken = base64.StdEncoding.EncodeToString([]byte(token))
}

// ID returns user ID
func (u *UserBase) ID() string {
	return u.UserID
}

// HasPermission returns true if user have specified permission level
func (u *UserBase) HasPermission(needPerm uint8) bool {
	return u.Permission >= needPerm
}
