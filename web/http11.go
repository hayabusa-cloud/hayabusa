package web

import (
	"bytes"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttprouter"
)

type http11Ctx struct {
	*fasthttp.RequestCtx
	fasthttprouter.Params
}

func createHttp11Ctx(server *Server, ctx *fasthttp.RequestCtx, params fasthttprouter.Params) *ctx {
	var newCtx = createNewCtx(server)
	newCtx.httpCtx = &http11Ctx{
		RequestCtx: ctx,
		Params:     params,
	}
	return newCtx
}
func (c *http11Ctx) Authentication() *Authentication {
	var ret = c.UserValue("Authentication")
	if ret != nil {
		return ret.(*Authentication)
	}
	return nil
}
func (c *http11Ctx) RequestHeader() requestHeader {
	return &c.RequestCtx.Request.Header
}
func (c *http11Ctx) ResponseHeader() responseHeader {
	return &c.RequestCtx.Response.Header
}
func (c *http11Ctx) RequestBody() []byte {
	return c.RequestCtx.Request.Body()
}
func (c *http11Ctx) RequestURI() string {
	return c.RequestCtx.Request.URI().String()
}
func (c *http11Ctx) MethodString() string {
	return string(c.RequestCtx.Method())
}
func (c *http11Ctx) MethodIdempotent() bool {
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
func (c *http11Ctx) PathString() string {
	return string(c.RequestCtx.Path())
}
func (c *http11Ctx) SetCtxValue(name string, value interface{}) {
	c.RequestCtx.SetUserValue(name, value)
}
func (c *http11Ctx) CtxValue(name string) interface{} {
	// try get from http context
	if val := c.RequestCtx.UserValue(name); val != nil {
		return val
	}
	// try get from engine user value
	var val, ok = c.UserValue("Server").(*Server).engine.UserValue(name)
	if !ok {
		return nil
	}
	return val
}
func (c *http11Ctx) CtxString(name string) string {
	ret, ok := c.CtxValue(name).(string)
	if !ok {
		return ""
	}
	return ret
}
func (c *http11Ctx) CtxInt(name string) int {
	ret, ok := c.CtxValue(name).(int)
	if !ok {
		return 0
	}
	return ret
}
func (c *http11Ctx) CtxUint(name string) uint {
	ret, ok := c.CtxValue(name).(uint)
	if !ok {
		return 0
	}
	return ret
}
func (c *http11Ctx) CtxInt16(name string) int16 {
	ret, ok := c.CtxValue(name).(int16)
	if !ok {
		return 0
	}
	return ret
}
func (c *http11Ctx) CtxUint16(name string) uint16 {
	ret, ok := c.CtxValue(name).(uint16)
	if !ok {
		return 0
	}
	return ret
}
func (c *http11Ctx) CtxBool(name string) bool {
	ret, ok := c.CtxValue(name).(bool)
	if !ok {
		return false
	}
	return ret
}
func (c *http11Ctx) QueryArgs() httpArgs {
	return c.RequestCtx.QueryArgs()
}
func (c *http11Ctx) PostArgs() httpArgs {
	return c.RequestCtx.PostArgs()
}
func (c *http11Ctx) FormBytes(name string) []byte {
	return c.RequestCtx.FormValue(name)
}
func (c *http11Ctx) FormString(name string) string {
	return string(c.RequestCtx.FormValue(name))
}
func (c *http11Ctx) FormInt(name string) int {
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
func (c *http11Ctx) FormUint(name string) uint {
	return uint(c.FormInt(name))
}
func (c *http11Ctx) FormInt16(name string) int16 {
	return int16(c.FormInt(name))
}
func (c *http11Ctx) FormUint16(name string) uint16 {
	return uint16(c.FormInt(name))
}
func (c *http11Ctx) FormBool(name string) bool {
	return strings.EqualFold(c.FormString(name), "true")
}
func (c *http11Ctx) FormTime(name string) time.Time {
	var str = c.FormString(name)
	if str == "" {
		return time.Unix(0, 0)
	}
	var i64, err = strconv.ParseInt(str, 10, 64)
	if err != nil {
		return time.Unix(0, 0)
	}
	return time.Unix(i64, 0)
}
func (c *http11Ctx) RouteParams() httpParams {
	return c.Params
}
func (c *http11Ctx) RouteParamString(name string) string {
	return c.Params.ByName(name)
}
func (c *http11Ctx) RouteParamInt(name string) int {
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
func (c *http11Ctx) RouteParamUint(name string) uint {
	return uint(c.RouteParamInt(name))
}
func (c *http11Ctx) RouteParamInt16(name string) int16 {
	return int16(c.RouteParamInt(name))
}
func (c *http11Ctx) RouteParamUint16(name string) uint16 {
	return uint16(c.RouteParamInt(name))
}
func (c *http11Ctx) RouteParamBool(name string) bool {
	return strings.EqualFold(c.RouteParamString(name), "true")
}
func (c *http11Ctx) RouteParamTime(name string) time.Time {
	var str = c.RouteParamString(name)
	if str == "" {
		return time.Unix(0, 0)
	}
	var i64, err = strconv.ParseInt(str, 10, 64)
	if err != nil {
		return time.Unix(0, 0)
	}
	return time.Unix(i64, 0)
}
func (c *http11Ctx) StatusCode() int {
	return c.Response.Header.StatusCode()
}
func (c *http11Ctx) SetResponseBody(body []byte) {
	c.Response.SetBody(body)
}
func (c *http11Ctx) RealIP() (ip net.IP) {
	var xRealIP = c.Request.Header.Peek("X-Real-IP")
	if xRealIP == nil || len(xRealIP) < 1 {
		return c.RequestCtx.RemoteIP()
	}
	if ip = net.ParseIP(string(xRealIP)); ip == nil {
		return c.RequestCtx.RemoteIP()
	}
	return ip
}
