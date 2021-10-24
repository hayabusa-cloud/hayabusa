package web

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fasthttp"
)

type http3Ctx struct {
	*http.Request
	*http.Response
	http.ResponseWriter
	userValue map[string]interface{}
}

func createHttp3Ctx(server *Server, rw http.ResponseWriter, req *http.Request) *ctx {
	var newCtx = createNewCtx(server)
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
func (c *http3Ctx) Authentication() *Authentication {
	val, ok := c.userValue["Authentication"]
	if ok && val != nil {
		return val.(*Authentication)
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
	if val, ok := c.userValue["Server"].(*Server).engine.UserValue(name); ok {
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
func (c *http3Ctx) FormTime(name string) time.Time {
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
func (c *http3Ctx) RouteParamTime(name string) time.Time {
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
	_, _ = c.ResponseWriter.Write(body)
}
func (c *http3Ctx) RemoteIP() net.IP {
	return net.ParseIP(c.Request.RemoteAddr)
}
func (c *http3Ctx) RealIP() (ip net.IP) {
	var xRealIP = c.Request.Header.Get("X-Real-IP")
	if xRealIP == "" {
		return net.ParseIP(c.Request.RemoteAddr)
	}
	if ip = net.ParseIP(xRealIP); ip == nil {
		return net.ParseIP(c.Request.RemoteAddr)
	}
	return ip
}
func (c *http3Ctx) SetConnectionClose() {
	c.Request.Close = true
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

type http3Params struct {
	url.Values
}

func (v *http3Params) ByName(name string) string {
	return v.Get(name)
}
