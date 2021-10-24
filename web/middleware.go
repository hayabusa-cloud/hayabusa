package web

import (
	"bytes"
	"fmt"
	"net"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/hayabusa-cloud/hayabusa/engine"
	"github.com/hayabusa-cloud/hayabusa/plugins"
	"github.com/hayabusa-cloud/hayabusa/utils"
	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
	"go.mongodb.org/mongo-driver/mongo"
)

// Middleware represents middleware function type
type Middleware func(Handler) Handler

var middlewareZero = Middleware(func(Handler) Handler {
	return func(ctx Ctx) {}
})

// middleware0 returns zero-middleware which convert any Handler to nop
func middleware0() Middleware {
	return middlewareZero
}

var middlewareIE = Middleware(func(h Handler) Handler {
	return h
})

// middleware1 returns unit-middleware which "convert" any Handler to itself
func middleware1() Middleware {
	return middlewareIE
}

// Apply applies middleware to handler
func (m Middleware) Apply(f Handler) Handler {
	return m(f)
}

// Left left combines another middleware
func (m Middleware) Left(right Middleware) Middleware {
	return func(f Handler) Handler {
		return m.Apply(right.Apply(f))
	}
}

// Right right combines another middleware
func (m Middleware) Right(left Middleware) Middleware {
	return func(f Handler) Handler {
		return left.Apply(m.Apply(f))
	}
}

type middlewareList struct {
	m    Middleware
	next *middlewareList
}

func (list *middlewareList) add(m Middleware) *middlewareList {
	parent, current := &middlewareList{next: list}, list
	for current != nil {
		parent = current
		current = current.next
	}
	current = &middlewareList{m: m, next: nil}
	parent.next = current
	return current
}

//lint:ignore U1000 reserving
func (list *middlewareList) tail() *middlewareList {
	tail := list
	for tail.next != nil {
		tail = tail.next
	}
	return tail
}

type middlewareTuple struct {
	middlewareID string
	m            Middleware
}

var middlewareTupleList []*middlewareTuple

func registerMiddlewareImpl(middlewareID string, m Middleware) {
	middlewareTupleList = append(middlewareTupleList, &middlewareTuple{
		middlewareID: middlewareID,
		m:            m,
	})
}

type middlewareTree struct {
	children       [0x80]*middlewareTree
	middlewareList *middlewareList
}

//lint:ignore U1000 reserving
func (tree *middlewareTree) seek(path []byte) (node *middlewareTree, err error) {
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

func (tree *middlewareTree) add(path []byte, m Middleware) {
	if path == nil || m == nil {
		return
	}
	current := tree
	for i := 0; i < len(path); i++ {
		if path[i] > 0x7F {
			// ignore invalid character
			continue
		}
		if current.children[path[i]] == nil {
			current.children[path[i]] = &middlewareTree{
				children:       [0x80]*middlewareTree{nil},
				middlewareList: nil,
			}
		}
		current = current.children[path[i]]
	}
	if current.middlewareList == nil {
		current.middlewareList = &middlewareList{m: m, next: nil}
		return
	}
	current.middlewareList.add(m)
}

func (tree *middlewareTree) combine(path []byte) (ret Middleware) {
	if path == nil || len(path) < 1 {
		return middlewareIE
	}
	node := tree
	ret = middlewareIE
	for i := 0; i < len(path); i++ {
		if path[i] > 0x7F {
			return ret
		}
		node = node.children[path[i]]
		if node == nil {
			return ret
		}
		list := node.middlewareList
		for list != nil {
			if list.m != nil {
				ret = ret.Left(list.m)
			}
			list = list.next
		}
	}
	return ret
}

func middlewareBuiltinSysLog(server *Server) Middleware {
	return func(h Handler) Handler {
		return func(c Ctx) {
			h(c)
			logLevel, isLog := server.config.LogLevel, false
			if strings.EqualFold(logLevel, "debug") || strings.EqualFold(logLevel, "info") {
				isLog = true
			} else if strings.EqualFold(logLevel, "warn") || strings.EqualFold(logLevel, "warning") {
				isLog = c.(*ctx).statusCodeType() > 3
			} else if strings.EqualFold(logLevel, "error") || strings.EqualFold(logLevel, "fatal") {
				isLog = c.(*ctx).statusCodeType() > 4
			} else {
				isLog = !c.(*ctx).isSuccess()
			}
			if !isLog {
				return
			}
			// set log content
			rawLogger := c.(*ctx).sysLoggerEntry
			if server.engine.AppName() != "" {
				rawLogger = rawLogger.WithField("app_name", server.engine.AppName())
			}
			if server.engine.Version() != "" {
				rawLogger = rawLogger.WithField("version", server.engine.Version())
			}
			if server.engine.Env() != "" {
				rawLogger = rawLogger.WithField("env", server.engine.Env())
			}
			if server.config.ID != "" {
				rawLogger = rawLogger.WithField("server_id", server.config.ID)
			}
			user := c.CtxValue("Authentication")
			if user != nil {
				rawLogger = rawLogger.WithField("user", user.(*Authentication).ID())
			}
			requestCtx := c.(*ctx).httpCtx
			rawLogger = rawLogger.WithField("req_id", c.ID())
			auth := string(c.RequestHeader().Peek("Authorization"))
			rawLogger = rawLogger.WithField("authorization", auth)
			rawLogger = rawLogger.WithField("method", string(c.Method()))
			rawLogger = rawLogger.WithField("path", string(c.Path()))
			if !c.(*ctx).isSuccess() {
				rawLogger = rawLogger.WithField("req_body", string(requestCtx.RequestBody()))
				rawLogger = rawLogger.WithField("resp_body", c.(*ctx).response)
			}
			rawLogger = rawLogger.WithField("status_code", requestCtx.StatusCode())
			rawLogger = rawLogger.WithField("sys_log", c.(*ctx).sysLogCollector)
			if server.engine.DebugMode() || !c.(*ctx).isSuccess() {
				rawLogger = rawLogger.WithField("game_log", c.(*ctx).gameLogCollector)
			}
			// application common fields
			if val := c.CtxValue("LogFields"); val != nil {
				if fields, ok := val.(map[string]interface{}); ok {
					for fieldName, fieldValue := range fields {
						rawLogger = rawLogger.WithField(fieldName, fieldValue)
					}
				}
			}
			// write log
			switch c.(*ctx).statusCodeType() {
			case fasthttp.StatusOK / 100:
				rawLogger.Infoln()
			case fasthttp.StatusFound / 100:
				rawLogger.Infoln()
			case fasthttp.StatusBadRequest / 100:
				rawLogger.Warnln()
			case fasthttp.StatusInternalServerError / 100:
				rawLogger.Errorln()
			default:
				rawLogger.Infoln()
			}
		}
	}
}

func middlewareBuiltinIPControl(allow []string, deny []string) Middleware {
	if len(allow) < 1 && len(deny) < 1 {
		return middlewareIE
	}
	allowIPNetList, denyIPNetList := make([]*net.IPNet, 0, len(allow)), make([]*net.IPNet, 0, len(deny))
	for _, allowStr := range allow {
		if _, allowNet, err := net.ParseCIDR(allowStr); err != nil {
			panic(err)
		} else {
			allowIPNetList = append(allowIPNetList, allowNet)
		}
	}
	for _, denyStr := range deny {
		if _, denyNet, err := net.ParseCIDR(denyStr); err != nil {
			panic(err)
		} else {
			denyIPNetList = append(denyIPNetList, denyNet)
		}
	}
	return func(h Handler) Handler {
		return func(c Ctx) {
			var isAllowed = false
			if len(allow) < 1 {
				isAllowed = true
			}
			for _, allowNet := range allowIPNetList {
				if allowNet.Contains(c.RealIP()) {
					isAllowed = true
					break
				}
			}
			for _, denyNet := range denyIPNetList {
				if denyNet.Contains(c.RealIP()) {
					isAllowed = false
					break
				}
			}
			if !isAllowed {
				c.StatusForbidden("IP address denied")
				return
			}
			h(c)
		}
	}
}

func middlewareBuiltinSlowQuery(server *Server, warn time.Duration, error time.Duration) Middleware {
	return func(h Handler) Handler {
		// skip slow query check
		if warn < time.Millisecond && error < time.Millisecond {
			return h
		}
		// error must greater than or equal warn
		if error >= time.Millisecond && error < warn {
			error = warn
		}
		// return handler with slow query
		return func(ctx Ctx) {
			var st = server.engine.Now()
			h(ctx)
			var cost = server.engine.Now().Sub(st)
			// check warn
			if warn >= time.Millisecond && cost >= warn {
				server.sysLogger.Warnf("slow query %s cost %s", ctx.Path(), cost.String())
			}
			// check error
			if error >= time.Millisecond && cost >= error {
				server.sysLogger.Errorf("slow query %s cost %s", ctx.Path(), cost.String())
			}
		}
	}
}

func middlewareBuiltinDefaultStatusCode() Middleware {
	_, deleteMethod := []byte("POST"), []byte("DELETE")
	return func(h Handler) Handler {
		return func(c Ctx) {
			if bytes.Equal(c.Method(), deleteMethod) {
				c.SetStatusCode(fasthttp.StatusNoContent)
			}
			h(c)
		}
	}
}

func middlewareBuiltinResponseJSON(server *Server) Middleware {
	return func(h Handler) Handler {
		return func(c Ctx) {
			h(c)
			var (
				ret []byte = nil
				err error  = nil
			)
			if server.engine.DebugMode() {
				c.(*ctx).response["debugLog"] = c.(*ctx).sysLogCollector
				ret, err = jsoniter.MarshalIndent(c.(*ctx).response, "", "  ")
				if err != nil {
					c.Error(fasthttp.StatusInternalServerError, err.Error())
					return
				}
			} else {
				ret, err = jsoniter.Marshal(c.(*ctx).response)
				if err != nil {
					c.Error(fasthttp.StatusInternalServerError, err.Error())
					return
				}
			}
			c.SetContentType("application/json; charset=utf-8")
			c.SetResponseBody(ret)
		}
	}
}

func middlewareBuiltinResponseFound(server *Server) Middleware {
	return func(h Handler) Handler {
		return func(c Ctx) {
			h(c)
			c.SetStatusCode(fasthttp.StatusFound)
			var locBytes = c.ResponseHeader().PeekBytes([]byte("Location"))
			if locBytes == nil || len(locBytes) < 1 {
				var redirectTo = c.ConstStringValue("redirect_to")
				if len(redirectTo) > 0 {
					c.ResponseHeader().Set("Location", redirectTo)
					return
				}

				var (
					val = c.CtxValue("redirect_to")
					ok  bool
				)
				if val != nil {
					if redirectTo, ok = val.(string); ok {
						c.ResponseHeader().Set("Location", redirectTo)
						return
					}
				}
			}
		}
	}
}

func middlewarePluginCache(eng engine.Interface, name string) Middleware {
	var (
		nextSaveAt = eng.Now().Add(time.Minute)
	)
	return func(h Handler) Handler {
		var plugin = eng.Cache(name)
		if plugin == nil {
			eng.Errorf("cache %s not exists", name)
			return handler0
		}
		return func(c Ctx) {
			c.(*ctx).cache[name] = plugin
			h(c)
			if plugin.Config.Filepath == "" || plugin.Config.AutoSave < 1 {
				return
			}
			nextSaveAt = nextSaveAt.Add(-time.Millisecond * 3)
			if c.Now().After(nextSaveAt) {
				var interval = time.Duration(plugin.ItemCount()/5)*time.Second + time.Minute*15
				interval /= time.Duration(plugin.Config.AutoSave*plugin.Config.AutoSave + 1)
				nextSaveAt = c.Now().Add(interval)
				if err := plugin.Save(); err != nil {
					c.SysLogf("cache save error:%s", err)
					eng.Warnf("cache save error:%s", err)
				}
			}
		}
	}
}
func middlewarePluginRedis(eng engine.Interface, name string) Middleware {
	return func(h Handler) Handler {
		var plugin = eng.Redis(name)
		if plugin == nil {
			eng.Errorf("redis %s not exists", name)
			return handler0
		}
		return func(c Ctx) {
			c.(*ctx).redis[name] = plugin
			h(c)
		}
	}
}
func middlewarePluginMongo(eng engine.Interface, name string) Middleware {
	return func(h Handler) Handler {
		var plugin = eng.Mongo(name)
		if plugin == nil {
			eng.Errorf("mongo %s not exists", name)
			return handler0
		}
		if len(plugin.Config.ReplicaSet) > 1 {
			// with transaction to ensure replica set semantics
			return func(c Ctx) {
				var dbCtx = c.Context()
				var session, err = plugin.Client().StartSession()
				if err != nil {
					c.SysLogf("start mongo session error:%s", err)
					c.StatusInternalServerError()
					return
				}
				defer session.EndSession(dbCtx)
				// store database reference
				c.(*ctx).mongodb[name] = &plugins.Mongo{
					Database: session.Client().Database(plugin.Config.Database),
					Config:   plugin.Config,
				}
				// operation with session as follows:
				_, _ = session.WithTransaction(dbCtx, func(sessCtx mongo.SessionContext) (interface{}, error) {
					c.(*ctx).context = sessCtx
					h(c)
					return nil, sessCtx.Err()
				})
				if c.HasError() && plugin.Config.AutoAbortTransaction {
					_ = session.AbortTransaction(dbCtx)
					c.SysLogf("mongo %s transaction aborted", name)
				} else if !c.HasError() && plugin.Config.AutoCommitTransaction {
					_ = session.CommitTransaction(dbCtx)
					c.SysLogf("mongo %s transaction committed", name)
				}
			}
		} else {
			// in case of single node, without transaction
			return func(c Ctx) {
				var dbCtx = c.Context()
				var session, err = plugin.Client().StartSession()
				if err != nil {
					c.SysLogf("start mongo session error:%s", err)
					c.StatusInternalServerError()
					return
				}
				defer session.EndSession(dbCtx)
				c.(*ctx).mongodb[name] = plugin
				c.(*ctx).context = mongo.NewSessionContext(dbCtx, session)
				h(c)
			}
		}
	}
}
func middlewarePluginMySQL(eng engine.Interface, name string) Middleware {
	return func(h Handler) Handler {
		var plugin = eng.MySQL(name)
		if plugin == nil {
			eng.Errorf("mysql %s not exists", name)
			return handler0
		}
		if plugin.Config.AutoWithTransaction {
			return func(c Ctx) {
				tx := plugin.Begin()
				defer func() {
					if !c.HasError() && plugin.Config.AutoCommitTransaction {
						tx.Commit()
						return
					} else if c.HasError() && plugin.Config.AutoRollbackTransaction {
						tx.Rollback()
					}
				}()
				c.(*ctx).mysql[name] = &plugins.MySQL{Config: plugin.Config, DB: tx}
				h(c)
			}
		} else {
			return func(c Ctx) {
				c.(*ctx).mysql[name] = plugin
				h(c)
			}
		}
	}
}
func middlewarePluginSqlite3(eng engine.Interface, name string) Middleware {
	return func(h Handler) Handler {
		var plugin = eng.Sqlite(name)
		if plugin == nil {
			eng.Errorf("sqlite %s not exists", name)
			return handler0
		}
		return func(c Ctx) {
			c.(*ctx).sqlite[name] = plugin
			h(c)
		}
	}
}

func middlewareBuiltinSystemInfo(server *Server) Middleware {
	type sysStatus struct {
		NumCPU       int              `json:"numCpu"`
		NumGoroutine int              `json:"numGoroutine"`
		MemStats     runtime.MemStats `json:"-"`
		Alloc        string           `json:"alloc"`
		Sys          string           `json:"sys"`
		TotalAlloc   string           `json:"totalAlloc"`
		At           *time.Time       `json:"-"`
		Comment      string           `json:"comment"`
	}
	var lastSysStatus = &sysStatus{At: nil, Comment: "SysStatus is only returned on debug mode"}
	var refreshSysStatusFn = func() {
		if lastSysStatus.At != nil && !lastSysStatus.At.Add(time.Minute/2).Before(server.engine.Now()) {
			return
		}
		var now = server.engine.Now()
		lastSysStatus.At = &now
		lastSysStatus.NumCPU = runtime.NumCPU()
		lastSysStatus.NumGoroutine = runtime.NumGoroutine()
		runtime.ReadMemStats(&lastSysStatus.MemStats)
		lastSysStatus.Alloc = utils.NumMetaKiloString(lastSysStatus.MemStats.Alloc)
		lastSysStatus.Sys = utils.NumMetaKiloString(lastSysStatus.MemStats.Sys)
		lastSysStatus.TotalAlloc = utils.NumMetaKiloString(lastSysStatus.MemStats.TotalAlloc)
	}
	return func(h Handler) Handler {
		return func(c Ctx) {
			refreshSysStatusFn()
			c.(*ctx).response["env"] = server.engine.Env()
			c.(*ctx).response["serverId"] = server.config.ID
			c.(*ctx).response["serverTime"] = server.engine.Now().Unix()
			if server.engine.DebugMode() {
				c.(*ctx).response["debugMode"] = true
				c.(*ctx).response["sysStatus"] = lastSysStatus
			}
			h(c)
		}
	}
}

func middlewareBuiltinPathParamsAllow(name string, allow []string) Middleware {
	allowExps := make([]*regexp.Regexp, 0, len(allow))
	for _, elem := range allow {
		allowExps = append(allowExps, regexp.MustCompile(elem))
	}
	if len(allowExps) < 1 {
		return middlewareIE
	}
	errorMessage := []byte(fmt.Sprintf("value of path param %s is not allowed", name))
	return func(h Handler) Handler {
		return func(c Ctx) {
			arg := c.(*ctx).httpCtx.RouteParams().ByName(name)
			isAllowed := false
			for _, rule := range allowExps {
				if rule.MatchString(arg) {
					isAllowed = true
					break
				}
			}
			if !isAllowed {
				c.SetStatusCode(fasthttp.StatusBadRequest)
				c.SetResponseBody(errorMessage)
				return
			}
			h(c)
		}
	}
}

func middlewareBuiltinPathParamsDeny(name string, deny []string) Middleware {
	denyExps := make([]*regexp.Regexp, 0, len(deny))
	for _, elem := range deny {
		denyExps = append(denyExps, regexp.MustCompile(elem))
	}
	if len(denyExps) < 1 {
		return middlewareIE
	}
	errorMessage := []byte(fmt.Sprintf("value of path param %s is denied", name))
	return func(h Handler) Handler {
		return func(c Ctx) {
			arg := c.RouteParams().ByName(name)
			for _, rule := range denyExps {
				if rule.MatchString(arg) {
					c.SetStatusCode(fasthttp.StatusBadRequest)
					c.SetResponseBody(errorMessage)
					return
				}
			}
			h(c)
		}
	}
}

func middlewareBuiltinFormValueAllow(key string, allow []string) Middleware {
	allowExps := make([]*regexp.Regexp, 0, len(allow))
	for _, elem := range allow {
		allowExps = append(allowExps, regexp.MustCompile(elem))
	}
	if len(allowExps) < 1 {
		return middlewareIE
	}
	errorMessage := []byte(fmt.Sprintf("form value %s is not allowed", key))
	return func(h Handler) Handler {
		return func(c Ctx) {
			value := c.FormBytes(key)
			isAllowed := false
			for _, rule := range allowExps {
				if rule.Match(value) {
					isAllowed = true
					break
				}
			}
			if !isAllowed {
				c.SetStatusCode(fasthttp.StatusBadRequest)
				c.SetResponseBody(errorMessage)
				return
			}
			h(c)
		}
	}
}

func middlewareBuiltinFormValueDeny(key string, deny []string) Middleware {
	denyExps := make([]*regexp.Regexp, 0, len(deny))
	for _, elem := range deny {
		denyExps = append(denyExps, regexp.MustCompile(elem))
	}
	if len(denyExps) < 1 {
		return middlewareIE
	}
	errorMessage := []byte(fmt.Sprintf("form value %s is denied", key))
	return func(h Handler) Handler {
		return func(c Ctx) {
			value := c.FormBytes(key)
			for _, rule := range denyExps {
				if rule.Match(value) {
					c.SetStatusCode(fasthttp.StatusBadRequest)
					c.SetResponseBody(errorMessage)
					return
				}
			}
			h(c)
		}
	}
}

func middlewareBuiltinConstParam(key string, value interface{}) Middleware {
	return func(h Handler) Handler {
		return func(ctx Ctx) {
			ctx.SetCtxValue(fmt.Sprintf("__CONST_%s", key), value)
			h(ctx)
		}
	}
}

func middlewareBuiltinResponseLinks(links interface{}) Middleware {
	return func(h Handler) Handler {
		return func(c Ctx) {
			h(c)
			if links != nil {
				mpLinks, ok := links.(map[string]struct{ Href string })
				if ok && len(mpLinks) > 0 {
					c.(*ctx).response["links"] = links
				}
			}
		}
	}
}

func middlewareBuiltinGameLog(server *Server) Middleware {
	return func(h Handler) Handler {
		return func(c Ctx) {
			h(c)
			if !c.(*ctx).isSuccess() {
				return
			}
			if len(c.(*ctx).gameLogCollector) < 1 {
				return
			}
			// set log content
			var entry = c.(*ctx).gameLoggerEntry
			if server.engine.AppName() != "" {
				entry = entry.WithField("app_name", server.engine.AppName())
			}
			if server.engine.Version() != "" {
				entry = entry.WithField("version", server.engine.Version())
			}
			if server.engine.Env() != "" {
				entry = entry.WithField("env", server.engine.Env())
			}
			if server.config.ID != "" {
				entry = entry.WithField("server_id", server.config.ID)
			}
			user := c.CtxValue("Authentication")
			if user != nil {
				entry = entry.WithField("user", user.(*Authentication).ID())
			}
			entry = entry.WithField("req_id", c.ID())
			auth := c.RequestHeader().Peek("Authorization")
			entry = entry.WithField("authorization", string(auth))
			entry = entry.WithField("method", string(c.Method()))
			entry = entry.WithField("path", c.RequestURI())
			entry = entry.WithField("status_code", c.StatusCode())
			// application common fields
			if val := c.CtxValue("LogFields"); val != nil {
				if fields, ok := val.(map[string]interface{}); ok {
					for fieldName, fieldValue := range fields {
						entry = entry.WithField(fieldName, fieldValue)
					}
				}
			}
			// write logs
			for _, gl := range c.(*ctx).gameLogCollector {
				gameLogEntry := entry.WithField("action", gl.Action)
				if gl.Fields != nil {
					for fieldName, fieldValue := range gl.Fields {
						gameLogEntry = gameLogEntry.WithField(fieldName, fieldValue)
					}
				}
				gameLogEntry.Infoln(gl.Comment)
			}
		}
	}
}

func middlewareBuiltinAuthentication(server *Server) Middleware {
	var authHeaderPrefix = []byte("Bearer ")
	var (
		blackMap     = make(map[string]time.Time)
		blackMapLock = &sync.RWMutex{}
	)
	var guestAuthentication = &Authentication{
		UserID:     "guest",
		Permission: PermissionGuest,
	}
	guestAuthentication.randomize()
	return func(h Handler) Handler {
		return func(ctx Ctx) {
			auth := ctx.RequestHeader().Peek("Authorization")
			if len(auth) > 0 {
				blackMapLock.RLock()
				until, ok := blackMap[string(auth)]
				blackMapLock.RUnlock()
				if ok && until.After(time.Now()) {
					ctx.Error(fasthttp.StatusTooManyRequests)
					return
				}
			}
			if bytes.HasPrefix(auth, authHeaderPrefix) {
				var auth = AuthenticationFromAccessToken(string(auth[len(authHeaderPrefix):]))
				if auth != nil {
					ctx.SetCtxValue("Authentication", auth)
				} else {
					ctx.SetCtxValue("Authentication", guestAuthentication)
				}
			}
			h(ctx)
			if ctx.HasError() && ctx.StatusCode() < 500 {
				blackMapLock.Lock()
				if len(blackMap) > server.config.Concurrency*4+500 {
					blackMap = make(map[string]time.Time)
				}
				if server.engine.DebugMode() {
					blackMap[string(auth)] = time.Now().Add(time.Second * 5)
				} else {
					blackMap[string(auth)] = time.Now().Add(time.Minute)
				}
				blackMapLock.Unlock()
			}
		}
	}
}
