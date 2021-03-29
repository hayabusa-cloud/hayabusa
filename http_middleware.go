package hybs

import (
	"bytes"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
	"go.mongodb.org/mongo-driver/mongo"
	"gorm.io/gorm"
	"net"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Middleware represents middleware function type
type Middleware func(ServiceHandler) ServiceHandler

var middlewareZero = Middleware(func(ServiceHandler) ServiceHandler {
	return func(ctx Ctx) {}
})

// middleware0 returns 零元 middleware which convert any ServiceHandler to nop
func middleware0() Middleware {
	return middlewareZero
}

var middlewareIE = Middleware(func(h ServiceHandler) ServiceHandler {
	return h
})

// middleware1 returns 単位元 middleware which "convert" any ServiceHandler to itself
func middleware1() Middleware {
	return middlewareIE
}

// Apply applies middleware to handler
func (m Middleware) Apply(f ServiceHandler) ServiceHandler {
	return m(f)
}

// Left left combines another middleware
func (m Middleware) Left(right Middleware) Middleware {
	return func(f ServiceHandler) ServiceHandler {
		return m.Apply(right.Apply(f))
	}
}

// Right right combines another middleware
func (m Middleware) Right(left Middleware) Middleware {
	return func(f ServiceHandler) ServiceHandler {
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
func (list *middlewareList) tail() *middlewareList {
	tail := list
	for tail.next != nil {
		tail = tail.next
	}
	return tail
}

type middlewareTree struct {
	children       [0x80]*middlewareTree
	middlewareList *middlewareList
}

func (tree *middlewareTree) seekPath(path []byte) (node *middlewareTree, err error) {
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
			// 不正キャラクターを無視
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

func middlewareBuiltinHttpLog(server *hybsHttpServer) Middleware {
	return func(h ServiceHandler) ServiceHandler {
		return func(ctx Ctx) {
			h(ctx)
			var engineConfig = server.engine.config
			logLevel, isLog := engineConfig.LoggerConfig.LogLevel, false
			if strings.EqualFold(logLevel, "debug") || strings.EqualFold(logLevel, "info") {
				isLog = true
			} else if strings.EqualFold(logLevel, "warn") || strings.EqualFold(logLevel, "warning") {
				isLog = ctx.(*hybsCtx).statusCodeType() > 3
			} else if strings.EqualFold(logLevel, "error") || strings.EqualFold(logLevel, "fatal") {
				isLog = ctx.(*hybsCtx).statusCodeType() > 4
			} else {
				isLog = !ctx.(*hybsCtx).isSuccess()
			}
			if !isLog {
				return
			}
			// set log content
			rawLogger := ctx.(*hybsCtx).sysLoggerEntry
			if engineConfig.AppName != "" {
				rawLogger = rawLogger.WithField("app_name", server.engine.config.AppName)
			}
			if engineConfig.Version != "" {
				rawLogger = rawLogger.WithField("version", engineConfig.Version)
			}
			if engineConfig.Env != "" {
				rawLogger = rawLogger.WithField("env", engineConfig.Env)
			}
			if server.httpConfig.ID != "" {
				rawLogger = rawLogger.WithField("server_id", server.httpConfig.ID)
			}
			user := ctx.CtxValue("UserBase")
			if user != nil {
				rawLogger = rawLogger.WithField("user", user.(*UserBase).ID())
			}
			requestCtx := ctx.(*hybsCtx).httpCtx
			rawLogger = rawLogger.WithField("req_id", ctx.(*hybsCtx).ID())
			auth := fmt.Sprintf("%s", ctx.RequestHeader().Peek("Authorization"))
			rawLogger = rawLogger.WithField("authorization", auth)
			rawLogger = rawLogger.WithField("method", fmt.Sprintf("%s", ctx.(*hybsCtx).Method()))
			rawLogger = rawLogger.WithField("path", fmt.Sprintf("%s", ctx.(*hybsCtx).Path()))
			if !ctx.(*hybsCtx).isSuccess() {
				rawLogger = rawLogger.WithField("req_body", string(requestCtx.RequestBody()))
				rawLogger = rawLogger.WithField("resp_body", ctx.(*hybsCtx).response)
			}
			rawLogger = rawLogger.WithField("status_code", requestCtx.StatusCode())
			rawLogger = rawLogger.WithField("sys_log", ctx.(*hybsCtx).sysLogCollector)
			if engineConfig.DebugMode || !ctx.(*hybsCtx).isSuccess() {
				rawLogger = rawLogger.WithField("game_log", ctx.(*hybsCtx).gameLogCollector)
			}
			// application common fields
			if val := ctx.CtxValue("LogFields"); val != nil {
				if fields, ok := val.(map[string]interface{}); ok {
					for fieldName, fieldValue := range fields {
						rawLogger = rawLogger.WithField(fieldName, fieldValue)
					}
				}
			}
			// write log
			switch ctx.(*hybsCtx).statusCodeType() {
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
	return func(h ServiceHandler) ServiceHandler {
		return func(ctx Ctx) {
			var isAllowed = false
			if len(allow) < 1 {
				isAllowed = true
			}
			for _, allowNet := range allowIPNetList {
				if allowNet.Contains(ctx.(*hybsCtx).RealIP()) {
					isAllowed = true
					break
				}
			}
			for _, denyNet := range denyIPNetList {
				if denyNet.Contains(ctx.(*hybsCtx).RealIP()) {
					isAllowed = false
					break
				}
			}
			if !isAllowed {
				ctx.StatusForbidden("IP address denied")
				return
			}
			h(ctx)
		}
	}
}

func middlewareBuiltinSlowQuery(server *hybsHttpServer, warn time.Duration, error time.Duration) Middleware {
	return func(h ServiceHandler) ServiceHandler {
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
			var st = Now()
			h(ctx)
			var cost = Now().Sub(st)
			// check warn
			if warn >= time.Millisecond && cost >= warn {
				server.engine.sysLogger.Warnf("slow query %s cost %s", ctx.Path(), cost.String())
			}
			// check error
			if error >= time.Millisecond && cost >= error {
				server.engine.sysLogger.Errorf("slow query %s cost %s", ctx.Path(), cost.String())
			}
		}
	}
}

func middlewareBuiltinDefaultStatusCode(server *hybsHttpServer) Middleware {
	_, deleteMethod := []byte("POST"), []byte("DELETE")
	return func(h ServiceHandler) ServiceHandler {
		return func(ctx Ctx) {
			if bytes.Equal(ctx.(*hybsCtx).Method(), deleteMethod) {
				ctx.(*hybsCtx).SetStatusCode(fasthttp.StatusNoContent)
			}
			h(ctx)
		}
	}
}

func middlewareBuiltinResponseJSON(server *hybsHttpServer) Middleware {
	return func(h ServiceHandler) ServiceHandler {
		return func(ctx Ctx) {
			h(ctx)
			var (
				ret []byte = nil
				err error  = nil
			)
			if server.engine.debugMode() {
				ctx.(*hybsCtx).response["debugLog"] = ctx.(*hybsCtx).sysLogCollector
				ret, err = jsoniter.MarshalIndent(ctx.(*hybsCtx).response, "", "  ")
				if err != nil {
					ctx.(*hybsCtx).Error(fasthttp.StatusInternalServerError, err.Error())
					return
				}
			} else {
				ret, err = jsoniter.Marshal(ctx.(*hybsCtx).response)
				if err != nil {
					ctx.(*hybsCtx).Error(fasthttp.StatusInternalServerError, err.Error())
					return
				}
			}
			ctx.(*hybsCtx).SetContentType("application/json; charset=utf-8")
			ctx.(*hybsCtx).SetResponseBody(ret)
		}
	}
}

func middlewareBuiltinCacheUse(eng *hybsEngine, name string) Middleware {
	var config *cacheConfig = nil
	for i, loopConfig := range eng.config.Cache {
		if loopConfig.ID == name {
			config = &eng.config.Cache[i]
			break
		}
	}
	if config == nil {
		fmt.Printf("local cache %s not exists", name)
		return middleware0()
	}
	var nextSaveAt = eng.Now().Add(10 * time.Minute / time.Duration(config.AutoSave*config.AutoSave+1))
	return func(h ServiceHandler) ServiceHandler {
		var cache *hybsCache = nil
		for _, loopCache := range eng.cacheList {
			if loopCache.config.ID != name {
				continue
			}
			cache = loopCache
			break
		}
		if cache == nil {
			return func(ctx Ctx) {
				eng.sysLogger.Errorf(fmt.Sprintf("local cache %s is not working", name))
				ctx.(*hybsCtx).Error(fasthttp.StatusInternalServerError, fmt.Sprintf("Cache Error:%s", name))
			}
		}
		return func(ctx Ctx) {
			ctx.(*hybsCtx).cache[name] = cache.Cache
			h(ctx)
			if config.Filepath == "" || config.AutoSave < 1 {
				return
			}
			nextSaveAt = nextSaveAt.Add(-time.Millisecond * 5)
			if ctx.Now().After(nextSaveAt) {
				var interval = time.Duration(cache.ItemCount()/5+600) * time.Second
				interval /= time.Duration(config.AutoSave*config.AutoSave + 1)
				nextSaveAt = ctx.Now().Add(interval)
				if err := cacheSave(cache.Cache, cache.config); err != nil {
					eng.LogErrorf("local cache [%s] save file to %s failed:%s", config.ID, config.Filepath, err)
				}
			}
		}
	}
}
func middlewareBuiltinRedisUse(eng *hybsEngine, name string) Middleware {
	var config *redisConfig = nil
	for i, loopConfig := range eng.config.Redis {
		if loopConfig.ID == name {
			config = &eng.config.Redis[i]
			break
		}
	}
	if config == nil {
		fmt.Printf("redis %s not exists", name)
		return middleware0()
	}
	return func(h ServiceHandler) ServiceHandler {
		var redisClient *hybsRedis = nil
		for _, loopRedis := range eng.redisList {
			if loopRedis.config.ID != name {
				continue
			}
			redisClient = loopRedis
			break
		}
		if redisClient == nil {
			return func(ctx Ctx) {
				eng.sysLogger.Errorf(fmt.Sprintf("redis %s is not working", name))
				ctx.(*hybsCtx).Error(fasthttp.StatusInternalServerError, fmt.Sprintf("Redis Error:%s", name))
			}
		}
		return func(ctx Ctx) {
			ctx.(*hybsCtx).redis[name] = redisClient
			h(ctx)
		}
	}
}
func middlewareBuiltinMongoDBUse(eng *hybsEngine, name string) Middleware {
	var dbConfig *mongoDBConfig = nil
	for i, config := range eng.config.MongoDB {
		if config.ID == name {
			dbConfig = &eng.config.MongoDB[i]
			break
		}
	}
	if dbConfig == nil {
		fmt.Printf("mongo db %s not exists", name)
		return middleware0()
	}
	return func(h ServiceHandler) ServiceHandler {
		var val, ok = eng.mongoDBMap.Load(dbConfig.ID)
		if !ok {
			return func(ctx Ctx) {
				eng.sysLogger.Errorf(fmt.Sprintf("use mongodb [%s] failed", name))
				ctx.(*hybsCtx).Error(fasthttp.StatusInternalServerError, fmt.Sprintf("DB Error:%s", name))
			}
		}
		var client = val.(*mongo.Client)
		if len(dbConfig.ReplicaSet) > 1 {
			// with transaction to ensure replica set semantics
			return func(ctx Ctx) {
				var dbCtx = ctx.Context()
				var session, err = client.StartSession()
				if err != nil {
					eng.sysLogger.Errorf("start mongo session failed:%s", err)
					return
				}
				defer session.EndSession(dbCtx)
				// store database reference
				ctx.(*hybsCtx).mongodb[name] = session.Client().Database(dbConfig.Database)
				// operation with session as follows:
				session.WithTransaction(dbCtx, func(sessCtx mongo.SessionContext) (interface{}, error) {
					ctx.(*hybsCtx).context = sessCtx
					h(ctx)
					return nil, sessCtx.Err()
				})
				if ctx.HasError() && dbConfig.AutoAbortTransaction {
					session.AbortTransaction(dbCtx)
					ctx.SysLogf("mongo %s transaction aborted", name)
				} else if !ctx.HasError() && dbConfig.AutoCommitTransaction {
					session.CommitTransaction(dbCtx)
					ctx.SysLogf("mongo %s transaction committed", name)
				}
			}
		} else {
			// in case of single node, without transaction
			return func(ctx Ctx) {
				var dbCtx = ctx.Context()
				var session, err = client.StartSession()
				if err != nil {
					eng.sysLogger.Errorf("start mongo session failed:%s", err)
					return
				}
				defer session.EndSession(dbCtx)
				ctx.(*hybsCtx).mongodb[name] = client.Database(dbConfig.Database)
				ctx.(*hybsCtx).context = mongo.NewSessionContext(dbCtx, session)
				h(ctx)
			}
		}
	}
}
func middlewareBuiltinMySQLUse(eng *hybsEngine, name string) Middleware {
	var dbConfig *mySQLConfig = nil
	for i, config := range eng.config.MySQL {
		if config.ID == name {
			dbConfig = &eng.config.MySQL[i]
			break
		}
	}
	if dbConfig == nil {
		fmt.Printf("mysql db %s not exists", name)
		return middleware0()
	}
	return func(h ServiceHandler) ServiceHandler {
		var val, ok = eng.mySQLMap.Load(name)
		if !ok {
			return func(ctx Ctx) {
				eng.sysLogger.Errorf("use mysql [%s] failed", name)
				ctx.(*hybsCtx).Error(fasthttp.StatusInternalServerError)
			}
		}
		if dbConfig.AutoWithTransaction {
			return func(ctx Ctx) {
				tx := val.(*gorm.DB).Begin()
				defer func() {
					if !ctx.HasError() && dbConfig.AutoCommitTransaction {
						tx.Commit()
						return
					} else if ctx.HasError() && dbConfig.AutoRollbackTransaction {
						tx.Rollback()
					}
				}()
				ctx.(*hybsCtx).mysql[name] = tx
				h(ctx)
			}
		} else {
			return func(ctx Ctx) {
				ctx.(*hybsCtx).mysql[name] = val.(*gorm.DB)
				h(ctx)
			}
		}
	}
}
func middlewareBuiltinSqliteUse(eng *hybsEngine, name string) Middleware {
	var dbConfig *sqlite3Config = nil
	for i, config := range eng.config.Sqlite3 {
		if config.ID == name {
			dbConfig = &eng.config.Sqlite3[i]
			break
		}
	}
	if dbConfig == nil {
		fmt.Printf("sqlite %s not exists", name)
		return middleware0()
	}
	return func(h ServiceHandler) ServiceHandler {
		var val, ok = eng.sqlite3Map.Load(name)
		if !ok {
			return func(ctx Ctx) {
				eng.sysLogger.Errorf("use sqlite [%s] failed", name)
				ctx.(*hybsCtx).Error(fasthttp.StatusInternalServerError)
			}
		}
		return func(ctx Ctx) {
			ctx.(*hybsCtx).sqlite[name] = val.(*gorm.DB)
			h(ctx)
		}
	}
}

func middlewareBuiltinSystemInfo(server *hybsHttpServer) Middleware {
	type sysStatus struct {
		NumCPU       int              `json:"numCpu"`
		NumGoroutine int              `json:"numGoroutine"`
		MemStats     runtime.MemStats `json:"-"`
		Alloc        string           `json:"alloc"`
		Sys          string           `json:"sys"`
		TotalAlloc   string           `json:"totalAlloc"`
		At           Time             `json:"-"`
		Comment      string           `json:"comment"`
	}
	var lastSysStatus = &sysStatus{At: TimeNil(), Comment: "SysStatus is only returned on debug mode"}
	var refreshSysStatusFn = func() {
		if !lastSysStatus.At.Add(time.Minute / 2).Before(server.engine.Now()) {
			return
		}
		lastSysStatus.At = server.engine.Now()
		lastSysStatus.NumCPU = runtime.NumCPU()
		lastSysStatus.NumGoroutine = runtime.NumGoroutine()
		runtime.ReadMemStats(&lastSysStatus.MemStats)
		lastSysStatus.Alloc = metaKiloDisplay(lastSysStatus.MemStats.Alloc)
		lastSysStatus.Sys = metaKiloDisplay(lastSysStatus.MemStats.Sys)
		lastSysStatus.TotalAlloc = metaKiloDisplay(lastSysStatus.MemStats.TotalAlloc)
	}
	return func(h ServiceHandler) ServiceHandler {
		return func(ctx Ctx) {
			refreshSysStatusFn()
			ctx.(*hybsCtx).response["env"] = server.engine.config.Env
			ctx.(*hybsCtx).response["serverId"] = server.httpConfig.ID
			ctx.(*hybsCtx).response["serverTime"] = server.engine.Now().Unix()
			if server.engine.debugMode() {
				ctx.(*hybsCtx).response["debugMode"] = true
				ctx.(*hybsCtx).response["sysStatus"] = lastSysStatus
			}
			h(ctx)
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
	return func(h ServiceHandler) ServiceHandler {
		return func(ctx Ctx) {
			arg := ctx.(*hybsCtx).httpCtx.RouteParams().ByName(name)
			isAllowed := false
			for _, rule := range allowExps {
				if rule.MatchString(arg) {
					isAllowed = true
					break
				}
			}
			if !isAllowed {
				ctx.(*hybsCtx).SetStatusCode(fasthttp.StatusBadRequest)
				ctx.(*hybsCtx).SetResponseBody(errorMessage)
				return
			}
			h(ctx)
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
	return func(h ServiceHandler) ServiceHandler {
		return func(ctx Ctx) {
			arg := ctx.(*hybsCtx).RouteParams().ByName(name)
			for _, rule := range denyExps {
				if rule.MatchString(arg) {
					ctx.(*hybsCtx).SetStatusCode(fasthttp.StatusBadRequest)
					ctx.(*hybsCtx).SetResponseBody(errorMessage)
					return
				}
			}
			h(ctx)
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
	return func(h ServiceHandler) ServiceHandler {
		return func(ctx Ctx) {
			value := ctx.(*hybsCtx).FormBytes(key)
			isAllowed := false
			for _, rule := range allowExps {
				if rule.Match(value) {
					isAllowed = true
					break
				}
			}
			if !isAllowed {
				ctx.(*hybsCtx).SetStatusCode(fasthttp.StatusBadRequest)
				ctx.(*hybsCtx).SetResponseBody(errorMessage)
				return
			}
			h(ctx)
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
	return func(h ServiceHandler) ServiceHandler {
		return func(ctx Ctx) {
			value := ctx.(*hybsCtx).FormBytes(key)
			for _, rule := range denyExps {
				if rule.Match(value) {
					ctx.(*hybsCtx).SetStatusCode(fasthttp.StatusBadRequest)
					ctx.(*hybsCtx).SetResponseBody(errorMessage)
					return
				}
			}
			h(ctx)
		}
	}
}

func middlewareBuiltinConstParam(key string, value interface{}) Middleware {
	return func(h ServiceHandler) ServiceHandler {
		return func(ctx Ctx) {
			ctx.SetCtxValue(fmt.Sprintf("__CONST_%s", key), value)
			h(ctx)
		}
	}
}

func middlewareBuiltinResponseLinks(server *hybsHttpServer, links interface{}) Middleware {
	return func(h ServiceHandler) ServiceHandler {
		return func(ctx Ctx) {
			h(ctx)
			if links != nil {
				mpLinks, ok := links.(map[string]struct{ Href string })
				if ok && len(mpLinks) > 0 {
					ctx.(*hybsCtx).response["links"] = links
				}
			}
		}
	}
}

func middlewareBuiltinGameLog(server *hybsHttpServer) Middleware {
	return func(h ServiceHandler) ServiceHandler {
		return func(ctx Ctx) {
			h(ctx)
			if !ctx.(*hybsCtx).isSuccess() {
				return
			}
			if len(ctx.(*hybsCtx).gameLogCollector) < 1 {
				return
			}
			// set log content
			rawLogger := ctx.(*hybsCtx).gameLoggerEntry
			if server.engine.config.AppName != "" {
				rawLogger = rawLogger.WithField("app_name", server.engine.config.AppName)
			}
			if server.engine.config.Version != "" {
				rawLogger = rawLogger.WithField("version", server.engine.config.Version)
			}
			if server.engine.config.Env != "" {
				rawLogger = rawLogger.WithField("env", server.engine.config.Env)
			}
			if server.httpConfig.ID != "" {
				rawLogger = rawLogger.WithField("server_id", server.httpConfig.ID)
			}
			user := ctx.CtxValue("UserBase")
			if user != nil {
				rawLogger = rawLogger.WithField("user", user.(*UserBase).ID())
			}
			requestCtx := ctx.(*hybsCtx)
			rawLogger = rawLogger.WithField("req_id", ctx.(*hybsCtx).ID())
			auth := ctx.RequestHeader().Peek("Authorization")
			rawLogger = rawLogger.WithField("authorization", fmt.Sprintf("%s", auth))
			rawLogger = rawLogger.WithField("method", fmt.Sprintf("%s", ctx.(*hybsCtx).Method()))
			rawLogger = rawLogger.WithField("path", fmt.Sprintf("%s", requestCtx.RequestURI()))
			rawLogger = rawLogger.WithField("status_code", requestCtx.StatusCode())
			// application common fields
			if val := ctx.CtxValue("LogFields"); val != nil {
				if fields, ok := val.(map[string]interface{}); ok {
					for fieldName, fieldValue := range fields {
						rawLogger = rawLogger.WithField(fieldName, fieldValue)
					}
				}
			}
			// write logs
			for _, gl := range ctx.(*hybsCtx).gameLogCollector {
				gameLogEntry := rawLogger.WithField("action", gl.action)
				if gl.fieldMap != nil {
					for fieldName, fieldValue := range gl.fieldMap {
						gameLogEntry = gameLogEntry.WithField(fieldName, fieldValue)
					}
				}
				gameLogEntry.Infoln(gl.comment)
			}
		}
	}
}

func middlewareBuiltinAuthentication(server *hybsHttpServer) Middleware {
	var basicAuthPrefix = []byte("Bearer ")
	var blackMap = make(map[string]time.Time)
	var blackMapLock = &sync.RWMutex{}
	return func(h ServiceHandler) ServiceHandler {
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
			if bytes.HasPrefix(auth, basicAuthPrefix) {
				u := newUserFromAccessToken(string(auth[len(basicAuthPrefix):]))
				if u != nil {
					ctx.SetCtxValue("UserBase", u)
				}
			}
			h(ctx)
			if ctx.HasError() && ctx.(*hybsCtx).StatusCode() < 500 {
				blackMapLock.Lock()
				if len(blackMap) > server.httpConfig.Concurrency*4 {
					blackMap = make(map[string]time.Time)
				}
				if server.engine.debugMode() {
					blackMap[string(auth)] = time.Now().Add(time.Second * 5)
				} else {
					blackMap[string(auth)] = time.Now().Add(time.Minute)
				}
				blackMapLock.Unlock()
			}
		}
	}
}
