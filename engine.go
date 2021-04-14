package hybs

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jinzhu/configor"
	"github.com/patrickmn/go-cache"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type engineConfig struct {
	AppName        string             `yaml:"app_name" default:""`
	Env            string             `yaml:"env" default:""`
	Version        string             `yaml:"version" default:""`
	PidFilepath    string             `yaml:"pid_filepath" default:""`
	LoggerConfig   loggerConfig       `yaml:",inline"`
	AppConfig      string             `yaml:"app_config" default:"config/application.yml"`
	DebugMode      bool               `yaml:"debug_mode" default:"false"`
	DisplayLogo    bool               `yaml:"display_logo" default:"true"`
	CsvFilepath    string             `yaml:"csv_filepath" default:"csv/"`
	Cache          []cacheConfig      `yaml:"cache"`
	Redis          []redisConfig      `yaml:"redis"`
	MongoDB        []mongoDBConfig    `yaml:"mongodb"`
	MySQL          []mySQLConfig      `yaml:"mysql"`
	Sqlite3        []sqlite3Config    `yaml:"sqlite"`
	Batch          []batchConfig      `yaml:"batch"`
	LBClient       []lbClientConfig   `yaml:"lb_client"`
	HttpGate       []httpGateConfig   `yaml:"http_gate"`
	HttpServer     []httpServerConfig `yaml:"http_server"`
	RealtimeServer []realtimeConfig   `yaml:"realtime_server"`
}

func loadEngineConfig() (config *engineConfig, err error) {
	config = &engineConfig{}
	// load serverConfig file
	var configFile = flag.String("f", "./config/default.yml", "filepath of config files")
	flag.Parse()
	if configFile == nil {
		panic(fmt.Errorf("-f flag specifies config filename"))
	}
	var configLoader = configor.New(&configor.Config{
		AutoReload: false,
	})
	if err := configLoader.Load(config, *configFile); err != nil {
		return nil, fmt.Errorf("load config file failed:%s", err)
	}
	return config, nil
}

// Engine is interface of hybsEngine implements
type Engine interface {
	Stop() (err error)
	SqliteDo(dbID string, f func(db *gorm.DB) error) error
	MySQLDo(dbID string, f func(db *gorm.DB) error) error
	MongoDo(dbID string, f func(db *mongo.Database) error) error
	RedisDo(insID string, f func(r redis.UniversalClient) error) error
	CacheDo(insID string, f func(c *cache.Cache) error) error
	LogInfof(format string, args ...interface{})
	LogWarnf(format string, args ...interface{})
	LogErrorf(format string, args ...interface{})
	Now() Time
	AppConfig() string
	Context() context.Context
	UserValue(key string) (value interface{}, ok bool)
	SetUserValue(key string, value interface{})
}

type hybsEngine struct {
	// config
	config *engineConfig
	// instances
	batches         []*hybsBatch           // batch and timer events
	lbClients       []*hybsBalancingClient // balancing clients
	httpGates       []*hybsHttpGate        // simple proxy server
	httpServers     []*hybsHttpServer      // fasthttp server or http3 server
	realtimeServers []*hybsRealtimeServer  // udp socket server
	// plugins
	sqlite3Map        *sync.Map    // name->*gorm.DB
	mySQLMap          *sync.Map    // name->*gorm.DB
	mongoDBMap        *sync.Map    // name->*Session
	redisList         []*hybsRedis // for performance, DO NOT use map
	cacheList         []*hybsCache // for performance, DO NOT use map
	masterDataMgr     masterDataManager
	masterDataLoaders []masterDataLoaderFunc
	sysLogger         Logger
	gameLogger        Logger
	scheduler         *scheduler

	context   context.Context
	userValue kvPairs
}

func (eng *hybsEngine) Sqlite(dbID string) (db *gorm.DB) {
	if eng.sqlite3Map == nil {
		return nil
	}
	var val, ok = eng.sqlite3Map.Load(dbID)
	if !ok {
		return nil
	}
	return val.(*gorm.DB)
}

func (eng *hybsEngine) MySQL(dbID string) (db *gorm.DB) {
	if eng.mySQLMap == nil {
		return nil
	}
	var val, ok = eng.mySQLMap.Load(dbID)
	if !ok {
		return nil
	}
	return val.(*gorm.DB)
}

func (eng *hybsEngine) Mongo(dbID string) (db *mongo.Database) {
	if eng.mongoDBMap == nil {
		return nil
	}
	// search database
	var databaseName = ""
	for _, dbConfig := range eng.config.MongoDB {
		if dbConfig.ID == dbID {
			databaseName = dbConfig.Database
			break
		}
	}
	if databaseName == "" {
		return nil
	}
	var val, ok = eng.mongoDBMap.Load(dbID)
	if !ok {
		return nil
	}
	return val.(*mongo.Client).Database(databaseName)
}

func (eng *hybsEngine) Redis(insID string) (r RedisClient) {
	if eng.redisList == nil {
		return nil
	}
	// search redis list
	for i, loopRedis := range eng.redisList {
		if !strings.EqualFold(insID, loopRedis.config.ID) {
			continue
		}
		r = eng.redisList[i]
		break
	}
	return
}

func (eng *hybsEngine) Cache(insID string) (cache *cache.Cache) {
	if eng.cacheList == nil {
		return nil
	}
	// search cache list
	for _, loopCache := range eng.cacheList {
		// for high performance, DO NOT use strings.EqualFold
		if !(insID == loopCache.config.ID) {
			continue
		}
		cache = loopCache.Cache
		break
	}
	return
}

func (eng *hybsEngine) SqliteDo(dbID string, f func(db *gorm.DB) error) error {
	var db = eng.Sqlite(dbID)
	if db == nil {
		return fmt.Errorf("sqlite [%s] not found", dbID)
	}
	return f(db)
}

func (eng *hybsEngine) MySQLDo(dbID string, f func(db *gorm.DB) error) error {
	var db = eng.MySQL(dbID)
	if db == nil {
		return fmt.Errorf("mysql [%s] not found", dbID)
	}
	return f(db)
}

func (eng *hybsEngine) MongoDo(dbID string, f func(db *mongo.Database) error) error {
	var db = eng.Mongo(dbID)
	if db == nil {
		return fmt.Errorf("mongodb [%s] not found", dbID)
	}
	return f(db)
}

func (eng *hybsEngine) RedisDo(insID string, f func(r redis.UniversalClient) error) error {
	var r = eng.Redis(insID)
	if r == nil {
		return fmt.Errorf("redis [%s] not found", insID)
	}
	return f(r)
}

func (eng *hybsEngine) CacheDo(insID string, f func(c *cache.Cache) error) error {
	var c = eng.Cache(insID)
	if c == nil {
		return fmt.Errorf("cache [%s] not found", insID)
	}
	return f(c)
}

func (eng *hybsEngine) Stop() (err error) {
	// stop realtime services
	for _, loopService := range eng.realtimeServers {
		loopService.stop()
		fmt.Printf("realtime service [%s] stopped\n", loopService.ID)
	}
	// stop http gates
	for _, loopGate := range eng.httpGates {
		loopGate.stop()
		fmt.Printf("http(s) gateway [%s] stopped\n", loopGate.config.ID)
	}
	// stop http services
	for _, loopService := range eng.httpServers {
		loopService.stop()
		fmt.Printf("http(s) service [%s] stopped\n", loopService.httpConfig.ID)
	}
	// stop batches
	for _, loopBatch := range eng.batches {
		close(loopBatch.pulser)
	}
	// stop scheduler
	close(eng.scheduler.pulser)
	// save cache
	for _, cache := range eng.cacheList {
		if cache.config.Filepath == "" {
			continue
		}
		if err = cacheSave(cache.Cache, cache.config); err != nil {
			eng.LogErrorf("cache [%s] save file %s failed:[%s]", cache.config.ID, cache.config.Filepath, err)
		} else {
			fmt.Printf("cache [%s] saved file to %s\n", cache.config.ID, cache.config.Filepath)
		}
	}
	// stop redis connections
	for _, r := range eng.redisList {
		if err = r.Close(); err != nil {
			fmt.Printf("regis [%s] close connection error:%s", r.config.ID, err)
		}
	}
	// stop mongodb connections
	eng.mongoDBMap.Range(func(name, db interface{}) bool {
		if db != nil {
			db.(*mongo.Client).Disconnect(context.Background())
		}
		fmt.Printf("disconnected to mongodb:%s\n", name)
		return true
	})
	// stop mysql connections
	eng.mySQLMap.Range(func(name, client interface{}) bool {
		return true
	})
	// stop sqlite connections
	eng.sqlite3Map.Range(func(name, client interface{}) bool {
		return true
	})
	// stop other plugins
	fmt.Println("bye bye!")
	return err
}

func (eng *hybsEngine) LogInfof(format string, args ...interface{}) {
	eng.sysLogger.Infof(format, args...)
}

func (eng *hybsEngine) LogWarnf(format string, args ...interface{}) {
	eng.sysLogger.Warnf(format, args...)
}

func (eng *hybsEngine) LogErrorf(format string, args ...interface{}) {
	eng.sysLogger.Errorf(format, args...)
}

func (eng *hybsEngine) Now() Time {
	return Time(time.Now())
}

func (eng *hybsEngine) AppConfig() string {
	return eng.config.AppConfig
}

func (eng *hybsEngine) Context() context.Context {
	return eng.context
}

func (eng *hybsEngine) UserValue(key string) (value interface{}, ok bool) {
	return eng.userValue.Get(key)
}

func (eng *hybsEngine) SetUserValue(key string, val interface{}) {
	eng.userValue = eng.userValue.Set(key, val)
}

func (eng *hybsEngine) openCache() (err error) {
	for i, config := range eng.config.Cache {
		// store reference
		if c, err := newCache(eng.config.Cache[i]); err != nil {
			return err
		} else {
			eng.cacheList = append(eng.cacheList, c)
		}
		fmt.Printf("ready cache [%s] file=%s done\n", config.ID, config.Filepath)
	}
	return
}
func (eng *hybsEngine) dialRedis() error {
	for i, config := range eng.config.Redis {
		// store reference
		if r, err := newRedis(eng.config.Redis[i]); err != nil {
			return err
		} else if err = r.Ping(eng.context).Err(); err != nil {
			return err
		} else {
			eng.redisList = append(eng.redisList, r)
		}
		fmt.Printf("dial redis server [%s]%s/%d success\n", config.ID, config.Address, config.DB)
	}
	return nil
}
func (eng *hybsEngine) dialMongoDB() error {
	for _, config := range eng.config.MongoDB {
		var c = context.Background()
		// create client
		var newClient, err = newMongoDBClient(config)
		if err != nil {
			return fmt.Errorf("init mongodb client failed:%s", err)
		}
		// connect server
		if err = newClient.Connect(c); err != nil {
			return fmt.Errorf("connect mongodb %s failed:%s", config.ID, err)
		}
		// ping test
		if err = newClient.Ping(c, readpref.Primary()); err != nil {
			return fmt.Errorf("ping mongodb %s failed:%s", config.ID, err)
		}
		if err = newClient.Ping(c, readpref.Secondary()); err != nil {
			return fmt.Errorf("ping mongodb %s failed:%s", config.ID, err)
		}
		// save reference
		eng.mongoDBMap.Store(config.ID, newClient)
		fmt.Printf("dial mongo server [%s]%s/%s success\n", config.ID, config.Hosts, config.Database)
	}
	return nil
}
func (eng *hybsEngine) dialMySQL() error {
	for _, config := range eng.config.MySQL {
		var newClient, err = newMySQLConnection(config)
		if err != nil {
			return fmt.Errorf("dial mysql [%s] failed:%s", config.ID, err)
		}
		// ping test
		if db, err := newClient.DB(); err != nil {
			return fmt.Errorf("dial mysql [%s] failed:%s", config.ID, err)
		} else if err = db.Ping(); err != nil {
			return fmt.Errorf("dial mysql [%s] failed:%s", config.ID, err)
		}
		newClient.Logger.LogMode(logger.Warn)
		// save reference
		eng.mySQLMap.Store(config.ID, newClient)
		fmt.Printf("dial mysql server [%s]%s/%s success\n", config.ID, config.Addr, config.Database)
	}
	return nil
}
func (eng *hybsEngine) openSqlite3() error {
	for _, config := range eng.config.Sqlite3 {
		var newConnector, err = newSqlite3Connection(config)
		if err != nil {
			return fmt.Errorf("open sqlite [%s] failed:%s", config.ID, err)
		}
		// access test
		if db, err := newConnector.DB(); err != nil {
			return fmt.Errorf("open sqlite [%s] failed:%s", config.ID, err)
		} else if err = db.Ping(); err != nil {
			return fmt.Errorf("open sqlite [%s] failed:%s", config.ID, err)
		}
		newConnector.Logger.LogMode(logger.Warn)
		// save reference
		eng.sqlite3Map.Store(config.ID, newConnector)
		fmt.Printf("open sqlite db file [%s]%s success\n", config.ID, config.Filename)
	}
	return nil
}
func (eng *hybsEngine) preloadMasterData() error {
	eng.masterDataLoaders = make([]masterDataLoaderFunc, 0)
	for _, l := range defaultMasterDataLoaderList {
		if f, err := masterDataLoadFunc(l.mux, l.loader, l.args...); err != nil {
			return err
		} else {
			eng.masterDataLoaders = append(eng.masterDataLoaders, f)
		}
	}
	return nil
}
func (eng *hybsEngine) loadMasterData() (err error) {
	for _, f := range eng.masterDataLoaders {
		if err = f(eng); err != nil {
			return fmt.Errorf("load master data failed:%s", err)
		}
	}
	if len(eng.masterDataLoaders) > 0 {
		fmt.Printf("%d csv master table(s) loaded\n", len(eng.masterDataLoaders))
	}
	return
}
func (eng *hybsEngine) debugMode() bool {
	return eng.config.DebugMode
}

// StartServices initialize plugins, starts listening and network services
// applicationUp function will be run after initialized plugins
// applicationDown function will be run before unload plugins
func StartService(applicationUp func(engine Engine), applicationDown func(engine Engine)) {
	var config, err = loadEngineConfig()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if config.DebugMode && config.DisplayLogo {
		printLogo()
	}
	// init create hybsEngine
	var engine = &hybsEngine{
		config:            config,
		lbClients:         make([]*hybsBalancingClient, 0, len(config.LBClient)),
		httpGates:         make([]*hybsHttpGate, 0, len(config.HttpGate)),
		httpServers:       make([]*hybsHttpServer, 0, len(config.HttpServer)),
		realtimeServers:   make([]*hybsRealtimeServer, 0, len(config.RealtimeServer)),
		sqlite3Map:        &sync.Map{},
		mySQLMap:          &sync.Map{},
		mongoDBMap:        &sync.Map{},
		redisList:         make([]*hybsRedis, 0, len(config.Redis)),
		cacheList:         make([]*hybsCache, 0, len(config.Cache)),
		masterDataMgr:     masterDataManager{Map: &sync.Map{}},
		masterDataLoaders: make([]masterDataLoaderFunc, 0),
		context:           context.Background(),
		userValue:         make(kvPairs, 0, 0x400),
	}
	// preload master data
	if err = engine.preloadMasterData(); err != nil {
		fmt.Printf("preload master data failed:%s\n", err)
		return
	}
	// init sysLogger
	engine.sysLogger, err = newSysLogger(
		engine.config.LoggerConfig.LogFilePath,
		engine.config.LoggerConfig.InfoLogAge,
		engine.config.LoggerConfig.WarnLogAge,
		engine.config.LoggerConfig.ErrorLogAge)
	if err != nil {
		fmt.Printf("init sysLogger failed:%s\n", err)
		return
	}
	// init gameLogger
	engine.gameLogger, err = newGameLogger(
		engine.config.LoggerConfig.LogFilePath,
		engine.config.LoggerConfig.GameLogAge)
	if err != nil {
		fmt.Printf("init gameLogger failed:%s\n", err)
		return
	}
	// load master data
	if err = engine.loadMasterData(); err != nil {
		fmt.Printf("load master data failed:%s\n", err)
		return
	}
	// load local cache
	if err = engine.openCache(); err != nil {
		fmt.Printf("start local cache failed:%s\n", err)
		return
	}
	// dial redis
	if err = engine.dialRedis(); err != nil {
		fmt.Printf("dial to redis failed:%s\n", err)
		return
	}
	// dial mongodb
	if err = engine.dialMongoDB(); err != nil {
		fmt.Printf("dial to mongo failed:%s\n", err)
		return
	}
	// dial mysql
	if err = engine.dialMySQL(); err != nil {
		fmt.Printf("dial to mysql failed:%s\n", err)
		return
	}
	// open sqlite
	if err = engine.openSqlite3(); err != nil {
		fmt.Printf("open sqlite db file failed:%s\n", err)
		return
	}
	// init scheduler. Scheduler is for lite tasks, implement by time wheel
	engine.scheduler = &scheduler{server: engine}
	// register scheduler events
	for _, evt := range defaultSchedulerEvents {
		engine.scheduler.insert(Now().Add(time.Second), evt)
	}
	// create and init batch instances. Batch is for heavy tasks, no need for precise time control
	for _, batchConfig := range config.Batch {
		var newBatch = newHybsBatch(batchConfig, engine)
		engine.batches = append(engine.batches, newBatch)
	}
	// create and init http load balancing clients
	for _, clientConfig := range config.LBClient {
		var client, err = newLBClient(engine, clientConfig)
		if err != nil {
			fmt.Printf("init balancing client [%s] failed:%s\n", clientConfig.ID, err)
			return
		}
		engine.lbClients = append(engine.lbClients, client)
	}
	// create and init http gate instances
	for _, gateConfig := range config.HttpGate {
		var gate, err = newHttpGate(engine, gateConfig)
		if err != nil {
			fmt.Printf("init http gateway [%s] failed:%s\n", gateConfig.ID, err)
			return
		}
		engine.httpGates = append(engine.httpGates, gate)
	}
	// create and init http server instances
	for _, httpConfig := range config.HttpServer {
		// init create hybs http service
		var server, err = newHttpServer(engine, httpConfig)
		if err != nil {
			fmt.Printf("init http(s) service [%s] failed:%s\n", httpConfig.ID, err)
			return
		}
		// register service function
		for _, apiFuncDefine := range serviceDefineList {
			server.serviceMap[apiFuncDefine.serviceID] = apiFuncDefine.handler
		}
		// register builtin service
		server.serviceMap["Document"] = builtinServiceAPIDocument(server)
		// register builtin middleware
		server.middlewareMap["ResponseJSON"] = middlewareBuiltinResponseJSON(server)
		server.middlewareMap["Authentication"] = middlewareBuiltinAuthentication(server)
		server.middlewareMap["HttpLog"] = middlewareBuiltinHttpLog(server)
		server.middlewareMap["GameLog"] = middlewareBuiltinGameLog(server)
		server.middlewareMap["HybsLog"] = middlewareBuiltinHttpLog(server).Left(middlewareBuiltinGameLog(server))
		// register middleware
		for _, loopMiddleware := range middlewareDefineList {
			server.middlewareMap[loopMiddleware.middlewareID] = loopMiddleware.m
		}
		// register builtin middleware
		for _, c := range server.engine.config.Cache {
			server.middlewareMap["Use"+c.ID] = middlewareBuiltinCacheUse(engine, c.ID)
		}
		for _, r := range server.engine.config.Redis {
			server.middlewareMap["Use"+r.ID] = middlewareBuiltinRedisUse(engine, r.ID)
		}
		for _, db := range server.engine.config.MongoDB {
			server.middlewareMap["Use"+db.ID] = middlewareBuiltinMongoDBUse(engine, db.ID)
		}
		for _, db := range server.engine.config.MySQL {
			server.middlewareMap["Use"+db.ID] = middlewareBuiltinMySQLUse(engine, db.ID)
		}
		for _, db := range server.engine.config.Sqlite3 {
			server.middlewareMap["Use"+db.ID] = middlewareBuiltinSqliteUse(engine, db.ID)
		}
		engine.httpServers = append(engine.httpServers, server)
	}
	// create and init realtime services
	for _, realtimeConfig := range config.RealtimeServer {
		var server = newRealtimeServer(engine, realtimeConfig)
		var l rtListenerInterface = nil
		if strings.EqualFold(realtimeConfig.Protocol, "kcp") {
			if l, err = getKcpListenerFn(&realtimeConfig); err != nil {
				fmt.Printf("listen socket failed:%s\n", err)
				return
			}
			server.setListener(l)
		} else if strings.EqualFold(realtimeConfig.Protocol, "quic") {
			if l, err = getQuicListenerFn(&realtimeConfig); err != nil {
				fmt.Printf("listen socket failed:%s\n", err)
				return
			}
			server.setListener(l)
		} else {
			fmt.Printf("unknown protocol name:%s\n", realtimeConfig.Protocol)
			return
		}
		if err = server.loadControllerConfig(); err != nil {
			fmt.Printf("load realtime service [%s] controllers failed:%s\n", realtimeConfig.ID, err)
			return
		}
		if err = server.initHandlerMap(); err != nil {
			fmt.Printf("init realtime handlers failed:%s\n", err)
			return
		}
		server.sessionManager.init(server)
		server.defaultModule = newRTModule()
		engine.realtimeServers = append(engine.realtimeServers, server)
	}
	// set utils
	Now = engine.Now
	// application up
	if applicationUp != nil {
		applicationUp(engine)
	}
	// start running scheduler
	var secondPulser = Pulser(time.Second)
	go engine.scheduler.run(secondPulser)
	// start batches
	for _, batch := range engine.batches {
		go batch.start()
	}
	// start http services
	for _, server := range engine.httpServers {
		if err = server.serve(); err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("http(s) service [%s] started at %s://%s\n",
			server.httpConfig.ID, server.httpConfig.Network, server.httpConfig.Address)
	}
	// start http gates
	for _, gate := range engine.httpGates {
		if err = gate.serve(); err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("http(s) gateway [%s] started at %s://%s\n",
			gate.config.ID, gate.config.Network, gate.config.Address)
	}
	// start realtime services
	for _, server := range engine.realtimeServers {
		if err = server.serve(); err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("realtime service [%s] started at %s://%s\n", server.ID, server.Network, server.Address)
	}

	fmt.Printf("pid=%d\n", os.Getpid())
	if config.PidFilepath != "" {
		ioutil.WriteFile(config.PidFilepath, []byte(fmt.Sprintln(os.Getpid())), 0600)
	}
	// wait stop
	var osSignal = make(chan os.Signal, 1)
	signal.Notify(osSignal, syscall.SIGTERM, syscall.SIGINT)
	<-osSignal
	fmt.Printf("stopping engine... - %s\n", engine.Now().LogString())
	if applicationDown != nil {
		applicationDown(engine)
	}
	if err = engine.Stop(); err != nil {
		engine.LogErrorf("stopping engine failed:%s", err)
	}
}
