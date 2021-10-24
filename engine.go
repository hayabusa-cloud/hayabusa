package hayabusa

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hayabusa-cloud/hayabusa/batch"
	"github.com/hayabusa-cloud/hayabusa/plugins"
	"github.com/hayabusa-cloud/hayabusa/realtime"
	"github.com/hayabusa-cloud/hayabusa/utils"
	"github.com/hayabusa-cloud/hayabusa/web"
	"github.com/lucas-clemente/quic-go"
	"github.com/sirupsen/logrus"
	"github.com/xtaci/kcp-go"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"gorm.io/gorm/logger"
)

type engine struct {
	// config
	config *engineConfig
	// services
	batches         []service // batch and timer events
	httpGates       []service // basic proxy server
	httpServers     []service // fasthttp server or http3 server
	realtimeServers []service // udp socket server
	// plugins
	lbClients  []*plugins.LBClient // balancing clients
	cacheList  []*plugins.Cache    // DO NOT use map
	redisList  []*plugins.Redis    // DO NOT use map
	mongoDBMap *sync.Map           // id->*plugins.Mongo
	mySQLMap   *sync.Map           // id->*plugins.MySQL
	sqlite3Map *sync.Map           // id->*plugins.Sqlite
	masterData plugins.MasterDataManager
	sysLogger  plugins.Logger
	gameLogger plugins.Logger
	scheduler  scheduler
	// context
	context    context.Context
	userValue  utils.KVPairs
	timeOffset time.Duration
}

func (eng *engine) AppName() string {
	return eng.config.AppName
}

func (eng *engine) Env() string {
	return eng.config.Env
}

func (eng *engine) Version() string {
	return eng.config.Version
}

func (eng *engine) Sqlite(id string) (plugin *plugins.Sqlite) {
	if eng.sqlite3Map == nil {
		return nil
	}
	var val, ok = eng.sqlite3Map.Load(id)
	if !ok {
		return nil
	}
	return val.(*plugins.Sqlite)
}

func (eng *engine) MySQL(id string) (plugin *plugins.MySQL) {
	if eng.mySQLMap == nil {
		return nil
	}
	var val, ok = eng.mySQLMap.Load(id)
	if !ok {
		return nil
	}
	return val.(*plugins.MySQL)
}

func (eng *engine) Mongo(id string) (plugin *plugins.Mongo) {
	if eng.mongoDBMap == nil {
		return nil
	}
	// search database
	var databaseName = ""
	for _, dbConfig := range eng.config.MongoDB {
		if dbConfig.ID == id {
			databaseName = dbConfig.Database
			break
		}
	}
	if databaseName == "" {
		return nil
	}
	var val, ok = eng.mongoDBMap.Load(id)
	if !ok {
		return nil
	}
	return val.(*plugins.Mongo)
}

func (eng *engine) Redis(id string) (plugin *plugins.Redis) {
	if eng.redisList == nil {
		return nil
	}
	// search redis list
	for i, loopRedis := range eng.redisList {
		if !strings.EqualFold(id, loopRedis.Config.ID) {
			continue
		}
		plugin = eng.redisList[i]
		break
	}
	return
}

func (eng *engine) Cache(id string) (plugin *plugins.Cache) {
	if eng.cacheList == nil {
		return nil
	}
	// search cache list
	for _, loopCache := range eng.cacheList {
		// DO NOT use strings.EqualFold
		if !(id == loopCache.Config.ID) {
			continue
		}
		plugin = loopCache
		break
	}
	return
}

func (eng *engine) LBClient(id string) (plugin *plugins.LBClient) {
	if eng.lbClients == nil {
		return nil
	}
	// search lbclients list
	for _, loopClient := range eng.lbClients {
		if !(loopClient.Config.ID == id) {
			continue
		}
		plugin = loopClient
		break
	}
	return
}

func (eng *engine) MasterData() plugins.MasterDataManager {
	return eng.masterData
}

func (eng *engine) Printf(format string, args ...interface{}) {
	eng.sysLogger.Printf(format, args...)
}
func (eng *engine) Infof(format string, args ...interface{}) {
	eng.sysLogger.Infof(format, args...)
}

func (eng *engine) Warnf(format string, args ...interface{}) {
	eng.sysLogger.Warnf(format, args...)
}

func (eng *engine) Errorf(format string, args ...interface{}) {
	eng.sysLogger.Errorf(format, args...)
}
func (eng *engine) Fatalf(format string, args ...interface{}) {
	eng.sysLogger.Errorf(format, args...)
	if eng.DebugMode() {
		panic(fmt.Errorf(format, args...))
	}
}
func (eng *engine) WithField(key string, value interface{}) *logrus.Entry {
	return eng.sysLogger.WithField(key, value)
}

func (eng *engine) Now() time.Time {
	return time.Now().Add(eng.timeOffset)
}

func (eng *engine) AppConfig() string {
	return eng.config.AppConfig
}

func (eng *engine) Context() context.Context {
	return eng.context
}

func (eng *engine) UserValue(key string) (value interface{}, ok bool) {
	return eng.userValue.Get(key)
}

func (eng *engine) SetUserValue(key string, val interface{}) {
	eng.userValue = eng.userValue.Set(key, val)
}

func (eng *engine) initialCachePlugins() (err error) {
	for i, config := range eng.config.Cache {
		if plugin, err := plugins.NewCache(eng.config.Cache[i]); err != nil {
			return err
		} else {
			eng.cacheList = append(eng.cacheList, plugin)
		}
		fmt.Printf("initial cache [%s] file=%s done\n", config.ID, config.Filepath)
	}
	return
}
func (eng *engine) initialRedisPlugins() error {
	for i, config := range eng.config.Redis {
		if plugin, err := plugins.NewRedis(eng.config.Redis[i]); err != nil {
			return err
		} else if err = plugin.Ping(eng.context).Err(); err != nil {
			return err
		} else {
			eng.redisList = append(eng.redisList, plugin)
		}
		fmt.Printf("initial redis [%s]%s/%d success\n", config.ID, config.Address, config.DB)
	}
	return nil
}
func (eng *engine) initialMongoPlugins() error {
	for _, config := range eng.config.MongoDB {
		var ctx = context.Background()
		var plugin, err = plugins.NewMongo(config)
		if err != nil {
			return fmt.Errorf("create mongodb plugin error:%s", err)
		}
		if err = plugin.Client().Connect(ctx); err != nil {
			return fmt.Errorf("connect mongodb %s error:%s", config.ID, err)
		}
		// do ping test at this timing
		if err = plugin.Client().Ping(ctx, readpref.Primary()); err != nil {
			return fmt.Errorf("ping mongodb %s error:%s", config.ID, err)
		}
		if err = plugin.Client().Ping(ctx, readpref.Secondary()); err != nil {
			return fmt.Errorf("ping mongodb %s error:%s", config.ID, err)
		}
		eng.mongoDBMap.Store(config.ID, plugin)
		fmt.Printf("initial mongo [%s]%s/%s success\n", config.ID, config.Hosts, config.Database)
	}
	return nil
}
func (eng *engine) initialMySQLPlugins() error {
	for _, config := range eng.config.MySQL {
		var plugin, err = plugins.NewMySQL(config)
		if err != nil {
			return fmt.Errorf("create mysql plugin [%s] error:%s", config.ID, err)
		}
		// do ping test at this timing
		if db, err := plugin.DB.DB(); err != nil {
			return fmt.Errorf("mysql [%s] error:%s", config.ID, err)
		} else if err = db.Ping(); err != nil {
			return fmt.Errorf("ping mysql [%s] error:%s", config.ID, err)
		}
		plugin.Logger.LogMode(logger.Warn)
		eng.mySQLMap.Store(config.ID, plugin)
		fmt.Printf("initial mysql [%s]%s/%s success\n", config.ID, config.Addr, config.Database)
	}
	return nil
}
func (eng *engine) initialSqlitePlugins() error {
	for _, config := range eng.config.Sqlite3 {
		var plugin, err = plugins.NewSqlite3(config)
		if err != nil {
			return fmt.Errorf("create sqlite plugin [%s] error:%s", config.ID, err)
		}
		if db, err := plugin.DB.DB(); err != nil {
			return fmt.Errorf("sqlite [%s] error:%s", config.ID, err)
		} else if err = db.Ping(); err != nil {
			return fmt.Errorf("open sqlite [%s] error:%s", config.ID, err)
		}
		plugin.Logger.LogMode(logger.Warn)
		eng.sqlite3Map.Store(config.ID, plugin)
		fmt.Printf("initial sqlite [%s]%s success\n", config.ID, config.Filename)
	}
	return nil
}
func (eng *engine) DebugMode() bool {
	return eng.config.DebugMode
}

// StartService initialize plugins, starts listening and network services
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
	// init create eng
	var eng = &engine{
		config:          config,
		sqlite3Map:      &sync.Map{},
		mySQLMap:        &sync.Map{},
		mongoDBMap:      &sync.Map{},
		redisList:       make([]*plugins.Redis, 0, len(config.Redis)),
		cacheList:       make([]*plugins.Cache, 0, len(config.Cache)),
		lbClients:       make([]*plugins.LBClient, 0, len(config.LBClient)),
		httpGates:       make([]service, 0, len(config.HttpGate)),
		httpServers:     make([]service, 0, len(config.HttpServer)),
		realtimeServers: make([]service, 0, len(config.RealtimeServer)),
		context:         context.Background(),
		userValue:       make(utils.KVPairs, 0, 0x400),
	}

	// initial plugins and services
	eng.prepareStartService()

	// application up should be called after all plugins initialized
	if applicationUp != nil {
		applicationUp(eng)
	}

	go eng.scheduler.Start(eng.context) // scheduler.Start() blocks until progress exited
	for _, b := range eng.batches {
		var localBatch = b
		go func() {
			// batch.Serve() blocks() until progress exited
			if err = localBatch.Serve(eng.context); err != nil {
				println(err.Error())
				eng.Errorf("start batch [%s] failed: %s", localBatch.ID(), err)
			}
		}()
	}
	// start web API servers
	for _, server := range eng.httpServers {
		// server.Serve() is non-blocking
		if err = server.Serve(eng.context); err != nil {
			println(err.Error())
			eng.Errorf("start server [%s] failed: %s", server.ID(), err)
			return
		}
	}
	// start web API gates
	for _, gate := range eng.httpGates {
		// gate.Serve() is non-blocking
		if err = gate.Serve(eng.context); err != nil {
			println(err.Error())
			eng.Errorf("start gate [%s] failed: %s", gate.ID(), err)
			return
		}
	}
	// start realtime servers
	for _, server := range eng.realtimeServers {
		// server.Serve() is non-blocking
		if err = server.Serve(eng.context); err != nil {
			println(err.Error())
			eng.Errorf("start realtime server [%s] failed: %s", server.ID(), err)
			return
		}
	}

	fmt.Printf("pid=%d\n", os.Getpid())
	if config.PidFilepath != "" {
		if err = ioutil.WriteFile(config.PidFilepath, []byte(fmt.Sprintln(os.Getpid())), 0600); err != nil {
			println(err.Error())
		}
	}

	// wait stop
	var osSignal = make(chan os.Signal, 1)
	signal.Notify(osSignal, syscall.SIGTERM)
	<-osSignal
	fmt.Printf("stopping engine...  %s\n", eng.Now())

	// applicationDown should be called before unloading plugins
	if applicationDown != nil {
		applicationDown(eng)
	}

	if err = eng.Stop(); err != nil {
		eng.Errorf("stopping engine error:%s", err)
	}
}

func (eng *engine) prepareStartService() (err error) {
	eng.sysLogger, err = plugins.NewLogger(eng.config.SysLogConfig)
	if err != nil {
		fmt.Printf("create new system logger error:%s\n", err)
		return
	}
	eng.gameLogger, err = plugins.NewLogger(eng.config.GameLogConfig)
	if err != nil {
		fmt.Printf("create new game logger error:%s\n", err)
		return
	}

	var csvManager plugins.MasterDataManager = nil
	if csvManager, err = plugins.NewCsvManager(); err != nil {
		fmt.Printf("create new csv manager error:%s", err)
		return
	}
	eng.masterData = csvManager
	if err = csvManager.(*plugins.CsvManager).Load(eng.config.CsvFilepath, eng); err != nil {
		fmt.Printf("load master data error:%s", err)
		return
	}

	if err = eng.initialCachePlugins(); err != nil {
		fmt.Printf("initial cache plugins error:%s\n", err)
		return
	}

	if err = eng.initialRedisPlugins(); err != nil {
		fmt.Printf("initial redis plugins error:%s\n", err)
		return
	}

	if err = eng.initialMongoPlugins(); err != nil {
		fmt.Printf("initial mongo plugins error:%s\n", err)
		return
	}

	if err = eng.initialMySQLPlugins(); err != nil {
		fmt.Printf("initial mysql plugins error:%s\n", err)
		return
	}

	if err = eng.initialSqlitePlugins(); err != nil {
		fmt.Printf("initial sqlite3 plugins error:%s\n", err)
		return
	}

	// scheduler is designed for lite tasks, implemented by a single time wheel
	eng.scheduler = plugins.DefaultScheduler(eng)
	// Batch is for heavier tasks which is no need to precise time control
	for _, batchConfig := range eng.config.Batch {
		var newBatch = batch.NewBatch(eng, batchConfig)
		eng.batches = append(eng.batches, newBatch)
	}

	for _, clientConfig := range eng.config.LBClient {
		var client *plugins.LBClient = nil
		client, err = plugins.NewLBClient(eng.sysLogger, clientConfig)
		if err != nil {
			fmt.Printf("create new balancing client [%s] error:%s\n", clientConfig.ID, err)
			return
		}
		eng.lbClients = append(eng.lbClients, client)
	}

	for _, gateConfig := range eng.config.HttpGate {
		var gate *web.Gate = nil
		gate, err = web.NewGate(eng, gateConfig)
		if err != nil {
			fmt.Printf("create new web gate [%s] error:%s\n", gateConfig.ID, err)
			return
		}
		eng.httpGates = append(eng.httpGates, gate)
	}

	for _, serverConfig := range eng.config.HttpServer {
		var server *web.Server = nil
		server, err = web.NewServer(eng, serverConfig)
		if err != nil {
			fmt.Printf("create new web server [%s] error:%s\n", serverConfig.ID, err)
			return
		}
		// set logger
		server.SetSysLogger(eng.sysLogger).SetGameLogger(eng.gameLogger)
		// register plugin middlewares
		for _, plugin := range eng.config.Cache {
			server.RegisterPluginCacheMiddleware(plugin.ID)
		}
		for _, plugin := range eng.config.Redis {
			server.RegisterPluginRedisMiddleware(plugin.ID)
		}
		for _, plugin := range eng.config.MongoDB {
			server.RegisterPluginMongoMiddleware(plugin.ID)
		}
		for _, plugin := range eng.config.MySQL {
			server.RegisterPluginMySQLMiddleware(plugin.ID)
		}
		for _, plugin := range eng.config.Sqlite3 {
			server.RegisterPluginSqliteMiddleware(plugin.ID)
		}
		eng.httpServers = append(eng.httpServers, server)
	}
	// create and init realtime services
	for _, realtimeConfig := range eng.config.RealtimeServer {
		var (
			server   realtime.Server   = nil
			listener realtime.Listener = nil
		)
		if strings.EqualFold(realtimeConfig.Protocol, "kcp") {
			l, _ := kcp.Listen(realtimeConfig.Address)
			listener = &realtime.KcpListener{Listener: l.(*kcp.Listener), Config: &realtimeConfig}
		} else if strings.EqualFold(realtimeConfig.Protocol, "quic") {
			l, _ := quic.ListenAddr(realtimeConfig.Address, nil, nil)
			listener = &realtime.QuicListener{Listener: l}
		} else {
			fmt.Printf("unimplemented realtime protocol %s", realtimeConfig.Protocol)
			return
		}
		if server, err = realtime.NewServer(eng, listener, realtimeConfig); err != nil {
			fmt.Printf("create new realtime server [%s] error:%s", realtimeConfig.ID, err)
			return
		}
		eng.realtimeServers = append(eng.realtimeServers, server.(service))
	}
	return
}

func (eng *engine) Stop() (err error) {
	// stop services -> unload plugins -> close loggers
	for _, loopServer := range eng.realtimeServers {
		loopServer.Stop()
		fmt.Printf("realtime server [%s] stopped\n", loopServer.ID())
	}
	for _, loopGate := range eng.httpGates {
		loopGate.Stop()
		fmt.Printf("web API gate [%s] stopped\n", loopGate.ID())
	}
	for _, loopServer := range eng.httpServers {
		loopServer.Stop()
		fmt.Printf("web service [%s] stopped\n", loopServer.ID())
	}
	for _, loopBatch := range eng.batches {
		loopBatch.Stop()
		fmt.Printf("batch [%s] stopped\n", loopBatch.ID())
	}
	eng.scheduler.Stop()

	for _, plugin := range eng.cacheList {
		if err = plugin.Save(); err != nil {
			eng.Errorf("plugin cache [%s] saves file %s error:[%s]", plugin.Config.ID, plugin.Config.Filepath, err)
		} else {
			fmt.Printf("plugin cache [%s] saved file to %s\n", plugin.Config.ID, plugin.Config.Filepath)
		}
	}
	for _, plugin := range eng.redisList {
		if err = plugin.Close(); err != nil {
			fmt.Printf("plugin redis [%s] close connection error:%s", plugin.Config.ID, err)
		}
	}
	eng.mongoDBMap.Range(func(name, plugin interface{}) bool {
		plugin.(*plugins.Mongo).Client().Disconnect(eng.context)
		fmt.Printf("mongodb [%s] disconnected\n", name)
		return true
	})
	eng.mySQLMap.Range(func(name, plugin interface{}) bool {
		return true
	})
	eng.sqlite3Map.Range(func(name, plugin interface{}) bool {
		return true
	})
	fmt.Println("all plugin(s) unloaded")

	if err = eng.gameLogger.(*logrus.Entry).Writer().Close(); err != nil {
		eng.Warnf("close game logger error:%s", err)
	}
	if err = eng.sysLogger.(*logrus.Entry).Writer().Close(); err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println("see you!\nsupport: git@hybscloud.com")
	return err
}
