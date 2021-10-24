package hayabusa

import (
	"flag"
	"fmt"

	"github.com/hayabusa-cloud/hayabusa/batch"
	"github.com/hayabusa-cloud/hayabusa/plugins"
	"github.com/hayabusa-cloud/hayabusa/realtime"
	"github.com/hayabusa-cloud/hayabusa/web"
	"github.com/jinzhu/configor"
)

type engineConfig struct {
	AppName        string                   `yaml:"app_name" default:""`
	Env            string                   `yaml:"env" default:""`
	Version        string                   `yaml:"version" default:""`
	PidFilepath    string                   `yaml:"pid_filepath" default:""`
	AppConfig      string                   `yaml:"app_config" default:"config/application.yml"`
	DebugMode      bool                     `yaml:"debug_mode" default:"false"`
	DisplayLogo    bool                     `yaml:"display_logo" default:"true"`
	SysLogConfig   plugins.LoggerConfig     `yaml:"sys_log"`
	GameLogConfig  plugins.LoggerConfig     `yaml:"game_log"`
	CsvFilepath    string                   `yaml:"csv_filepath" default:"csv/"`
	Cache          []plugins.CacheConfig    `yaml:"cache"`
	Redis          []plugins.RedisConfig    `yaml:"redis"`
	MongoDB        []plugins.MongoDBConfig  `yaml:"mongodb"`
	MySQL          []plugins.MySQLConfig    `yaml:"mysql"`
	Sqlite3        []plugins.Sqlite3Config  `yaml:"sqlite"`
	LBClient       []plugins.LBClientConfig `yaml:"lb_client"`
	HttpGate       []web.GateConfig         `yaml:"web_gate"`
	HttpServer     []web.ServerConfig       `yaml:"web_server"`
	RealtimeServer []realtime.ServerConfig  `yaml:"realtime_server"`
	Batch          []batch.Config           `yaml:"batch"`
}

func loadEngineConfig() (config *engineConfig, err error) {
	config = &engineConfig{}
	// load config files
	var configFile = flag.String("f", "./config/default.yml", "filepath of config files")
	flag.Parse()
	if configFile == nil {
		panic(fmt.Errorf("-f flag specifies config filename"))
	}
	var configLoader = configor.New(&configor.Config{
		AutoReload: false,
	})
	if err = configLoader.Load(config, *configFile); err != nil {
		return nil, fmt.Errorf("load config file error:%s", err)
	}
	return config, nil
}
