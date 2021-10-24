package plugins

import (
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBConfig struct {
	ID                      string            `yaml:"id" required:"true"`
	Hosts                   []string          `yaml:"hosts" required:"true"`
	ReplicaSet              string            `yaml:"replica_set" default:""`
	Database                string            `yaml:"database" required:"true"`
	AuthMechanism           string            `yaml:"auth_mechanism" default:""`
	AuthMechanismProperties map[string]string `yaml:"auth_mechanism_properties"`
	AuthSource              string            `yaml:"auth_source" default:""`
	Username                string            `yaml:"username" required:"true"`
	Password                string            `yaml:"password" required:"true"`
	PasswordSet             bool              `yaml:"password_set"`
	MinPoolSize             uint64            `yaml:"min_pool_size" default:"0"`
	MaxPoolSize             uint64            `yaml:"max_pool_size" default:"0"`
	MaxConnIdleTime         time.Duration     `yaml:"max_conn_idle_time" default:"0m"`
	ConnectTimeout          time.Duration     `yaml:"connect_timeout" default:"0s"`
	ServerSelectionTimeout  time.Duration     `yaml:"server_selection_timeout" default:"0s"`
	SocketTimeout           time.Duration     `yaml:"socket_timeout" default:"0s"`
	CompressionLevel        int               `yaml:"compression_level" default:"0"`

	// automatically abort transaction on errors
	AutoAbortTransaction bool `yaml:"auto_abort_transaction" default:"false"`
	// automatically commit transaction if return success
	AutoCommitTransaction bool `yaml:"auto_commit_transaction" default:"true"`
}

type Mongo struct {
	Config *MongoDBConfig
	*mongo.Database
}

func NewMongo(config MongoDBConfig) (plugin *Mongo, err error) {
	var newOption = options.Client().SetAppName("hayabusa")
	// set pool size config
	newOption.SetMinPoolSize(uint64(runtime.NumCPU() + 1))
	if config.MinPoolSize > 0 {
		newOption.SetMinPoolSize(config.MinPoolSize)
	}
	newOption.SetMaxPoolSize(uint64(runtime.NumCPU()*4 + 16))
	if config.MaxPoolSize > 0 {
		newOption.SetMaxPoolSize(config.MaxPoolSize)
	}
	// set timeout
	newOption.SetMaxConnIdleTime(config.MaxConnIdleTime)
	newOption.SetConnectTimeout(config.ConnectTimeout)
	newOption.SetServerSelectionTimeout(config.ServerSelectionTimeout)
	newOption.SetSocketTimeout(config.SocketTimeout)
	// set compression level
	if config.CompressionLevel > 0 {
		newOption.SetCompressors([]string{"zstd"})
		newOption.SetZstdLevel(config.CompressionLevel)
	}
	// set connection information
	newOption.SetHosts(config.Hosts)
	newOption.SetReplicaSet(config.ReplicaSet)
	var authSource = config.AuthSource
	if authSource == "" {
		authSource = config.Database
	}
	newOption.SetAuth(options.Credential{
		AuthMechanism:           config.AuthMechanism,
		AuthMechanismProperties: config.AuthMechanismProperties,
		AuthSource:              authSource,
		Username:                config.Username,
		Password:                config.Password,
		PasswordSet:             config.PasswordSet,
	})
	// create new client
	var client *mongo.Client = nil
	if client, err = mongo.NewClient(newOption); err != nil {
		return nil, err
	}
	return &Mongo{Database: client.Database(config.Database), Config: &config}, nil
}
