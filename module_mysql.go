package hybs

import (
	"database/sql"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"runtime"
	"time"
)

type mySQLConfig struct {
	ID              string        `yaml:"id" required:"true"`
	Addr            string        `yaml:"addr" required:"true"`
	Database        string        `yaml:"database" required:"true"`
	Username        string        `yaml:"username" required:"true"`
	Password        string        `yaml:"password" required:"true"`
	Charset         string        `yaml:"charset" default:"utf8"`
	ParseTime       string        `yaml:"parse_time" default:"True"`
	Loc             string        `yaml:"loc" default:"Local"`
	MaxIdleConns    int           `yaml:"max_idle_conns" default:"0"`
	MaxOpenConns    int           `yaml:"max_open_conns" default:"0"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time" default:"5m"`
	ConnMaxLifeTime time.Duration `yaml:"conn_max_life_time" default:"0"`

	// automatically start transaction before API be processed
	AutoWithTransaction bool `yaml:"auto_with_transaction" default:"false"`
	// automatically rollback transaction on errors
	AutoRollbackTransaction bool `yaml:"auto_rollback_transaction" default:"false"`
	// automatically commit transaction if return success
	AutoCommitTransaction bool `yaml:"auto_commit_transaction" default:"true"`
}

func newMySQLConnection(config mySQLConfig) (gdb *gorm.DB, err error) {
	// set dialect settings
	var dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s&parseTime=%s&loc=%s",
		config.Username,
		config.Password,
		config.Addr,
		config.Database,
		config.Charset,
		config.ParseTime,
		config.Loc)
	gdb, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if gdb == nil || err != nil {
		return nil, err
	}
	var db *sql.DB = nil
	if db, err = gdb.DB(); db == nil || err != nil {
		return nil, err
	}
	// connection pool settings
	if config.MaxIdleConns > 0 {
		db.SetMaxIdleConns(config.MaxIdleConns)
	} else {
		// default connection pool size
		db.SetMaxIdleConns(runtime.NumCPU())
	}
	if config.MaxOpenConns > 0 {
		db.SetMaxOpenConns(config.MaxOpenConns)
	}
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)
	db.SetConnMaxLifetime(config.ConnMaxLifeTime)
	return gdb, err
}
