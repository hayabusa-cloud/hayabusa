package plugins

import (
	"database/sql"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Sqlite3Config struct {
	ID       string `yaml:"id" required:"true"`
	Filename string `yaml:"filename" required:"true"`
}

type Sqlite struct {
	Config *Sqlite3Config
	*gorm.DB
}

func NewSqlite3(config Sqlite3Config) (plugin *Sqlite, err error) {
	var gdb *gorm.DB = nil
	// set dialect settings
	gdb, err = gorm.Open(sqlite.Open(config.Filename), &gorm.Config{})
	if gdb == nil || err != nil {
		return nil, err
	}
	var db *sql.DB = nil
	if db, err = gdb.DB(); db == nil || err != nil {
		return nil, err
	}
	// sqlite connection pool size should be 1 fixed
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	return &Sqlite{Config: &config, DB: gdb}, err
}
