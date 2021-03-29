package hybs

import (
	"database/sql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type sqlite3Config struct {
	ID       string `yaml:"id" required:"true"`
	Filename string `yaml:"filename" required:"true"`
}

func newSqlite3Connection(config sqlite3Config) (gdb *gorm.DB, err error) {
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
	return gdb, err
}
