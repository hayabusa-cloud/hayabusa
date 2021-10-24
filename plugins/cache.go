package plugins

import (
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"
)

type CacheConfig struct {
	ID                string        `yaml:"id" required:"true"`
	Filepath          string        `yaml:"filepath" default:""`
	AutoSave          int8          `yaml:"auto_save" default:"0"`
	CreateNewFile     bool          `yaml:"create_new_file" default:"false"`
	DefaultExpiration time.Duration `yaml:"default_expiration" default:"0s"`
	CleanupInterval   time.Duration `yaml:"cleanup_interval" default:"0s"`
}

type Cache struct {
	Config *CacheConfig
	*cache.Cache
}

func NewCache(config CacheConfig) (plugin *Cache, err error) {
	var expiration, cleanup = cache.NoExpiration, cache.NoExpiration
	if config.DefaultExpiration >= time.Second {
		expiration = config.DefaultExpiration
	}
	if config.CleanupInterval >= time.Second {
		cleanup = config.CleanupInterval
	}
	var raw = cache.New(expiration, cleanup)
	plugin = &Cache{
		Config: &config,
		Cache:  raw,
	}
	if config.Filepath != "" {
		if err = raw.LoadFile(config.Filepath); err != nil {
			if config.CreateNewFile {
				return plugin, nil
			}
			return nil, fmt.Errorf("cache [%s] load from file %s error:%s", config.ID, config.Filepath, err)
		}
		fmt.Printf("cache [%s] loaded from file=%s\n", config.ID, config.Filepath)
	}
	return plugin, nil
}

func (c *Cache) Save() (err error) {
	if c.Config.Filepath == "" {
		return nil
	}
	if err = c.SaveFile(c.Config.Filepath); err != nil {
		return fmt.Errorf("cache [%s] save to file %s error:%s", c.Config.ID, c.Config.Filepath, err)
	}
	return
}
