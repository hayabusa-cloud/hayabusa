package hybs

import (
	"fmt"
	"github.com/patrickmn/go-cache"
	"time"
)

type cacheConfig struct {
	ID                string        `yaml:"id" required:"true"`
	Filepath          string        `yaml:"filepath" default:""`
	AutoSave          int8          `yaml:"auto_save" default:"0"`
	CreateNewFile     bool          `yaml:"create_new_file" default:"false"`
	DefaultExpiration time.Duration `yaml:"default_expiration" default:"0s"`
	CleanupInterval   time.Duration `yaml:"cleanup_interval" default:"0s"`
}

type hybsCache struct {
	config *cacheConfig
	*cache.Cache
}

func newCache(config cacheConfig) (c *hybsCache, err error) {
	var expiration, cleanup = cache.NoExpiration, cache.NoExpiration
	if config.DefaultExpiration >= time.Second {
		expiration = config.DefaultExpiration
	}
	if config.CleanupInterval >= time.Second {
		cleanup = config.CleanupInterval
	}
	var raw = cache.New(expiration, cleanup)
	c = &hybsCache{
		config: &config,
		Cache:  raw,
	}
	if config.Filepath != "" {
		if err = raw.LoadFile(config.Filepath); err != nil {
			if config.CreateNewFile {
				return c, nil
			}
			return nil, fmt.Errorf("cache [%s] load from file %s failed:%s", config.ID, config.Filepath, err)
		}
		fmt.Printf("cache [%s] loaded from file=%s\n", config.ID, config.Filepath)
	}
	return c, nil
}

func cacheSave(c *cache.Cache, config *cacheConfig) (err error) {
	if config.Filepath == "" {
		return nil
	}
	if err = c.SaveFile(config.Filepath); err != nil {
		return fmt.Errorf("cache [%s] save to file %s failed:%s", config.ID, config.Filepath, err)
	}
	return
}
