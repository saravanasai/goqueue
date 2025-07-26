package config

import (
	"errors"
	"runtime"
)

const (
	DriverMemory   = "memory"
	DriverRedis    = "redis"
	DriverDatabase = "database"
)

type DriverConfig interface {
	Type() string
}

type Config struct {
	Driver           string
	DriverConfig     DriverConfig
	MaxWorkers       int
	ConcurrencyLimit int
}

type RedisConfig struct {
	Addr     string
	Password string
	Db       int
}

func (r RedisConfig) Type() string {
	return "redis"
}

func sensibleDefaultMaxWorkers() int {

	return runtime.NumCPU() * 2
}

func sensibleDefaultConcurrencyLimit() int {
	return runtime.NumCPU() * 4
}

func NewInMemoryConfig() Config {
	return Config{
		Driver:           DriverMemory,
		DriverConfig:     nil,
		MaxWorkers:       sensibleDefaultMaxWorkers(),
		ConcurrencyLimit: sensibleDefaultConcurrencyLimit(),
	}
}

func NewRedisConfig(address string, password string, db int) Config {
	return Config{
		Driver: DriverRedis,
		DriverConfig: RedisConfig{
			Addr:     address,
			Password: password,
			Db:       db,
		},
		MaxWorkers:       sensibleDefaultMaxWorkers(),
		ConcurrencyLimit: sensibleDefaultConcurrencyLimit(),
	}
}

func (c Config) WithMaxWorkers(maxWorkers int) Config {
	c.MaxWorkers = maxWorkers
	return c
}

func (c Config) WithConcurrencyLimit(limit int) Config {
	c.ConcurrencyLimit = limit
	return c
}

func (c Config) Validate() error {

	if c.MaxWorkers <= 0 {
		return errors.New("MaxWorkers must be greater than 0")
	}

	if c.ConcurrencyLimit <= 0 {
		return errors.New("ConcurrencyLimit must be greater than 0")
	}

	switch c.Driver {
	case DriverMemory:
		return nil
	case DriverRedis:
		return nil
	default:
		return errors.New("unsupported driver: " + c.Driver)
	}
}
