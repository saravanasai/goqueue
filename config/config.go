package config

import (
	"errors"
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
	Driver       string
	DriverConfig DriverConfig
}

type RedisConfig struct {
	Addr     string
	Password string
	Db       int
}

func (r RedisConfig) Type() string {
	return "redis"
}

func NewInMemoryConfig() Config {
	return Config{
		Driver:       DriverMemory,
		DriverConfig: nil,
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
	}
}

func (c Config) Validate() error {
	switch c.Driver {
	case DriverMemory:
		return nil
	case DriverRedis:
		return nil
	default:
		return errors.New("unsupported driver: " + c.Driver)
	}
}
