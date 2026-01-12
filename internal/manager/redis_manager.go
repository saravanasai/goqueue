package manager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/danish-a1/goqueue/internal/logger"
	"github.com/redis/go-redis/v9"
)

type RedisConnectionInfo struct {
	addr     string
	password string
	db       int
}

type RedisClientManager struct {
	mu              sync.Mutex
	connectionInfo  RedisConnectionInfo
	clients         map[string]*redis.Client
	healthStatus    map[string]*atomic.Bool
	lastHealthCheck map[string]time.Time
	logger          logger.Logger
}

func NewRedisClientManager(addr string, password string, db int, logger logger.Logger) *RedisClientManager {
	return &RedisClientManager{
		connectionInfo: RedisConnectionInfo{
			addr:     addr,
			password: password,
			db:       db,
		},
		clients:         make(map[string]*redis.Client),
		healthStatus:    make(map[string]*atomic.Bool),
		lastHealthCheck: make(map[string]time.Time),
		logger:          logger,
	}
}

func (r *RedisClientManager) Key(addr string, db int) string {
	// Only use address and DB for key, never password
	return fmt.Sprintf("%s|%d", addr, db)
}

func (r *RedisClientManager) GetClient(addr, password string, db int) *redis.Client {
	key := r.Key(addr, db)

	r.mu.Lock()
	defer r.mu.Unlock()

	if client, exists := r.clients[key]; exists {
		return client
	}

	client := r.newRedisClient(addr, password, db)
	r.clients[key] = client
	return client
}

func (r *RedisClientManager) StartPeriodicHealthCheck(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.checkAllClientsHealth(ctx)
			}
		}
	}()
}

func (r *RedisClientManager) checkAllClientsHealth(ctx context.Context) {

	clientsToCheck := make(map[string]*redis.Client)

	r.mu.Lock()
	for key, client := range r.clients {
		clientsToCheck[key] = client
		if _, exists := r.healthStatus[key]; !exists {
			r.healthStatus[key] = &atomic.Bool{}
			r.healthStatus[key].Store(true)
		}
	}
	r.mu.Unlock()

	// Check each client without holding the lock
	for key, client := range clientsToCheck {
		// Create per-client context with short timeout
		pingCtx, pingCancel := context.WithTimeout(ctx, 500*time.Millisecond)

		// Perform the ping directly in this function
		_, err := client.Ping(pingCtx).Result()
		isHealthy := err == nil

		// Update health status
		r.mu.Lock()
		r.lastHealthCheck[key] = time.Now()
		if status, exists := r.healthStatus[key]; exists {
			status.Store(isHealthy)
		}
		r.mu.Unlock()

		r.mu.Lock()
		if !isHealthy {
			r.logger.Error("Redis client is unhealthy, reconnecting...", "key", key, "error", err)
			client.Close()
			// Recreate the client
			addr := r.connectionInfo.addr
			db := r.connectionInfo.db
			password := r.connectionInfo.password

			newClient := r.newRedisClient(addr, password, db)
			r.clients[key] = newClient
			r.healthStatus[key].Store(true)
		}

		r.mu.Unlock()

		pingCancel()
	}
}

func (r *RedisClientManager) IsHealthy(key string) bool {
	r.mu.Lock()
	status, exists := r.healthStatus[key]
	r.mu.Unlock()

	if !exists {
		return true // Assume healthy if no status yet
	}

	return status.Load()
}

func (r *RedisClientManager) CloseAll() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, client := range r.clients {
		_ = client.Close()
		delete(r.clients, key)
	}
	return nil
}

func (r *RedisClientManager) newRedisClient(addr, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:            addr,
		Password:        password,
		DB:              db,
		PoolSize:        30,
		MinIdleConns:    10,
		PoolTimeout:     4 * time.Second,
		DialTimeout:     10 * time.Second,
		ReadTimeout:     10 * time.Second,
		WriteTimeout:    10 * time.Second,
		MaxRetries:      3,
		MinRetryBackoff: 8 * time.Millisecond,
		MaxRetryBackoff: 512 * time.Millisecond,
	})
}
