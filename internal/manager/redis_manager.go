package manager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClientManager struct {
	mu              sync.Mutex
	clients         map[string]*redis.Client
	healthStatus    map[string]*atomic.Bool
	lastHealthCheck map[string]time.Time
}

func NewRedisClientManager() *RedisClientManager {
	return &RedisClientManager{
		clients:         make(map[string]*redis.Client),
		healthStatus:    make(map[string]*atomic.Bool),
		lastHealthCheck: make(map[string]time.Time),
	}
}

func (r *RedisClientManager) Key(addr string, password string, db int) string {
	return fmt.Sprintf("%s|%s|%d", addr, password, db)
}

func (r *RedisClientManager) GetClient(addr, password string, db int) *redis.Client {
	key := r.Key(addr, password, db)

	r.mu.Lock()
	defer r.mu.Unlock()

	if client, exists := r.clients[key]; exists {
		return client
	}

	client := redis.NewClient(&redis.Options{
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
	// Create a copy of all clients to check while minimizing lock time
	clientsToCheck := make(map[string]*redis.Client)

	r.mu.Lock()
	for key, client := range r.clients {
		clientsToCheck[key] = client
		// Initialize health status if it doesn't exist
		if _, exists := r.healthStatus[key]; !exists {
			r.healthStatus[key] = &atomic.Bool{}
			r.healthStatus[key].Store(true) // Assume healthy initially
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

		if !isHealthy {
			fmt.Printf("Redis client %s is unhealthy: %v\n", key, err)
		}

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
