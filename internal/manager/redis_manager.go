package manager

import (
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

type RedisClientManager struct {
	mu      sync.Mutex
	clients map[string]*redis.Client
}

func NewRedisClientManager() *RedisClientManager {
	return &RedisClientManager{
		clients: make(map[string]*redis.Client),
	}
}

func (r *RedisClientManager) key(addr, password string, db int) string {
	return fmt.Sprintf("%s|%s|%d", addr, password, db)
}

func (r *RedisClientManager) GetClient(addr, password string, db int) *redis.Client {
	key := r.key(addr, password, db)

	r.mu.Lock()
	defer r.mu.Unlock()

	if client, exists := r.clients[key]; exists {
		return client
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	r.clients[key] = client
	return client
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
