package connection_manager

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"sync"
	"time"
)

type RedisConnection struct {
	ConnectionURL string
	Username      string
	Password      string
	client        *redis.Client
	clientOptions *redis.Options
	initialized   bool
	sync.Mutex
}

func (c *RedisConnection) IsInitialized() bool {
	return c.initialized
}

func (c *RedisConnection) LoadConfig(opts *redis.Options) {
	c.clientOptions = opts
	c.initialized = true
}

// Close terminates the database connection.
func (c *RedisConnection) Close() error {
	c.Lock()
	defer c.Unlock()

	if c.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		cn := c.client.Conn(ctx)
		if err := cn.Close(); err != nil {
			return err
		}
	}

	c.client = nil

	return nil
}

func (c *RedisConnection) CreateClient(ctx context.Context) (client *redis.Client, err error) {

	if !c.IsInitialized() {
		return nil, fmt.Errorf("failed to create client: connection producer is not initialized")
	}
	if c.clientOptions == nil {
		return nil, fmt.Errorf("missing client options")
	}
	client = redis.NewClient(c.clientOptions)

	return client, nil

}

func (c *RedisConnection) VerifyConnection(ctx context.Context) error {
	client, err := c.CreateClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to verify connection: %w", err)
	}
	statusCmd := client.Ping(ctx)
	if statusCmd.Err() != nil {
		return fmt.Errorf("failed to verify connection: %w", statusCmd.Err())
	}
	c.client = client
	return nil
}

func (c *RedisConnection) GetConnection(ctx context.Context) (*redis.Client, error) {
	if !c.IsInitialized() {
		return nil, fmt.Errorf("database client is not initialized")
	}

	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.client != nil {
		if statusCmd := c.client.Ping(ctx); statusCmd.Err() == nil {
			return c.client, nil
		}
	}

	client, err := c.CreateClient(ctx)
	if err != nil {
		return nil, err
	}
	c.client = client
	return c.client, nil
}
