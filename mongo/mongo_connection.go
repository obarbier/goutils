package mongo

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"sync"
	"time"
)

type Connection struct {
	ConnectionURL string
	Username      string
	Password      string
	client        *mongo.Client
	clientOptions *options.ClientOptions
	Initialized   bool
	sync.Mutex
}

func (c *Connection) LoadConfig(opts *options.ClientOptions) {
	c.clientOptions = opts
}
func (c *Connection) MakeClientOpts() (*options.ClientOptions, error) {
	opts := options.MergeClientOptions()
	return opts, nil
}
func (c *Connection) CreateClient(ctx context.Context) (client *mongo.Client, err error) {
	if !c.Initialized {
		return nil, fmt.Errorf("failed to create client: connection producer is not initialized")
	}
	if c.clientOptions == nil {
		return nil, fmt.Errorf("missing client options")
	}
	client, err = mongo.Connect(ctx, options.MergeClientOptions(options.Client().ApplyURI(c.ConnectionURL), c.clientOptions))
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Close terminates the database connection.
func (c *Connection) Close() error {
	c.Lock()
	defer c.Unlock()

	if c.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		if err := c.client.Disconnect(ctx); err != nil {
			return err
		}
	}

	c.client = nil

	return nil
}

func (c *Connection) VerifyConnection(ctx context.Context) error {
	client, err := c.CreateClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to verify connection: %w", err)
	}
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		_ = client.Disconnect(ctx) // Try to prevent any sort of resource leak
		return fmt.Errorf("failed to verify connection: %w", err)
	}
	c.client = client
	return nil
}

func (c *Connection) Connection(ctx context.Context) (*mongo.Client, error) {
	if !c.Initialized {
		return nil, fmt.Errorf("database client is not initialized")
	}

	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.client != nil {
		if err := c.client.Ping(ctx, readpref.Primary()); err == nil {
			return c.client, nil
		}
		// Ignore error on purpose since we want to re-create a session
		_ = c.client.Disconnect(ctx)
	}

	client, err := c.CreateClient(ctx)
	if err != nil {
		return nil, err
	}
	c.client = client
	return c.client, nil
}
