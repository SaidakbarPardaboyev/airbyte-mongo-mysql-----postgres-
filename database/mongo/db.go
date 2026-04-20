package mongodatabase

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoConfig struct {
	ConnectionString string
	DatabaseName     string
	ConnectTimeout   time.Duration
	PingTimeout      time.Duration
	MinPoolSize      uint64
	MaxPoolSize      uint64
}

func (c *MongoConfig) setDefaults() {
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	if c.PingTimeout == 0 {
		c.PingTimeout = 5 * time.Second
	}
	if c.MaxPoolSize == 0 {
		c.MaxPoolSize = 10
	}
	if c.MinPoolSize == 0 {
		c.MinPoolSize = 1
	}
}

type Database interface {
	Disconnect() error
	GetClient() *mongo.Client
}

type mongoDatabase struct {
	client *mongo.Client
}

func (m *mongoDatabase) Disconnect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return m.client.Disconnect(ctx)
}

func (m *mongoDatabase) GetClient() *mongo.Client {
	return m.client
}

func NewDatabase(cfg MongoConfig) (Database, error) {
	cfg.setDefaults()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	opts := options.Client().
		ApplyURI(cfg.ConnectionString).
		SetMinPoolSize(cfg.MinPoolSize).
		SetMaxPoolSize(cfg.MaxPoolSize).
		SetConnectTimeout(cfg.ConnectTimeout).
		SetReadPreference(readpref.PrimaryPreferred()).
		SetCompressors([]string{"zstd", "snappy"})

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), cfg.PingTimeout)
	defer pingCancel()

	if err = client.Ping(pingCtx, readpref.Primary()); err != nil {
		client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongo ping: %w", err)
	}

	return &mongoDatabase{client: client}, nil
}
