package db

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoConfig struct {
	URI            string        // "mongodb://user:pass@host:27017"
	ConnectTimeout time.Duration
	PingTimeout    time.Duration
	// For large collection reads: use a server-side cursor,
	// not pulling everything into memory at once.
	// These two fields tune the cursor batch size sent by the server.
	MinPoolSize uint64
	MaxPoolSize uint64
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

// ConnectMongo creates and validates a MongoDB client.
func ConnectMongo(cfg MongoConfig) (*mongo.Client, error) {
	cfg.setDefaults()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	opts := options.Client().
		ApplyURI(cfg.URI).
		SetMinPoolSize(cfg.MinPoolSize).
		SetMaxPoolSize(cfg.MaxPoolSize).
		SetConnectTimeout(cfg.ConnectTimeout).
		// ReadPreference: primary preferred for consistency during sync
		SetReadPreference(readpref.PrimaryPreferred()).
		// Compress traffic between driver and server
		SetCompressors([]string{"zstd", "snappy"})

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), cfg.PingTimeout)
	defer pingCancel()

	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongo ping: %w", err)
	}

	return client, nil
}

// DisconnectMongo cleanly closes the MongoDB client.
// Call this in your shutdown sequence: defer db.DisconnectMongo(client)
func DisconnectMongo(client *mongo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return client.Disconnect(ctx)
}