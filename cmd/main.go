package main

import (
	"airbyte-service/core"
	postgresdatabase "airbyte-service/database/postgres"
	syncworker "airbyte-service/sync/worker"
	"context"
	"errors"
	"log/slog"

	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func init() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, reading from environment")
	}

	core.MongoConnectionString = getEnv("MONGO_CONNECTION_STRING", "")
	core.MongoDatabaseName = getEnv("MONGO_DATABASE_NAME", "")
	core.MongoTableName = getEnv("MONGO_TABLE_NAME", "")
	core.PostgresConnectionString = getEnv("POSTGRES_CONNECTION_STRING", "")
	core.PrimaryKey = getEnv("PRIMARY_KEY", "_id")

	if d, err := time.ParseDuration(getEnv("SYNC_INTERVAL", "1m")); err == nil {
		core.SyncInterval = d
	} else {
		log.Fatalf("invalid SYNC_INTERVAL: %v", err)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	databasePostgres, err := postgresdatabase.NewDatabase(postgresdatabase.PostgresConfig{ConnectionString: core.PostgresConnectionString})
	if err != nil {
		log.Fatalf("cannot connect to postgres: %v", err)
	}
	defer databasePostgres.Close()

	// worker
	syncWorker, err := syncworker.New(
		core.SyncInterval,
		databasePostgres.GetPool(),
		slog.Default(),
	)
	if err != nil {
		log.Fatalf("cannot create sync worker: %v", err)
	}
	if err := syncWorker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
