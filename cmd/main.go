package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syncer/services/db"
	"syncer/services/destination"
	"syncer/services/scheduler"
	"syncer/services/sources/common"
	"syncer/services/sources/mongo"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var (
	mongoConnectionString = ""
	mongoDatabaseName     = ""
	mongoTableNames       = ""
	syncInterval          = time.Duration(0)
	mongoTableFields      = []common.FieldSpec{
		{Name: "_id", PgType: "VARCHAR"},
		{Name: "account.id", PgType: "VARCHAR"},
		{Name: "account.name", PgType: "VARCHAR"},
		{Name: "account.username", PgType: "VARCHAR"},
		{Name: "approved_at", PgType: "TIMESTAMPTZ"},
		{Name: "branch.id", PgType: "VARCHAR"},
		{Name: "branch.name", PgType: "VARCHAR"},
		{Name: "cash_box.id", PgType: "VARCHAR"},
		{Name: "cash_box.name", PgType: "VARCHAR"},
		{Name: "collected_at", PgType: "TIMESTAMPTZ"},
		{Name: "contractor.id", PgType: "VARCHAR"},
		{Name: "contractor.inn", PgType: "VARCHAR"},
		{Name: "contractor.name", PgType: "VARCHAR"},
		{Name: "created_at", PgType: "TIMESTAMPTZ"},
		{Name: "date", PgType: "TIMESTAMPTZ"},
		{Name: "deleted_at", PgType: "TIMESTAMPTZ"},
		{Name: "delivery_info", PgType: "VARCHAR"},
		{Name: "employee.id", PgType: "BIGINT"},
		{Name: "employee.name", PgType: "VARCHAR"},
		{Name: "handed_over_at", PgType: "TIMESTAMPTZ"},
		{Name: "is_approved", PgType: "BOOLEAN"},
		{Name: "is_collected", PgType: "BOOLEAN"},
		{Name: "is_deleted", PgType: "BOOLEAN"},
		{Name: "is_fiscalized", PgType: "BOOLEAN"},
		{Name: "is_for_debt", PgType: "BOOLEAN"},
		{Name: "is_handed_over", PgType: "BOOLEAN"},
		{Name: "is_reviewed", PgType: "BOOLEAN"},
		{Name: "number", PgType: "VARCHAR"},
		{Name: "location", PgType: "VARCHAR"},
		{Name: "organization.id", PgType: "VARCHAR"},
		{Name: "organization.name", PgType: "VARCHAR"},
		{Name: "payment.cash_box_states", PgType: "JSONB"},
		// {Name: "payment.cash_box_states.amount", PgType: "DOUBLE PRECISION"},
		// {Name: "payment.cash_box_states.currency.id", PgType: "BIGINT"},
		// {Name: "payment.cash_box_states.currency.is_national", PgType: "BOOLEAN"},
		// {Name: "payment.cash_box_states.currency.name", PgType: "VARCHAR"},
		// {Name: "payment.cash_box_states.payment_type", PgType: "INTEGER"},
		{Name: "payment.date", PgType: "TIMESTAMPTZ"},
		{Name: "payment.debt_states", PgType: "JSONB"},
		// {Name: "payment.debt_states.amount", PgType: "DOUBLE PRECISION"},
		// {Name: "payment.debt_states.currency.id", PgType: "BIGINT"},
		// {Name: "payment.debt_states.currency.is_national", PgType: "BOOLEAN"},
		// {Name: "payment.debt_states.currency.name", PgType: "VARCHAR"},
		{Name: "payment.id", PgType: "VARCHAR"},
		{Name: "payment.notes", PgType: "VARCHAR"},
		{Name: "percent_discount", PgType: "DOUBLE PRECISION"},
		{Name: "reviewed_at", PgType: "TIMESTAMPTZ"},
		{Name: "status", PgType: "INTEGER"},
		{Name: "tags", PgType: "JSONB"},
		{Name: "updated_at", PgType: "TIMESTAMPTZ"},

		{Name: "exact_discounts", PgType: "JSONB", SeparateTable: true},
		{Name: "exact_discounts.amount", PgType: "DOUBLE PRECISION"},
		{Name: "exact_discounts.currency.id", PgType: "BIGINT"},
		{Name: "exact_discounts.currency.is_national", PgType: "BOOLEAN"},
		{Name: "exact_discounts.currency.name", PgType: "VARCHAR"},

		{Name: "net_price", PgType: "JSONB", SeparateTable: true},
		{Name: "net_price.amount", PgType: "DOUBLE PRECISION"},
		{Name: "net_price.currency.id", PgType: "BIGINT"},
		{Name: "net_price.currency.is_national", PgType: "BOOLEAN"},
		{Name: "net_price.currency.name", PgType: "VARCHAR"},

		{Name: "items", PgType: "JSONB", SeparateTable: true},
		{Name: "items.created_at", PgType: "TIMESTAMPTZ"},
		{Name: "items.deleted_at", PgType: "TIMESTAMPTZ"},
		{Name: "items.discount.amount", PgType: "DOUBLE PRECISION"},
		{Name: "items.discount.type", PgType: "INTEGER"},
		{Name: "items.discount.value", PgType: "DOUBLE PRECISION"},
		{Name: "items.id", PgType: "VARCHAR"},
		{Name: "items.is_deleted", PgType: "BOOLEAN"},
		{Name: "items.net_price.amount", PgType: "DOUBLE PRECISION"},
		{Name: "items.net_price.currency.id", PgType: "BIGINT"},
		{Name: "items.net_price.currency.is_national", PgType: "BOOLEAN"},
		{Name: "items.net_price.currency.name", PgType: "VARCHAR"},
		{Name: "items.original_price.amount", PgType: "DOUBLE PRECISION"},
		{Name: "items.original_price.currency.id", PgType: "BIGINT"},
		{Name: "items.original_price.currency.is_national", PgType: "BOOLEAN"},
		{Name: "items.original_price.currency.name", PgType: "VARCHAR"},
		{Name: "items.price.amount", PgType: "DOUBLE PRECISION"},
		{Name: "items.price.currency.id", PgType: "BIGINT"},
		{Name: "items.price.currency.is_national", PgType: "BOOLEAN"},
		{Name: "items.price.currency.name", PgType: "VARCHAR"},
		{Name: "items.quantity", PgType: "DOUBLE PRECISION"},
		{Name: "items.updated_at", PgType: "TIMESTAMPTZ"},
		{Name: "items.warehouse_item.id", PgType: "VARCHAR"},
		{Name: "items.warehouse_item.marks", PgType: "JSONB"},
		{Name: "items.warehouse_item.name", PgType: "VARCHAR"},
		{Name: "items.warehouse_item.warehouse.id", PgType: "INTEGER"},
		{Name: "items.warehouse_item.warehouse.name", PgType: "VARCHAR"},
		{Name: "items.warehouse_item.warehouse_item_use.after_quantity", PgType: "DOUBLE PRECISION"},
		{Name: "items.warehouse_item.warehouse_item_use.before_quantity", PgType: "DOUBLE PRECISION"},
		{Name: "items.warehouse_item.warehouse_item_use.id", PgType: "BIGINT"},
	}

	postgresConnectionString = ""
	primaryKey               = ""
)

// checking data was synced correctly ✅
// sync prod data and analize and optimize it ✅
// add schedule system ✅
// add apis to sync settings

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, reading from environment")
	}

	mongoConnectionString = getEnv("MONGO_CONNECTION_STRING", "")
	mongoDatabaseName = getEnv("MONGO_DATABASE_NAME", "")
	mongoTableNames = getEnv("MONGO_TABLE_NAMES", "")
	postgresConnectionString = getEnv("POSTGRES_CONNECTION_STRING", "")
	primaryKey = getEnv("PRIMARY_KEY", "_id")

	if d, err := time.ParseDuration(getEnv("SYNC_INTERVAL", "1m")); err == nil {
		syncInterval = d
	} else {
		log.Fatalf("invalid SYNC_INTERVAL: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown on SIGINT / SIGTERM
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		log.Println("shutdown signal received, stopping…")
		cancel()
	}()

	sched := scheduler.New(syncInterval, runSync, slog.Default())
	if err := sched.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func runSync(ctx context.Context) error {
	// ── 1. Connect to MongoDB ────────────────────────────────────────────────
	mongoCli, err := db.ConnectMongo(db.MongoConfig{
		URI: mongoConnectionString,
	})
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer db.DisconnectMongo(mongoCli)

	// ── 2. Discover schema ───────────────────────────────────────────────────
	mongoCat, err := mongo.NewMongoDiscoverer(mongoCli, mongoDatabaseName, 1_000).Discover(ctx)
	if err != nil {
		return fmt.Errorf("discover schema: %w", err)
	}

	// log.Println("\n=== MongoDB catalog ===")
	// catalog.PrintCatalog(os.Stdout, mongoCat)

	// ── 3. Find the target stream and filter to requested fields ─────────────
	discovered, ok := mongoCat.Get(mongoDatabaseName, mongoTableNames)
	if !ok {
		return fmt.Errorf("collection %q not found in catalog", mongoTableNames)
	}
	stream := discovered.FilterFields(mongoTableFields)
	stream.FillTableNames()

	// ── 4. Connect to Postgres ───────────────────────────────────────────────
	pool, err := destination.NewPool(ctx, postgresConnectionString)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}

	// ── 5. Ensure destination tables exist ───────────────────────────────────
	if err := destination.EnsureTable(ctx, pool, "", stream); err != nil {
		return fmt.Errorf("ensure tables: %w", err)
	}

	// ── 6. Read from MongoDB, write to Postgres ──────────────────────────────
	msgCh, err := mongo.ReadCollection(ctx, mongoCli, mongoDatabaseName, mongoTableNames, stream)
	if err != nil {
		return fmt.Errorf("read collection: %w", err)
	}

	// Build one Writer per destination table
	writers := make(map[string]*destination.Writer)
	channels := make(map[string]chan mongo.Row)

	for _, tableName := range stream.Tables {
		ch := make(chan mongo.Row, 256)
		channels[tableName] = ch
		writers[tableName] = destination.NewWriter(pool, destination.WriterConfig{
			Table:      tableName,
			Mode:       destination.WriteModeOverwrite,
			PrimaryKey: []string{primaryKey},
		}, slog.Default())
	}

	// Dispatch goroutine — routes messages to per-table channels
	go func() {
		for msg := range msgCh {
			if ch, ok := channels[msg.Stream]; ok {
				ch <- msg.Row
			}
		}
		// Close all channels when source is done
		for _, ch := range channels {
			close(ch)
		}
	}()

	// Run all writers concurrently
	var wg sync.WaitGroup
	results := make(map[string]*destination.WriteResult)
	var mu sync.Mutex

	for tableName, w := range writers {
		wg.Add(1)
		go func(name string, w *destination.Writer, ch <-chan mongo.Row) {
			defer wg.Done()
			res, err := w.Write(ctx, stream, ch)
			if err != nil {
				log.Printf("write %s failed: %v", name, err)
				return
			}
			mu.Lock()
			results[name] = res
			mu.Unlock()
		}(tableName, w, channels[tableName])
	}

	wg.Wait()

	for name, res := range results {
		log.Printf("done [%s]: %d rows in %d batches (%s)", name, res.RowsCopied, res.Batches, res.Duration)
	}

	return nil
}
