package main

import (
	"airbyte-service/core"
	postgresdatabase "airbyte-service/database/postgres"
	sync_common "airbyte-service/sync/sources/common"
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

var syncInterval time.Duration
var mongoConnectionString string
var mongoDatabaseName string
var mongoTableName string
var postgresConnectionString string
var primaryKey string
var sources = []syncworker.Source{
	{
		MongoURI: getEnv("MONGO_CONNECTION_STRING", ""),
		Database: "warehouseOperationService",
		Tables: []syncworker.Table{
			{
				Name:       "sales",
				WriteMode:  core.WriteModeOverwrite,
				PrimaryKey: "_id",
				Fields: []sync_common.FieldSpec{
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
					{Name: "payment.date", PgType: "TIMESTAMPTZ"},
					{Name: "payment.debt_states", PgType: "JSONB"},
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
				},
			},
		},
	},
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, reading from environment")
	}

	mongoConnectionString = getEnv("MONGO_CONNECTION_STRING", "")
	mongoDatabaseName = getEnv("MONGO_DATABASE_NAME", "")
	mongoTableName = getEnv("MONGO_TABLE_NAME", "")
	postgresConnectionString = getEnv("POSTGRES_CONNECTION_STRING", "")
	primaryKey = getEnv("PRIMARY_KEY", "_id")

	if d, err := time.ParseDuration(getEnv("SYNC_INTERVAL", "1m")); err == nil {
		syncInterval = d
	} else {
		log.Fatalf("invalid SYNC_INTERVAL: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	databasePostgres, err := postgresdatabase.NewDatabase(postgresdatabase.PostgresConfig{ConnectionString: postgresConnectionString})
	if err != nil {
		log.Fatalf("cannot connect to postgres: %v", err)
	}
	defer databasePostgres.Close()

	// worker
	syncWorker := syncworker.New(
		syncInterval,
		databasePostgres.GetPool(),
		slog.Default(),
	)
	syncWorker.PutDatabasesWithTables(sources)

	if err := syncWorker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
