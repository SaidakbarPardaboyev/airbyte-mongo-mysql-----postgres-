package main

import (
	"context"
	"log"
	"log/slog"
	"time"

	"syncer/internal/catalog"
	"syncer/internal/db"
	"syncer/internal/destination"
	"syncer/internal/source"
)

var (
	mongoConnectionString = "mongodb://dba:0sjnrqoLlK7OzE81x3K0doeJY3ii1b7u@localhost:27016/?directConnection=true&authSource=admin"
	mongoDatabaseName     = "warehouseOperationService"
	mongoTableNames       = "sales"
	mongoTableFields      = []catalog.FieldSpec{
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
		{Name: "payment.cash_box_states.amount", PgType: "DOUBLE PRECISION"},
		{Name: "payment.cash_box_states.currency.id", PgType: "BIGINT"},
		{Name: "payment.cash_box_states.currency.is_national", PgType: "BOOLEAN"},
		{Name: "payment.cash_box_states.currency.name", PgType: "VARCHAR"},
		{Name: "payment.cash_box_states.payment_type", PgType: "INTEGER"},
		{Name: "payment.date", PgType: "TIMESTAMPTZ"},
		{Name: "payment.debt_states", PgType: "JSONB"},
		{Name: "payment.debt_states.amount", PgType: "DOUBLE PRECISION"},
		{Name: "payment.debt_states.currency.id", PgType: "BIGINT"},
		{Name: "payment.debt_states.currency.is_national", PgType: "BOOLEAN"},
		{Name: "payment.debt_states.currency.name", PgType: "VARCHAR"},
		{Name: "payment.id", PgType: "VARCHAR"},
		{Name: "payment.notes", PgType: "VARCHAR"},
		{Name: "percent_discount", PgType: "DOUBLE PRECISION"},
		{Name: "reviewed_at", PgType: "TIMESTAMPTZ"},
		{Name: "status", PgType: "INTEGER"},
		{Name: "tags", PgType: "JSONB"},
		{Name: "updated_at", PgType: "TIMESTAMPTZ"},
		{Name: "used_warehouses", PgType: "JSONB"},

		{Name: "exact_discounts", PgType: "JSONB"},
		{Name: "exact_discounts.amount", PgType: "DOUBLE PRECISION"},
		{Name: "exact_discounts.currency.id", PgType: "BIGINT"},
		{Name: "exact_discounts.currency.is_national", PgType: "BOOLEAN"},
		{Name: "exact_discounts.currency.name", PgType: "VARCHAR"},

		{Name: "net_price", PgType: "JSONB"},
		{Name: "net_price.amount", PgType: "DOUBLE PRECISION"},
		{Name: "net_price.currency.id", PgType: "BIGINT"},
		{Name: "net_price.currency.is_national", PgType: "BOOLEAN"},
		{Name: "net_price.currency.name", PgType: "VARCHAR"},

		{Name: "items", PgType: "JSONB"},
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

	postgresConnectionString = "postgresql://postgres:postgres@localhost:5432/report_service"
	primaryKey               = "_id"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// ── 1. Connect to MongoDB ────────────────────────────────────────────────
	mongoCli, err := db.ConnectMongo(db.MongoConfig{
		URI: mongoConnectionString,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.DisconnectMongo(mongoCli)

	// ── 2. Discover schema ───────────────────────────────────────────────────
	mongoCat, err := catalog.NewMongoDiscoverer(mongoCli, mongoDatabaseName).Discover(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// log.Println("\n=== MongoDB catalog ===")
	// catalog.PrintCatalog(os.Stdout, mongoCat)

	// ── 3. Find the target stream and filter to requested fields ─────────────
	discovered, ok := mongoCat.Get(mongoDatabaseName, mongoTableNames)
	if !ok {
		log.Fatalf("collection %q not found in catalog", mongoTableNames)
	}
	stream := discovered.FilterFields(mongoTableFields)

	// ── 4. Connect to Postgres ───────────────────────────────────────────────
	pool, err := destination.NewPool(ctx, postgresConnectionString)
	if err != nil {
		log.Fatal(err)
	}

	// ── 5. Ensure destination table exists ───────────────────────────────────
	if err := destination.EnsureTable(ctx, pool, "", mongoTableNames, stream); err != nil {
		log.Fatal(err)
	}

	// ── 6. Read from MongoDB, write to Postgres ──────────────────────────────
	writer := destination.NewWriter(pool, destination.WriterConfig{
		Schema:     "",
		Table:      mongoTableNames,
		Mode:       destination.WriteModeOverwrite,
		PrimaryKey: []string{primaryKey},
	}, slog.Default())

	ch, err := source.ReadCollection(ctx, mongoCli, mongoDatabaseName, mongoTableNames, stream)
	if err != nil {
		log.Fatal(err)
	}

	result, err := writer.Write(ctx, stream, ch)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("done: %d rows in %d batches (%s)", result.RowsCopied, result.Batches, result.Duration)
}
