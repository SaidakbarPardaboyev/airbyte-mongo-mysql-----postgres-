# MongoDB → PostgreSQL Syncer

A lightweight alternative to Airbyte for syncing MongoDB collections into PostgreSQL. Built in Go — no heavy infrastructure, no paid tiers, just a single binary.

> **Important:** This is not an installable package you can drop into your project unchanged. It is a fully working implementation that you study, then adapt — swap in your own MongoDB collections, define your own field list, and wire up your own connection strings. Think of it as a reference implementation, not a library.

## Why

Airbyte is powerful but resource-heavy and requires a paid plan for production use. This tool does one thing: read documents from a MongoDB collection, flatten nested fields, and upsert them into PostgreSQL tables on a schedule.

## Features

- **Schema discovery** — automatically introspects field names and BSON types from a MongoDB collection using aggregation pipelines (supports nested objects and arrays)
- **Flexible field mapping** — you choose which fields to sync and override their PostgreSQL types via a `FieldSpec` list
- **Array fields as child tables** — array fields (e.g. `items`, `exact_discounts`) can be split into their own PostgreSQL tables with a foreign key back to the parent
- **Scheduled sync** — runs the first sync immediately on startup, then repeats on a configurable interval; skips a tick if the previous sync is still running
- **Overwrite mode** — uses `COPY` + truncate for fast bulk loads
- **Graceful shutdown** — handles `SIGINT`/`SIGTERM` cleanly

## Tech Stack

- **Go** 1.21+
- **MongoDB** via `go.mongodb.org/mongo-driver`
- **PostgreSQL** via `github.com/jackc/pgx/v5`
- **Env loading** via `github.com/joho/godotenv`

## Getting Started

### Prerequisites

- Go 1.21+
- A running MongoDB instance
- A running PostgreSQL instance

### Installation

```bash
git clone https://github.com/SaidakbarPardaboyev/airbyte-mongo-mysql-----postgres-.git
cd airbyte-mongo-mysql-----postgres-
go mod download
```

### Configuration

Copy `.env` and fill in your values:

```bash
cp .env .env.local
```

| Variable | Description | Example |
|---|---|---|
| `MONGO_CONNECTION_STRING` | MongoDB connection URI | `mongodb://user:pass@localhost:27017/` |
| `MONGO_DATABASE_NAME` | Source database name | `warehouseOperationService` |
| `MONGO_TABLE_NAMES` | Collection to sync | `sales` |
| `SYNC_INTERVAL` | How often to sync (Go duration) | `1m`, `5m`, `1h` |
| `POSTGRES_CONNECTION_STRING` | PostgreSQL connection URL | `postgresql://user:pass@localhost:5432/db` |
| `PRIMARY_KEY` | Field used as upsert key | `_id` |

### Run

```bash
go run ./cmd/main.go
```

Or build a binary:

```bash
go build -o syncer ./cmd/main.go
./syncer
```

## Adapting to Your Project

The main thing you need to change is in [cmd/main.go](cmd/main.go):

1. **Set your collection name** via `MONGO_TABLE_NAMES` in `.env`
2. **Define your field list** — replace the `mongoTableFields` slice with the fields from your own collection:

```go
mongoTableFields = []catalog.FieldSpec{
    {Name: "_id",        PgType: "VARCHAR"},
    {Name: "created_at", PgType: "TIMESTAMPTZ"},
    {Name: "name",       PgType: "VARCHAR"},

    // Array field → gets its own child table with a FK back to the parent
    {Name: "items",      PgType: "JSONB", SeparateTable: true},
    {Name: "items.id",   PgType: "VARCHAR"},
    {Name: "items.qty",  PgType: "DOUBLE PRECISION"},
}
```

Fields not listed are ignored. Leave `PgType` empty to have the type inferred automatically from the BSON type.

## How It Works

1. **Connect** to MongoDB and run an aggregation pipeline to discover all field names and BSON types (sampling up to 1,000 documents by default)
2. **Filter** the discovered schema down to your `mongoTableFields` list, applying explicit PostgreSQL type overrides
3. **Split tables** — fields marked `SeparateTable: true` are routed to their own child tables; a `<parent>_id` FK column is injected automatically
4. **Ensure tables** — `CREATE TABLE IF NOT EXISTS` is run for every destination table
5. **Stream & write** — documents are read from MongoDB and written to PostgreSQL concurrently per table using `pgx` bulk copy
6. **Repeat** on the configured interval

## Project Structure

```
cmd/
  main.go               # entry point, config, field definitions
internal/
  catalog/              # schema types, discovery, field filtering
  db/                   # MongoDB connection helpers
  source/               # MongoDB collection reader
  destination/          # PostgreSQL pool, DDL, writer
  record/               # shared Row type
  scheduler/            # interval scheduler with job history
```
