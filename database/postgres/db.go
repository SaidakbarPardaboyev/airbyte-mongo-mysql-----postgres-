package postgres

import (
	"airbyte-service/core"
	sourcecommon "airbyte-service/sync/sources/common"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresConfig struct {
	ConnectionString string
	ConnectTimeout   time.Duration
	PingTimeout      time.Duration
	MinConns         int32
	MaxConns         int32
}

func (c *PostgresConfig) setDefaults() {
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	if c.PingTimeout == 0 {
		c.PingTimeout = 5 * time.Second
	}
	if c.MaxConns == 0 {
		c.MaxConns = 10
	}
	if c.MinConns == 0 {
		c.MinConns = 1
	}
}

type Database interface {
	Close()
	GetPool() *pgxpool.Pool
}

type postgresDatabase struct {
	pool *pgxpool.Pool
}

func (p *postgresDatabase) Close() {
	p.pool.Close()
}

func (p *postgresDatabase) GetPool() *pgxpool.Pool {
	return p.pool
}

func NewDatabase(cfg PostgresConfig) (Database, error) {
	cfg.setDefaults()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	poolCfg, err := pgxpool.ParseConfig(cfg.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("postgres parse config: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres create pool: %w", err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), cfg.PingTimeout)
	defer pingCancel()

	if err = pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}

	return &postgresDatabase{pool: pool}, nil
}

// EnsureTable creates all destination tables in table.Tables if they don't exist,
// mapping normalized types to Postgres types. Each field's TableName determines
// which table it belongs to.
func EnsureTable(ctx context.Context, pool *pgxpool.Pool, table *sourcecommon.Table) error {
	for _, tableName := range table.Tables {
		var cols []string
		var pks []string

		for _, f := range table.Fields {
			if f.TableName != tableName {
				continue
			}
			pgType := fieldPgType(f)
			nullable := ""
			if !f.Nullable && f.IsPrimary {
				nullable = " NOT NULL"
			}
			col := fmt.Sprintf("  %s %s%s", pgx.Identifier{f.Name}.Sanitize(), pgType, nullable)
			cols = append(cols, col)
			if f.IsPrimary {
				pks = append(pks, pgx.Identifier{f.Name}.Sanitize())
			}
		}

		if len(pks) > 0 {
			cols = append(cols, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(pks, ", ")))
		}

		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)",
			pgx.Identifier{tableName}.Sanitize(), strings.Join(cols, ",\n"))

		if _, err := pool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("ensure table %s: %w", tableName, err)
		}
	}
	return nil
}

// fieldPgType returns the PostgreSQL type for a field,
// using DestType when explicitly set, otherwise mapping from NormType.
func fieldPgType(f sourcecommon.Field) string {
	if f.DestType != "" {
		return f.DestType
	}
	return toPgType(f.NormType)
}

// toPgType maps a BSONType to a Postgres column type.
func toPgType(t core.BSONType) string {
	switch t {
	case core.BSONTypeBool:
		return "BOOLEAN"
	case core.BSONTypeInt32:
		return "INTEGER"
	case core.BSONTypeInt64:
		return "BIGINT"
	case core.BSONTypeDouble, core.BSONTypeDecimal128:
		return "DOUBLE PRECISION"
	case core.BSONTypeString, core.BSONTypeObjectID, core.BSONTypeSymbol,
		core.BSONTypeJavaScript, core.BSONTypeJavaScriptWithScope, core.BSONTypeRegex:
		return "VARCHAR"
	case core.BSONTypeDate, core.BSONTypeTimestamp:
		return "TIMESTAMPTZ"
	case core.BSONTypeBinData:
		return "BYTEA"
	case core.BSONTypeObject, core.BSONTypeArray:
		return "JSONB"
	default:
		return "TEXT" // safe fallback
	}
}

func QualifiedTable(tableName string) string {
	return pgx.Identifier{tableName}.Sanitize()
}

func TempTable(tableName string) string {
	return fmt.Sprintf("_gosync_tmp_%s", tableName)
}
