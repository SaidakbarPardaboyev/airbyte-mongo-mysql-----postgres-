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

func EnsureTable(ctx context.Context, pool *pgxpool.Pool, table *sourcecommon.Table) error {
	for _, tableName := range table.Tables {
		var columns []string
		var pks []string

		for _, f := range table.Fields {
			if f.TableName != tableName {
				continue
			}
			postgresType := fieldPostgresType(f)
			nullable := ""
			if !f.Nullable && f.IsPrimary {
				nullable = " NOT NULL"
			}
			columnName := strings.TrimPrefix(f.Name, tableName+".")
			column := fmt.Sprintf("  %s %s%s", pgx.Identifier{columnName}.Sanitize(), postgresType, nullable)
			columns = append(columns, column)
			if f.IsPrimary {
				pks = append(pks, pgx.Identifier{columnName}.Sanitize())
			}
		}

		if len(pks) > 0 {
			columns = append(columns, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(pks, ", ")))
		}

		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)",
			pgx.Identifier{tableName}.Sanitize(), strings.Join(columns, ",\n"))

		if _, err := pool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("ensure table %s: %w", tableName, err)
		}
	}
	return nil
}

func fieldPostgresType(f sourcecommon.Field) string {
	if f.DestinationType != "" {
		return f.DestinationType
	}
	return toPgType(f.SourceType)
}

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
		return "TEXT"
	}
}

func QualifiedTable(tableName string) string {
	return pgx.Identifier{tableName}.Sanitize()
}

func TempTable(tableName string) string {
	return fmt.Sprintf("_gosync_tmp_%s", tableName)
}
