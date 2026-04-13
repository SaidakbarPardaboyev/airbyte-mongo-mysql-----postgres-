package destination

import (
	"context"
	"fmt"
	"strings"

	"syncer/internal/catalog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EnsureTable creates the destination table if it doesn't exist,
// mapping normalized types to Postgres types.
func EnsureTable(ctx context.Context, pool *pgxpool.Pool, schema, table string, stream *catalog.Stream) error {
	cols := make([]string, 0, len(stream.Fields))
	var pks []string

	for _, f := range stream.Fields {
		pgType := fieldPgType(f)
		nullable := ""
		if !f.Nullable && f.IsPrimary {
			nullable = " NOT NULL"
		}
		cols = append(cols, fmt.Sprintf("  %s %s%s",
			pgx.Identifier{f.Name}.Sanitize(), pgType, nullable))
		if f.IsPrimary {
			pks = append(pks, pgx.Identifier{f.Name}.Sanitize())
		}
	}

	if len(pks) > 0 {
		cols = append(cols, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(pks, ", ")))
	}

	var qualifiedTable string
	if schema != "" {
		qualifiedTable = pgx.Identifier{schema, table}.Sanitize()
	} else {
		qualifiedTable = pgx.Identifier{table}.Sanitize()
	}

	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)",
		qualifiedTable, strings.Join(cols, ",\n"))

	_, err := pool.Exec(ctx, ddl)
	return err
}

// fieldPgType returns the PostgreSQL type for a field,
// using DestType when explicitly set, otherwise mapping from NormType.
func fieldPgType(f catalog.Field) string {
	if f.DestType != "" {
		return f.DestType
	}
	return toPgType(f.NormType)
}

// toPgType maps a BSONType to a Postgres column type.
func toPgType(t catalog.BSONType) string {
	switch t {
	case catalog.BSONTypeBool:
		return "BOOLEAN"
	case catalog.BSONTypeInt32:
		return "INTEGER"
	case catalog.BSONTypeInt64:
		return "BIGINT"
	case catalog.BSONTypeDouble, catalog.BSONTypeDecimal128:
		return "DOUBLE PRECISION"
	case catalog.BSONTypeString, catalog.BSONTypeObjectID, catalog.BSONTypeSymbol,
		catalog.BSONTypeJavaScript, catalog.BSONTypeJavaScriptWithScope, catalog.BSONTypeRegex:
		return "VARCHAR"
	case catalog.BSONTypeDate, catalog.BSONTypeTimestamp:
		return "TIMESTAMPTZ"
	case catalog.BSONTypeBinData:
		return "BYTEA"
	case catalog.BSONTypeObject, catalog.BSONTypeArray:
		return "JSONB"
	default:
		return "TEXT" // safe fallback
	}
}
