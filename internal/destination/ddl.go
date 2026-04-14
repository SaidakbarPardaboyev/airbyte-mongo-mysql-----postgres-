package destination

import (
	"context"
	"fmt"
	"strings"

	"syncer/internal/catalog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EnsureTable creates all destination tables in stream.Tables if they don't exist,
// mapping normalized types to Postgres types. Each field's TableName determines
// which table it belongs to.
func EnsureTable(ctx context.Context, pool *pgxpool.Pool, schema string, stream *catalog.Stream) error {
	for _, tableName := range stream.Tables {
		var cols []string
		var pks []string

		for _, f := range stream.Fields {
			if f.TableName != tableName {
				continue
			}
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
			qualifiedTable = pgx.Identifier{schema, tableName}.Sanitize()
		} else {
			qualifiedTable = pgx.Identifier{tableName}.Sanitize()
		}

		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)",
			qualifiedTable, strings.Join(cols, ",\n"))

		if _, err := pool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("ensure table %s: %w", tableName, err)
		}
	}
	return nil
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
