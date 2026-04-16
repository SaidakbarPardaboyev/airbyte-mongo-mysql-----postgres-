package destination

import (
	"context"
	"fmt"
	"strings"
	"syncer/core"
	"syncer/services/sources/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EnsureTable creates all destination tables in stream.Tables if they don't exist,
// mapping normalized types to Postgres types. Each field's TableName determines
// which table it belongs to.
func EnsureTable(ctx context.Context, pool *pgxpool.Pool, schema string, stream *common.Table) error {
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
			col := fmt.Sprintf("  %s %s%s", pgx.Identifier{f.Name}.Sanitize(), pgType, nullable)
			cols = append(cols, col)
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
func fieldPgType(f common.Field) string {
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
