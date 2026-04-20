package sync_mongo

import (
	sourcecommon "airbyte-service/sync/sources/common"
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// ReadCollection opens a cursor on collName and reads every document as a
// flattened Row into the returned channel.  The channel is closed when
// all documents have been sent or ctx is cancelled.
//
// Nested objects are expanded using dot-notation keys ("account.id").
// Array fields are kept as []any so normalizeValue in the writer serialises
// them to JSONB.  Only fields present in table.Fields are emitted.
func ReadCollection(
	ctx context.Context,
	client *mongo.Client,
	dbName, collName string,
	table *sourcecommon.Table,
) (<-chan Message, error) {
	coll := client.Database(dbName).Collection(collName)
	cursor, err := coll.Find(ctx, bson.M{"is_deleted": false})
	if err != nil {
		return nil, fmt.Errorf("mongo find %s.%s: %w", dbName, collName, err)
	}

	ch := make(chan Message, 256)

	go func() {
		defer close(ch)
		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			var doc bson.M
			if err := cursor.Decode(&doc); err != nil {
				continue
			}
			emitRows(ctx, doc, table, ch)
		}
	}()

	return ch, nil
}

// emitRows fans out one MongoDB document into N messages across M tables.
func emitRows(ctx context.Context, doc bson.M, table *sourcecommon.Table, ch chan<- Message) {
	// Build a flat map of all scalar/object values keyed by dot-path
	flat := make(Row, len(table.Fields))
	flattenBSONM(doc, "", table.FieldMap, flat)

	// Get the parent ID (always "_id")
	parentID := flat["_id"]

	// Group fields by destination table
	tableFields := make(map[string][]string)
	for _, f := range table.Fields {
		if f.TableName != "" {
			tableFields[f.TableName] = append(tableFields[f.TableName], f.Name)
		}
	}

	// Emit parent table row (e.g. "sales")
	parentRow := make(Row)
	for _, name := range tableFields[table.Name] {
		parentRow[name] = flat[name]
	}
	select {
	case ch <- Message{Table: table.Name, Row: parentRow}:
	case <-ctx.Done():
		return
	}

	// Emit child table rows — one message per array element
	for tableName, fieldNames := range tableFields {
		if tableName == table.Name {
			continue
		}

		// The raw array lives in the flat map under the table name key (e.g. "items")
		rawArray, ok := doc[tableName] // e.g. doc["items"]
		if !ok {
			continue
		}
		arr, ok := rawArray.(bson.A)
		if !ok {
			continue
		}

		// Build a fieldSet for only this child table's sub-fields
		childFieldSet := make(map[string]sourcecommon.Field)
		prefix := tableName + "."
		for _, name := range fieldNames {
			if strings.HasPrefix(name, prefix) {
				childFieldSet[name] = table.FieldMap[name]
			}
		}

		for _, elem := range arr {
			elemDoc, ok := elem.(bson.M)
			if !ok {
				continue
			}

			childRow := make(Row)
			fkColName := strings.TrimSuffix(table.Name, "s") + "_id"
			childRow[fkColName] = parentID // FK to parent

			// Flatten the element using the child's fieldSet,
			// but strip the "items." prefix from keys
			childFlat := make(Row)
			flattenBSONM(elemDoc, tableName, childFieldSet, childFlat)
			for k, v := range childFlat {
				childRow[k] = v
			}

			select {
			case ch <- Message{Table: tableName, Row: childRow}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// flattenBSONM recursively walks a bson.M and writes leaf values into row
// using dot-notation keys.  Only keys present in fieldSet are written.
func flattenBSONM(m bson.M, prefix string, fieldSet map[string]sourcecommon.Field, row Row) {
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}

		switch val := v.(type) {
		case bson.M:
			// nested object — recurse; the object key itself is not stored
			flattenBSONM(val, key, fieldSet, row)

		case bson.D:
			// ordered document — convert to bson.M then recurse
			flattenBSONM(bsonDToM(val), key, fieldSet, row)

		case bson.A:
			// array — keep as []any; writer serialises to JSONB
			if _, ok := fieldSet[key]; ok {
				row[key] = []any(val)
			}

		default:
			if _, ok := fieldSet[key]; ok {
				row[key] = normalizeBSONValue(v)
			}
		}

	}
}

func bsonDToM(d bson.D) bson.M {
	m := make(bson.M, len(d))
	for _, e := range d {
		m[e.Key] = e.Value
	}
	return m
}

// normalizeBSONValue converts BSON-specific types to plain Go types that pgx
// can send to Postgres without extra encoding.
func normalizeBSONValue(v any) any {
	switch val := v.(type) {
	case primitive.ObjectID:
		return val.Hex()
	case primitive.DateTime:
		return val.Time().UTC()
	case primitive.Timestamp:
		// Timestamp is (seconds, ordinal) — convert to time.Time
		return time.Unix(int64(val.T), 0).UTC()
	case primitive.Decimal128:
		return val.String()
	case primitive.Binary:
		return val.Data
	case primitive.Regex:
		return val.Pattern
	case primitive.JavaScript:
		return string(val)
	case primitive.Symbol:
		return string(val)
	case bson.M:
		// shouldn't reach here via flattenBSONM, but guard anyway
		return val
	case bson.A:
		return []any(val)
	default:
		return v
	}
}
