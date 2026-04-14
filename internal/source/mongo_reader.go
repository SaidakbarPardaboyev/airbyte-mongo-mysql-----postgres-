package source

import (
	"context"
	"fmt"
	"time"

	"syncer/internal/catalog"
	"syncer/internal/record"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// ReadCollection opens a cursor on collName and streams every document as a
// flattened record.Row into the returned channel.  The channel is closed when
// all documents have been sent or ctx is cancelled.
//
// Nested objects are expanded using dot-notation keys ("account.id").
// Array fields are kept as []any so normalizeValue in the writer serialises
// them to JSONB.  Only fields present in stream.Fields are emitted.
func ReadCollection(
	ctx context.Context,
	client *mongo.Client,
	dbName, collName string,
	stream *catalog.Stream,
) (<-chan record.Row, error) {
	coll := client.Database(dbName).Collection(collName)

	cursor, err := coll.Find(ctx, bson.M{
		"is_deleted": false,
	})
	if err != nil {
		return nil, fmt.Errorf("mongo find %s.%s: %w", dbName, collName, err)
	}

	ch := make(chan record.Row, 256)

	go func() {
		defer close(ch)
		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			var doc bson.M
			if err := cursor.Decode(&doc); err != nil {
				// skip undecodable documents; a real pipeline would log here
				continue
			}

			row := make(record.Row, len(stream.Fields))
			flattenBSONM(doc, "", stream.FieldMap, row)

			select {
			case ch <- row:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

// flattenBSONM recursively walks a bson.M and writes leaf values into row
// using dot-notation keys.  Only keys present in fieldSet are written.
func flattenBSONM(m bson.M, prefix string, fieldSet map[string]catalog.Field, row record.Row) {
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
	fmt.Printf("data: %+v\n", row)
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
