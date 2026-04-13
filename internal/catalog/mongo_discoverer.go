package catalog

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// MongoDiscoverer discovers every field in each collection by scanning all documents.
// Uses $objectToArray + $group to find distinct fields — no random sampling, so no
// fields are missed regardless of how sparse they are across documents.
// Arrays of objects are automatically unwound so their sub-fields are discovered too.
type MongoDiscoverer struct {
	client   *mongo.Client
	database string
}

func NewMongoDiscoverer(client *mongo.Client, database string) *MongoDiscoverer {
	return &MongoDiscoverer{client: client, database: database}
}

func (d *MongoDiscoverer) Discover(ctx context.Context) (*Catalog, error) {
	db := d.client.Database(d.database)

	colls, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("mongo list collections: %w", err)
	}

	cat := NewCatalog()
	for _, coll := range colls {
		stream, err := d.describeCollection(ctx, db, coll)
		if err != nil {
			return nil, fmt.Errorf("mongo describe %s: %w", coll, err)
		}
		cat.Add(stream)
	}
	return cat, nil
}

func (d *MongoDiscoverer) describeCollection(ctx context.Context, db *mongo.Database, coll string) (*Stream, error) {
	merged := make(map[string]BSONType)

	if err := d.discoverKeys(ctx, db.Collection(coll), "", nil, merged); err != nil {
		return nil, err
	}

	stream := &Stream{
		Name:      coll,
		Namespace: d.database,
	}

	keys := make([]string, 0, len(merged))
	for k := range merged {
		if k != "_id" {
			keys = append(keys, k)
		}
	}
	sortStrings(keys)
	keys = append([]string{"_id"}, keys...)

	for _, k := range keys {
		raw, ok := merged[k]
		if !ok {
			continue
		}
		stream.Fields = append(stream.Fields, Field{
			Name:      k,
			NormType:  raw,
			IsPrimary: k == "_id",
		})
	}

	return stream, nil
}

// discoverKeys finds all distinct fields at the given prefix path using
// $objectToArray + $group across all documents.
//
//   - prefix: dot-notation path to the object being introspected ("" = $$ROOT)
//   - unwindPaths: ancestor array fields that must be $unwound before prefix is
//     accessible; accumulated as we recurse deeper into arrays
func (d *MongoDiscoverer) discoverKeys(ctx context.Context, coll *mongo.Collection, prefix string, unwindPaths []string, merged map[string]BSONType) error {
	var pipeline mongo.Pipeline

	// Unwind every ancestor array so nested paths are reachable
	for _, p := range unwindPaths {
		pipeline = append(pipeline, bson.D{{
			Key: "$unwind",
			Value: bson.D{
				{Key: "path", Value: "$" + p},
				{Key: "preserveNullAndEmptyArrays", Value: false},
			},
		}})
	}

	// When targeting a nested path, keep only documents where it is an object
	if prefix != "" {
		pipeline = append(pipeline, bson.D{{
			Key:   "$match",
			Value: bson.D{{Key: prefix, Value: bson.D{{Key: "$type", Value: "object"}}}},
		}})
	}

	// Explode the target object's fields into [{k, v}] entries
	projectSrc := "$$ROOT"
	if prefix != "" {
		projectSrc = "$" + prefix
	}

	pipeline = append(pipeline,
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "fields", Value: bson.D{{Key: "$objectToArray", Value: projectSrc}}},
		}}},
		bson.D{{Key: "$unwind", Value: bson.D{
			{Key: "path", Value: "$fields"},
			{Key: "preserveNullAndEmptyArrays", Value: false},
		}}},
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$fields.k"},
			{Key: "sample", Value: bson.D{{Key: "$first", Value: "$fields.v"}}},
			// hasObject: 1 if ANY document has this field as a nested object.
			// Needed because $first may return null even if other docs have an object.
			// Uses $type instead of $isObject for compatibility with MongoDB < 4.4.
			{Key: "hasObject", Value: bson.D{{Key: "$max", Value: bson.D{
				{Key: "$cond", Value: bson.A{
					bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$fields.v"}}, "object"}}},
					1, 0,
				}},
			}}}},
			// hasArray: 1 if ANY document has this field as an array.
			{Key: "hasArray", Value: bson.D{{Key: "$max", Value: bson.D{
				{Key: "$cond", Value: bson.A{
					bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$fields.v"}}, "array"}}},
					1, 0,
				}},
			}}}},
		}}},
	)

	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var nestedObjPaths []string // object fields: recurse with the same unwindPaths
	var nestedArrPaths []string // array fields: recurse after appending the path to unwindPaths

	for cursor.Next(ctx) {
		var row struct {
			Name      string `bson:"_id"`
			Sample    any    `bson:"sample"`
			HasObject int    `bson:"hasObject"`
			HasArray  int    `bson:"hasArray"`
		}
		if err := cursor.Decode(&row); err != nil {
			return err
		}

		key := row.Name
		if prefix != "" {
			key = prefix + "." + row.Name
		}

		switch {
		case row.HasObject == 1:
			// Nested object — recurse to discover its fields
			nestedObjPaths = append(nestedObjPaths, key)

		case row.HasArray == 1:
			// Array field — mark as array now; recursion will add sub-fields if
			// elements are objects. If elements are scalars, it stays as array.
			if _, ok := merged[key]; !ok {
				merged[key] = BSONTypeArray
			}
			nestedArrPaths = append(nestedArrPaths, key)

		default:
			if existing, ok := merged[key]; !ok || existing == BSONTypeNull || existing == BSONTypeUnknown {
				merged[key] = inferMongoType(row.Sample)
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return err
	}

	// Recurse into nested objects (no new unwind needed)
	for _, path := range nestedObjPaths {
		if err := d.discoverKeys(ctx, coll, path, unwindPaths, merged); err != nil {
			return err
		}
	}

	// Recurse into arrays: unwind the array field itself so we can introspect elements
	for _, path := range nestedArrPaths {
		newUnwinds := make([]string, len(unwindPaths)+1)
		copy(newUnwinds, unwindPaths)
		newUnwinds[len(unwindPaths)] = path
		if err := d.discoverKeys(ctx, coll, path, newUnwinds, merged); err != nil {
			return err
		}
	}

	return nil
}

// inferMongoType returns the raw BSON type name for the given Go value.
func inferMongoType(v any) BSONType {
	switch v.(type) {
	case nil:
		return BSONTypeNull
	case bool:
		return BSONTypeBool
	case int32:
		return BSONTypeInt32
	case int64:
		return BSONTypeInt64
	case float64:
		return BSONTypeDouble
	case primitive.Decimal128:
		return BSONTypeDecimal128
	case string:
		return BSONTypeString
	case primitive.ObjectID:
		return BSONTypeObjectID
	case primitive.Symbol:
		return BSONTypeSymbol
	case primitive.JavaScript:
		return BSONTypeJavaScript
	case primitive.CodeWithScope:
		return BSONTypeJavaScriptWithScope
	case primitive.Regex:
		return BSONTypeRegex
	case primitive.DateTime:
		return BSONTypeDate
	case primitive.Timestamp:
		return BSONTypeTimestamp
	case []byte, primitive.Binary:
		return BSONTypeBinData
	case bson.M, bson.D:
		return BSONTypeObject
	case bson.A, []any:
		return BSONTypeArray
	case primitive.DBPointer:
		return BSONTypeDBPointer
	case primitive.Undefined:
		return BSONTypeUndefined
	case primitive.MinKey:
		return BSONTypeMinKey
	case primitive.MaxKey:
		return BSONTypeMaxKey
	default:
		return BSONTypeUnknown
	}
}

// sortStrings sorts in place (avoids importing sort just for this)
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
