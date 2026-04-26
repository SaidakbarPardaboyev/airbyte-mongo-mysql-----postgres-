package sync_mongo

import (
	"airbyte-service/core"
	"airbyte-service/plugins"
	sourcecommon "airbyte-service/sync/sources/common"
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDiscoverer struct {
	client     *mongo.Client
	database   string
	SampleSize int
}

func NewMongoDiscoverer(client *mongo.Client, database string, sampleSize int) *MongoDiscoverer {
	return &MongoDiscoverer{client: client, database: database, SampleSize: sampleSize}
}

func (d *MongoDiscoverer) Discover(ctx context.Context) (databaseScheme sourcecommon.DatabaseScheme, err error) {
	db := d.client.Database(d.database)

	// load collections list
	var collectionNames []string
	{
		collectionNames, err = db.ListCollectionNames(ctx, bson.M{})
		if err != nil {
			return
		}
	}

	// load collections' fields
	databaseScheme = sourcecommon.NewDatabaseScheme("", d.database)
	{
		for _, collectionName := range collectionNames {

			// describe collection. (collection with fields)
			var describedCollection *sourcecommon.Table
			describedCollection, err = d.describeCollection(ctx, db, collectionName)
			if err != nil {
				return
			}

			databaseScheme.Add(describedCollection)
		}
	}

	return
}

func (d *MongoDiscoverer) describeCollection(ctx context.Context, db *mongo.Database, collectionName string) (describedCollection *sourcecommon.Table, err error) {
	describedCollection = &sourcecommon.Table{
		Name:     collectionName,
		Database: d.database,
	}

	// discover fields
	var fieldsWithTypeMap = make(map[string]core.BSONType)
	{
		if err = d.discoverFields(ctx, db.Collection(collectionName), "", nil, fieldsWithTypeMap); err != nil {
			return
		}
	}

	// build collection
	{
		// list field names
		fieldNamesList := make([]string, 0, len(fieldsWithTypeMap))
		for fieldName := range fieldsWithTypeMap {
			if fieldName != "_id" {
				fieldNamesList = append(fieldNamesList, fieldName)
			}
		}
		plugins.SortStrings(fieldNamesList)
		fieldNamesList = append([]string{"_id"}, fieldNamesList...)

		// add fields to collection
		for _, k := range fieldNamesList {
			fieldType, ok := fieldsWithTypeMap[k]
			if !ok {
				continue
			}
			describedCollection.AddField(sourcecommon.Field{
				Name:       sourcecommon.MongoToPostgresFieldName(k),
				SourceType: fieldType,
				IsPrimary:  k == "_id",
			})
		}
	}

	return
}

func (d *MongoDiscoverer) discoverFields(ctx context.Context, mongoCollection *mongo.Collection, prefix string, unwindPaths []string, fieldsWithTypeMap map[string]core.BSONType) error {
	var pipeline mongo.Pipeline

	// Limit the number of documents inspected at every level of recursion.
	if d.SampleSize > 0 {
		pipeline = append(pipeline, bson.D{{
			Key:   "$sample",
			Value: bson.D{{Key: "size", Value: d.SampleSize}},
		}})
	}

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
			{Key: "hasObject", Value: bson.D{{Key: "$max", Value: bson.D{
				{Key: "$cond", Value: bson.A{
					bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$fields.v"}}, "object"}}},
					1, 0,
				}},
			}}}},
			{Key: "hasArray", Value: bson.D{{Key: "$max", Value: bson.D{
				{Key: "$cond", Value: bson.A{
					bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$fields.v"}}, "array"}}},
					1, 0,
				}},
			}}}},
		}}},
	)

	cursor, err := mongoCollection.Aggregate(ctx, pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var nestedObjPaths []string
	var nestedArrPaths []string

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
			nestedObjPaths = append(nestedObjPaths, key)

		case row.HasArray == 1:
			if _, ok := fieldsWithTypeMap[key]; !ok {
				fieldsWithTypeMap[key] = core.BSONTypeArray
			}
			nestedArrPaths = append(nestedArrPaths, key)

		default:
			if existing, ok := fieldsWithTypeMap[key]; !ok || existing == core.BSONTypeNull || existing == core.BSONTypeUnknown {
				fieldsWithTypeMap[key] = core.InferMongoType(row.Sample)
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return err
	}

	for _, path := range nestedObjPaths {
		if err := d.discoverFields(ctx, mongoCollection, path, unwindPaths, fieldsWithTypeMap); err != nil {
			return err
		}
	}

	for _, path := range nestedArrPaths {
		newUnwinds := make([]string, len(unwindPaths)+1)
		copy(newUnwinds, unwindPaths)
		newUnwinds[len(unwindPaths)] = path
		if err := d.discoverFields(ctx, mongoCollection, path, newUnwinds, fieldsWithTypeMap); err != nil {
			return err
		}
	}

	return nil
}
