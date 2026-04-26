package sync_mongo

import (
	"airbyte-service/core"
	sourcecommon "airbyte-service/sync/sources/common"
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func ReadCollection(
	ctx context.Context,
	client *mongo.Client,
	dbName string,
	table *sourcecommon.Table,
	startTime time.Time,
	endTime time.Time,
) (<-chan Message, error) {
	collection := client.Database(dbName).Collection(table.Name)
	cursor, err := collection.Find(ctx, buildFilter(table, table.WriteMode, startTime, endTime))
	if err != nil {
		return nil, fmt.Errorf("mongo find %s.%s: %w", dbName, table.Name, err)
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

func emitRows(ctx context.Context, doc bson.M, table *sourcecommon.Table, ch chan<- Message) {
	raw := make(Row, len(table.Fields))
	flattenBSONM(doc, "", table.FieldMap, raw)

	parentID := raw["id"]

	// map each tables fields: mao[table_name]fields
	tableFields := make(map[string][]string)
	{
		for _, f := range table.Fields {
			if f.TableName != "" {
				tableFields[f.TableName] = append(tableFields[f.TableName], f.Name)
			}
		}
	}

	// take parent's fields data
	parentRaw := make(Row)
	{
		for _, name := range tableFields[table.Name] {
			postgresFieldName := sourcecommon.MongoToPostgresFieldName(name)
			parentRaw[postgresFieldName] = raw[postgresFieldName]
		}
	}

	// send parend raw to channel
	{
		select {
		case ch <- Message{Table: table.Name, Row: parentRaw}:
		case <-ctx.Done():
			return
		}
	}

	// take inner tables data and send it to channel
	for tableName, fieldNames := range tableFields {
		// skip parent table
		if tableName == table.Name {
			continue
		}

		// take current innner table raw list
		var raws bson.A
		{
			rawArray, ok := doc[tableName] // e.g. doc["items"]
			if !ok {
				continue
			}
			raws, ok = rawArray.(bson.A)
			if !ok {
				continue
			}
		}

		// create a map for child table fields
		childFieldSet := make(map[string]sourcecommon.Field)
		prefix := tableName + "."
		{
			for _, name := range fieldNames {
				postgresFieldName := sourcecommon.MongoToPostgresFieldName(name)
				if strings.HasPrefix(postgresFieldName, prefix) {
					childFieldSet[postgresFieldName] = table.FieldMap[postgresFieldName]
				}
			}
		}

		// take each raw of innner table and send its' formated raw to channel
		for _, elem := range raws {
			elemDoc, ok := elem.(bson.M)
			if !ok {
				continue
			}

			childRow := make(Row)
			parentIDFieldName := table.Name + "_id"
			childRow[parentIDFieldName] = parentID

			childFlat := make(Row)
			flattenBSONM(elemDoc, tableName, childFieldSet, childFlat)
			for k, v := range childFlat {
				stripped := strings.TrimPrefix(k, prefix)
				childRow[stripped] = v
			}

			select {
			case ch <- Message{Table: tableName, Row: childRow}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func flattenBSONM(m bson.M, prefix string, fieldSet map[string]sourcecommon.Field, row Row) {
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		key = sourcecommon.MongoToPostgresFieldName(key)

		switch val := v.(type) {
		case bson.M:
			flattenBSONM(val, key, fieldSet, row)

		case bson.D:
			flattenBSONM(bsonDToM(val), key, fieldSet, row)

		case bson.A:
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

func normalizeBSONValue(v any) any {
	switch val := v.(type) {
	case primitive.ObjectID:
		return val.Hex()
	case primitive.DateTime:
		return val.Time().UTC()
	case primitive.Timestamp:
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
		return val
	case bson.A:
		return []any(val)
	default:
		return v
	}
}

func buildFilter(table *sourcecommon.Table, writeMode core.WriteMode, startTime, endTime time.Time) bson.M {
	switch writeMode {
	case core.WriteModeAppend:
		if table.CreatedTimeField == "" {
			return bson.M{}
		}
		f := bson.M{"$lte": endTime}
		if !startTime.IsZero() {
			f["$gte"] = startTime.Add(time.Minute)
		}
		return bson.M{table.CreatedTimeField: f}

	case core.WriteModeUpsert:
		timeRange := bson.M{"$lte": endTime}
		if !startTime.IsZero() {
			timeRange["$gte"] = startTime.Add(time.Minute)
		}
		or := bson.A{}
		if table.CreatedTimeField != "" {
			or = append(or, bson.M{table.CreatedTimeField: timeRange})
		}
		if table.UpdatedTimeField != "" {
			or = append(or, bson.M{table.UpdatedTimeField: timeRange})
		}
		if table.DeletedTimeField != "" {
			or = append(or, bson.M{table.DeletedTimeField: timeRange})
		}
		if len(or) > 0 {
			return bson.M{"$or": or}
		}
	}
	return bson.M{}
}
