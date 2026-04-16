package core

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// BSONType is the raw MongoDB BSON type name, matching the names returned by
// MongoDB's $type aggregation operator.
type BSONType string

const (
	BSONTypeDouble              BSONType = "float64"
	BSONTypeString              BSONType = "string"
	BSONTypeObject              BSONType = "object"
	BSONTypeArray               BSONType = "array"
	BSONTypeBinData             BSONType = "[]byte"
	BSONTypeUndefined           BSONType = "primitive.Undefined" // deprecated
	BSONTypeObjectID            BSONType = "primitive.ObjectID"
	BSONTypeBool                BSONType = "bool"
	BSONTypeDate                BSONType = "DateTime"
	BSONTypeNull                BSONType = "null"
	BSONTypeRegex               BSONType = "primitive.Regex"
	BSONTypeDBPointer           BSONType = "primitive.DBPointer" // deprecated
	BSONTypeJavaScript          BSONType = "primitive.JavaScript"
	BSONTypeSymbol              BSONType = "primitive.Symbol" // deprecated
	BSONTypeJavaScriptWithScope BSONType = "primitive.CodeWithScope"
	BSONTypeInt32               BSONType = "Int32"
	BSONTypeTimestamp           BSONType = "timestamp"
	BSONTypeInt64               BSONType = "int64"
	BSONTypeDecimal128          BSONType = "primitive.Decimal128"
	BSONTypeMinKey              BSONType = "primitive.MinKey"
	BSONTypeMaxKey              BSONType = "primitive.MaxKey"
	BSONTypeUnknown             BSONType = "unknown"
)

// inferMongoType returns the raw BSON type name for the given Go value.
func InferMongoType(v any) BSONType {
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
