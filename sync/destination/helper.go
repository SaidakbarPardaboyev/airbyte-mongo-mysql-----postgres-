package sync_destination

import (
	sourcecommon "airbyte-service/sync/sources/common"
	"encoding/json"
	"strings"
)

func resolvedColumns(table *sourcecommon.Table, tableName string) []string {
	var cols []string

	// remove inner table name from inner table's fields like items.id -- > id
	prefix := tableName + "."
	for _, f := range table.Fields {
		if f.TableName == tableName {
			name := strings.TrimPrefix(f.Name, prefix)
			cols = append(cols, name)
		}
	}
	return cols
}

func normalizeValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		return val
	case map[string]any, []any:
		return marshalJSON(val)
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		if val > 1<<63-1 {
			return int64(1<<63 - 1)
		}
		return int64(val)
	default:
		return val
	}
}

func marshalJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
