package sync_mongo

type Row map[string]any

type Message struct {
	Table string
	Row   Row
}

// func (r Row) Get(field string) (any, bool) {
// 	v, ok := r[field]
// 	return v, ok
// }

// func (r Row) GetString(field string) string {
// 	v, ok := r[field]
// 	if !ok || v == nil {
// 		return ""
// 	}
// 	switch val := v.(type) {
// 	case string:
// 		return val
// 	case []byte:
// 		return string(val)
// 	default:
// 		return fmt.Sprintf("%v", val)
// 	}
// }

// func (r Row) GetInt64(field string) int64 {
// 	v, ok := r[field]
// 	if !ok || v == nil {
// 		return 0
// 	}
// 	switch val := v.(type) {
// 	case int64:
// 		return val
// 	case int32:
// 		return int64(val)
// 	case int:
// 		return int64(val)
// 	case uint32:
// 		return int64(val)
// 	case uint64:
// 		if val > 1<<63-1 {
// 			return 1<<63 - 1
// 		}
// 		return int64(val)
// 	case float64:
// 		return int64(val)
// 	default:
// 		return 0
// 	}
// }

// func (r Row) GetTime(field string) time.Time {
// 	v, ok := r[field]
// 	if !ok || v == nil {
// 		return time.Time{}
// 	}
// 	switch val := v.(type) {
// 	case time.Time:
// 		return val
// 	case string:
// 		for _, layout := range []string{
// 			time.RFC3339Nano,
// 			time.RFC3339,
// 			"2006-01-02 15:04:05",
// 			"2006-01-02",
// 		} {
// 			if t, err := time.Parse(layout, val); err == nil {
// 				return t
// 			}
// 		}
// 		return time.Time{}
// 	case int64:
// 		return time.Unix(val, 0)
// 	default:
// 		return time.Time{}
// 	}
// }

// func (r Row) IsNil(field string) bool {
// 	v, ok := r[field]
// 	return !ok || v == nil
// }

// func (r Row) Fields() []string {
// 	keys := make([]string, 0, len(r))
// 	for k := range r {
// 		keys = append(keys, k)
// 	}
// 	return keys
// }

// func (r Row) Clone() Row {
// 	out := make(Row, len(r))
// 	for k, v := range r {
// 		switch val := v.(type) {
// 		case []byte:
// 			cp := make([]byte, len(val))
// 			copy(cp, val)
// 			out[k] = cp
// 		default:
// 			out[k] = v
// 		}
// 	}
// 	return out
// }

// func (r Row) Clear() {
// 	for k := range r {
// 		delete(r, k)
// 	}
// }

// func (r Row) String() string {
// 	parts := make([]string, 0, len(r))
// 	for k, v := range r {
// 		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
// 	}
// 	return "{" + strings.Join(parts, " ") + "}"
// }
