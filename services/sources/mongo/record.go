package mongo

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// ────────────────────────────────────────────────────────────────────────────
// Core types
// ────────────────────────────────────────────────────────────────────────────

// Row is a schema-agnostic row — field names map to raw scanned values.
// Using map[string]any avoids pre-defining structs per table and works
// identically for MySQL (database/sql), MongoDB (bson.M), and any future source.
type Row map[string]any

// Operation describes what happened to a row — used in CDC mode.
type Operation string

const (
	OpRead   Operation = "read"   // full refresh / incremental scan
	OpInsert Operation = "insert" // CDC: new row
	OpUpdate Operation = "update" // CDC: updated row
	OpDelete Operation = "delete" // CDC: deleted row
)

// Message is the unit flowing through the pipeline channel.
// It wraps a Row with stream identity and CDC metadata.
type Message struct {
	Stream    string    // source table / collection name
	Namespace string    // source schema / database name
	Row       Row       // the actual data
	Op        Operation // what kind of change this is
	EmittedAt time.Time // when the record was read from the source
	LSN       string    // optional: Postgres WAL LSN / MySQL binlog position
}

// ────────────────────────────────────────────────────────────────────────────
// Row helpers
// ────────────────────────────────────────────────────────────────────────────

// Get returns a field value and whether it existed.
func (r Row) Get(field string) (any, bool) {
	v, ok := r[field]
	return v, ok
}

// GetString returns a field as string, empty string if missing or wrong type.
func (r Row) GetString(field string) string {
	v, ok := r[field]
	if !ok || v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// GetInt64 returns a field as int64, 0 if missing or not convertible.
func (r Row) GetInt64(field string) int64 {
	v, ok := r[field]
	if !ok || v == nil {
		return 0
	}
	switch val := v.(type) {
	case int64:
		return val
	case int32:
		return int64(val)
	case int:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		if val > 1<<63-1 {
			return 1<<63 - 1
		}
		return int64(val)
	case float64:
		return int64(val)
	default:
		return 0
	}
}

// GetTime returns a field as time.Time, zero value if missing or wrong type.
func (r Row) GetTime(field string) time.Time {
	v, ok := r[field]
	if !ok || v == nil {
		return time.Time{}
	}
	switch val := v.(type) {
	case time.Time:
		return val
	case string:
		// try common formats
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, val); err == nil {
				return t
			}
		}
		return time.Time{}
	case int64:
		return time.Unix(val, 0)
	default:
		return time.Time{}
	}
}

// IsNil returns true if the field is missing or explicitly nil.
func (r Row) IsNil(field string) bool {
	v, ok := r[field]
	return !ok || v == nil
}

// Fields returns all field names in the row (order is random — map semantics).
func (r Row) Fields() []string {
	keys := make([]string, 0, len(r))
	for k := range r {
		keys = append(keys, k)
	}
	return keys
}

// Clone makes a shallow copy of the row.
// Scalar values (int, string, time.Time, bool) are value types so they're safe.
// []byte slices are deep-copied to avoid shared backing arrays.
func (r Row) Clone() Row {
	out := make(Row, len(r))
	for k, v := range r {
		switch val := v.(type) {
		case []byte:
			cp := make([]byte, len(val))
			copy(cp, val)
			out[k] = cp
		default:
			out[k] = v
		}
	}
	return out
}

// Clear removes all entries so a pooled Row can be reused.
func (r Row) Clear() {
	for k := range r {
		delete(r, k)
	}
}

// String returns a compact debug representation.
func (r Row) String() string {
	parts := make([]string, 0, len(r))
	for k, v := range r {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	return "{" + strings.Join(parts, " ") + "}"
}

// ────────────────────────────────────────────────────────────────────────────
// Pool — reusable Row maps to reduce GC pressure during streaming
// ────────────────────────────────────────────────────────────────────────────

// Pool is a sync.Pool of pre-allocated Row maps.
// Get a row, fill it, send it through the channel.
// After the writer flushes the batch, Put each row back.
//
// This keeps the GC from seeing millions of short-lived map allocations
// during a large full-refresh sync.
type Pool struct {
	p sync.Pool
}

// NewPool creates a Pool with maps pre-allocated to initialCap fields.
func NewPool(initialCap int) *Pool {
	if initialCap <= 0 {
		initialCap = 16
	}
	return &Pool{
		p: sync.Pool{
			New: func() any {
				return make(Row, initialCap)
			},
		},
	}
}

// Get returns a clean Row from the pool.
func (p *Pool) Get() Row {
	return p.p.Get().(Row)
}

// Put clears the row and returns it to the pool.
func (p *Pool) Put(r Row) {
	r.Clear()
	p.p.Put(r)
}

// ────────────────────────────────────────────────────────────────────────────
// Batch — a fixed-size slice of rows with its own lifecycle
// ────────────────────────────────────────────────────────────────────────────

// Batch is a slice of Rows collected before a write flush.
// Keeping it as a named type lets us add methods (e.g. ToColumnSlices).
type Batch []Row

// ToColumnSlices converts a batch into the [][]any format that
// pgx.CopyFromRows expects — one outer slice per row, one inner value per column.
// cols must be in the same order you want COPY to use.
func (b Batch) ToColumnSlices(cols []string, normalize func(any) any) [][]any {
	out := make([][]any, len(b))
	for i, row := range b {
		vals := make([]any, len(cols))
		for j, col := range cols {
			v := row[col]
			if normalize != nil {
				v = normalize(v)
			}
			vals[j] = v
		}
		out[i] = vals
	}
	return out
}

// ReturnToPool returns every row in the batch back to pool p.
// Call this after the batch has been written to the destination.
func (b Batch) ReturnToPool(p *Pool) {
	for _, row := range b {
		p.Put(row)
	}
}
