package destination

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"syncer/services/sources/common"
	"syncer/services/sources/mongo"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ────────────────────────────────────────────────────────────────────────────
// Config
// ────────────────────────────────────────────────────────────────────────────

type WriteMode string

const (
	WriteModeAppend    WriteMode = "append"    // COPY straight into target table
	WriteModeOverwrite WriteMode = "overwrite" // TRUNCATE then COPY
	WriteModeUpsert    WriteMode = "upsert"    // COPY into temp, then MERGE/INSERT ON CONFLICT
)

type WriterConfig struct {
	Schema     string
	Table      string
	Mode       WriteMode
	PrimaryKey []string      // required for upsert mode
	BatchSize  int           // rows per COPY call (default 5000)
	Timeout    time.Duration // per-batch timeout
}

func (c *WriterConfig) setDefaults() {
	if c.BatchSize == 0 {
		c.BatchSize = 5_000
	}
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Minute
	}
}

func (c *WriterConfig) qualifiedTable() string {
	if c.Schema == "" {
		return pgx.Identifier{c.Table}.Sanitize()
	}
	return pgx.Identifier{c.Schema, c.Table}.Sanitize()
}

func (c *WriterConfig) tempTable() string {
	return fmt.Sprintf("_gosync_tmp_%s", c.Table)
}

// ────────────────────────────────────────────────────────────────────────────
// Writer
// ────────────────────────────────────────────────────────────────────────────

type WriteResult struct {
	RowsCopied int64
	Duration   time.Duration
	Batches    int
}

type Writer struct {
	pool   *pgxpool.Pool
	cfg    WriterConfig
	logger *slog.Logger
}

func NewWriter(pool *pgxpool.Pool, cfg WriterConfig, logger *slog.Logger) *Writer {
	cfg.setDefaults()
	return &Writer{pool: pool, cfg: cfg, logger: logger}
}

// Write consumes all rows from ch and writes them to Postgres as fast as possible.
// The caller closes ch when reading is done.
func (w *Writer) Write(ctx context.Context, stream *common.Table, ch <-chan mongo.Row) (*WriteResult, error) {
	// resolve the ordered column list once — order matters for COPY
	cols := resolvedColumns(stream, w.cfg.Table)

	switch w.cfg.Mode {
	case WriteModeOverwrite:
		return w.writeOverwrite(ctx, cols, ch)
	case WriteModeUpsert:
		return w.writeUpsert(ctx, cols, ch)
	default:
		return w.writeAppend(ctx, cols, ch)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Mode: Append — COPY directly into target table
// ────────────────────────────────────────────────────────────────────────────

func (w *Writer) writeAppend(ctx context.Context, cols []string, ch <-chan mongo.Row) (*WriteResult, error) {
	start := time.Now()
	result := &WriteResult{}

	for {
		batch, done := drainBatch(ch, w.cfg.BatchSize)
		if len(batch) == 0 {
			break
		}

		n, err := w.copyBatch(ctx, w.cfg.qualifiedTable(), cols, batch)
		result.RowsCopied += n
		result.Batches++

		if err != nil {
			return result, fmt.Errorf("append batch %d: %w", result.Batches, err)
		}

		w.logger.Debug("batch written",
			"table", w.cfg.Table,
			"rows", n,
			"total", result.RowsCopied,
		)

		if done {
			break
		}
	}

	result.Duration = time.Since(start)
	return result, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Mode: Overwrite — TRUNCATE then COPY
// ────────────────────────────────────────────────────────────────────────────

func (w *Writer) writeOverwrite(ctx context.Context, cols []string, ch <-chan mongo.Row) (*WriteResult, error) {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	// TRUNCATE first — RESTART IDENTITY resets sequences
	_, err = conn.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", w.cfg.qualifiedTable()))
	if err != nil {
		return nil, fmt.Errorf("truncate: %w", err)
	}

	// release back to pool before copying — copyBatch acquires its own conn
	conn.Release()

	return w.writeAppend(ctx, cols, ch)
}

// ────────────────────────────────────────────────────────────────────────────
// Mode: Upsert — COPY into temp table, then MERGE into target
//
// Flow:
//   1. CREATE TEMP TABLE (same shape as target, no constraints)
//   2. COPY all rows into temp
//   3. INSERT INTO target ... SELECT FROM temp ON CONFLICT (...) DO UPDATE
//   4. DROP TEMP TABLE
//
// This is atomic: either everything merges or nothing does.
// ────────────────────────────────────────────────────────────────────────────

func (w *Writer) writeUpsert(ctx context.Context, cols []string, ch <-chan mongo.Row) (*WriteResult, error) {
	if len(w.cfg.PrimaryKey) == 0 {
		return nil, fmt.Errorf("upsert mode requires PrimaryKey to be set")
	}

	start := time.Now()
	result := &WriteResult{}

	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire conn for upsert: %w", err)
	}
	defer conn.Release()

	tmp := pgx.Identifier{w.cfg.tempTable()}.Sanitize()
	target := w.cfg.qualifiedTable()

	// 1. Create temp table — ON COMMIT DROP so it auto-cleans even on crash
	createTmp := fmt.Sprintf(`
		CREATE TEMP TABLE %s
		(LIKE %s INCLUDING DEFAULTS)
		ON COMMIT DROP
	`, tmp, target)

	_, err = conn.Exec(ctx, createTmp)
	if err != nil {
		return nil, fmt.Errorf("create temp table: %w", err)
	}

	// 2. COPY all batches into temp table
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for {
		batch, done := drainBatch(ch, w.cfg.BatchSize)
		if len(batch) == 0 {
			break
		}

		n, err := w.copyBatchTx(ctx, tx, tmp, cols, batch)
		result.RowsCopied += n
		result.Batches++

		if err != nil {
			return result, fmt.Errorf("copy to temp batch %d: %w", result.Batches, err)
		}

		if done {
			break
		}
	}

	// 3. Merge: INSERT ... SELECT FROM temp ON CONFLICT DO UPDATE
	mergeSQL := w.buildMergeSQL(tmp, target, cols)
	_, err = tx.Exec(ctx, mergeSQL)
	if err != nil {
		return result, fmt.Errorf("merge from temp: %w", err)
	}

	// 4. Commit — temp table drops automatically (ON COMMIT DROP)
	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit upsert: %w", err)
	}

	result.Duration = time.Since(start)
	return result, nil
}

// buildMergeSQL generates the INSERT ... ON CONFLICT upsert statement.
// e.g.:
//
//	INSERT INTO "public"."users" ("id","email","updated_at")
//	SELECT "id","email","updated_at" FROM _gosync_tmp_users
//	ON CONFLICT ("id") DO UPDATE SET
//	  "email" = EXCLUDED."email",
//	  "updated_at" = EXCLUDED."updated_at"
func (w *Writer) buildMergeSQL(tmpTable, targetTable string, cols []string) string {
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = pgx.Identifier{c}.Sanitize()
	}
	colList := strings.Join(quotedCols, ", ")

	// primary key conflict target
	pkCols := make([]string, len(w.cfg.PrimaryKey))
	for i, pk := range w.cfg.PrimaryKey {
		pkCols[i] = pgx.Identifier{pk}.Sanitize()
	}
	conflictTarget := strings.Join(pkCols, ", ")

	// SET clause — exclude PK columns from update
	pkSet := make(map[string]bool, len(w.cfg.PrimaryKey))
	for _, pk := range w.cfg.PrimaryKey {
		pkSet[pk] = true
	}
	var setClauses []string
	for _, c := range cols {
		if !pkSet[c] {
			qc := pgx.Identifier{c}.Sanitize()
			setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", qc, qc))
		}
	}

	return fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s FROM %s
		ON CONFLICT (%s) DO UPDATE SET %s
	`,
		targetTable, colList,
		colList, tmpTable,
		conflictTarget,
		strings.Join(setClauses, ", "),
	)
}

// ────────────────────────────────────────────────────────────────────────────
// COPY core — this is the fast path
// ────────────────────────────────────────────────────────────────────────────

// copyBatch acquires a connection from the pool and runs COPY for one batch.
func (w *Writer) copyBatch(ctx context.Context, table string, cols []string, batch []mongo.Row) (int64, error) {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	return w.copyBatchTx(ctx, conn.Conn(), table, cols, batch)
}

// copyBatchTx runs a COPY using an existing connection or transaction.
// pgx.CopyFromRows feeds rows into the Postgres COPY protocol directly —
// no SQL string building, no parameter binding, no per-row round trips.
func (w *Writer) copyBatchTx(
	ctx context.Context,
	conn interface {
		CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	},
	table string,
	cols []string,
	batch []mongo.Row,
) (int64, error) {
	// build the row source — pgx.CopyFromRows takes [][]any
	rows := make([][]any, len(batch))
	for i, row := range batch {
		vals := make([]any, len(cols))
		for j, col := range cols {
			vals[j] = normalizeValue(row[col])
		}
		rows[i] = vals
	}

	// pgx.Identifier handles quoting: {"public", "users"} → "public"."users"
	var tableIdent pgx.Identifier
	if w.cfg.Schema != "" {
		tableIdent = pgx.Identifier{w.cfg.Schema, w.cfg.Table}
	} else {
		tableIdent = pgx.Identifier{w.cfg.Table}
	}

	n, err := conn.CopyFrom(ctx, tableIdent, cols, pgx.CopyFromRows(rows))
	if err != nil {
		return n, fmt.Errorf("COPY %s: %w", table, err)
	}
	return n, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

// drainBatch reads up to n rows from ch without blocking if ch is empty.
// Returns (batch, true) when ch is closed and fully drained.
func drainBatch(ch <-chan mongo.Row, n int) ([]mongo.Row, bool) {
	batch := make([]mongo.Row, 0, n)
	for len(batch) < n {
		select {
		case row, ok := <-ch:
			if !ok {
				return batch, true // channel closed
			}
			batch = append(batch, row)
		default:
			if len(batch) > 0 {
				return batch, false // partial batch, ch not yet closed
			}
			// ch is empty but not closed — block until next row arrives
			row, ok := <-ch
			if !ok {
				return batch, true
			}
			batch = append(batch, row)
		}
	}
	return batch, false
}

// resolvedColumns returns a stable, ordered column list for fields belonging to tableName.
func resolvedColumns(stream *common.Table, tableName string) []string {
	var cols []string
	for _, f := range stream.Fields {
		if f.TableName == tableName {
			cols = append(cols, f.Name)
		}
	}
	return cols
}

// normalizeValue converts source values to Postgres-compatible types.
// pgx handles most Go primitives natively; this covers edge cases.
func normalizeValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		// MongoDB/MySQL raw bytes — return as-is, pgx maps to bytea
		return val
	case map[string]any, []any:
		// nested document / array — serialize to JSONB
		return marshalJSON(val)
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		// guard against overflow
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
