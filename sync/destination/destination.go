package sync_destination

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"airbyte-service/core"
	"airbyte-service/database/postgres"
	sourcecommon "airbyte-service/sync/sources/common"
	sourcemongo "airbyte-service/sync/sources/mongo"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type WriteResult struct {
	RowsCopied int64
	Duration   time.Duration
	Batches    int
}

type Writer struct {
	pool       *pgxpool.Pool
	tableName  string
	writeMode  core.WriteMode
	primaryKey []string
	logger     *slog.Logger
}

func NewWriter(pool *pgxpool.Pool, tableName string, writeMode core.WriteMode, primaryKey []string, logger *slog.Logger) *Writer {
	return &Writer{
		pool:       pool,
		tableName:  tableName,
		writeMode:  writeMode,
		primaryKey: primaryKey,
		logger:     logger,
	}
}

// Write consumes all rows from ch and writes them to Postgres as fast as possible.
// The caller closes ch when reading is done.
func (w *Writer) Write(ctx context.Context, table *sourcecommon.Table, ch <-chan sourcemongo.Row) (*WriteResult, error) {
	cols := resolvedColumns(table, w.tableName)

	switch w.writeMode {
	case core.WriteModeOverwrite:
		return w.writeOverwrite(ctx, cols, ch)
	case core.WriteModeUpsert:
		return w.writeUpsert(ctx, cols, ch)
	default:
		return w.writeAppend(ctx, cols, ch)
	}
}

func (w *Writer) writeAppend(ctx context.Context, cols []string, ch <-chan sourcemongo.Row) (*WriteResult, error) {
	start := time.Now()
	result := &WriteResult{}

	for {
		batch, done := drainBatch(ch, core.BatchSize)
		if len(batch) == 0 {
			break
		}

		n, err := w.copyBatch(ctx, postgres.QualifiedTable(w.tableName), cols, batch)
		result.RowsCopied += n
		result.Batches++

		if err != nil {
			return result, fmt.Errorf("append batch %d: %w", result.Batches, err)
		}

		w.logger.Debug("batch written",
			"table", w.tableName,
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

func (w *Writer) writeOverwrite(ctx context.Context, cols []string, ch <-chan sourcemongo.Row) (*WriteResult, error) {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", postgres.QualifiedTable(w.tableName)))
	if err != nil {
		return nil, fmt.Errorf("truncate: %w", err)
	}

	conn.Release()

	return w.writeAppend(ctx, cols, ch)
}

func (w *Writer) writeUpsert(ctx context.Context, cols []string, ch <-chan sourcemongo.Row) (*WriteResult, error) {
	if len(w.primaryKey) == 0 {
		return nil, fmt.Errorf("upsert mode requires PrimaryKey to be set")
	}

	start := time.Now()
	result := &WriteResult{}

	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire conn for upsert: %w", err)
	}
	defer conn.Release()

	tmp := postgres.QualifiedTable(postgres.TempTable(w.tableName))
	target := postgres.QualifiedTable(w.tableName)

	createTmp := fmt.Sprintf(`
		CREATE TEMP TABLE %s
		(LIKE %s INCLUDING DEFAULTS)
		ON COMMIT DROP
	`, tmp, target)

	_, err = conn.Exec(ctx, createTmp)
	if err != nil {
		return nil, fmt.Errorf("create temp table: %w", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for {
		batch, done := drainBatch(ch, core.BatchSize)
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

	mergeSQL := w.buildMergeSQL(tmp, target, cols)
	_, err = tx.Exec(ctx, mergeSQL)
	if err != nil {
		return result, fmt.Errorf("merge from temp: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit upsert: %w", err)
	}

	result.Duration = time.Since(start)
	return result, nil
}

func (w *Writer) buildMergeSQL(tmpTable, targetTable string, cols []string) string {
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = pgx.Identifier{c}.Sanitize()
	}
	colList := strings.Join(quotedCols, ", ")

	pkCols := make([]string, len(w.primaryKey))
	for i, pk := range w.primaryKey {
		pkCols[i] = pgx.Identifier{pk}.Sanitize()
	}
	conflictTarget := strings.Join(pkCols, ", ")

	pkSet := make(map[string]bool, len(w.primaryKey))
	for _, pk := range w.primaryKey {
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

func (w *Writer) copyBatch(ctx context.Context, table string, cols []string, batch []sourcemongo.Row) (int64, error) {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	return w.copyBatchTx(ctx, conn.Conn(), table, cols, batch)
}

func (w *Writer) copyBatchTx(
	ctx context.Context,
	conn interface {
		CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	},
	table string,
	cols []string,
	batch []sourcemongo.Row,
) (int64, error) {
	rows := make([][]any, len(batch))
	for i, row := range batch {
		vals := make([]any, len(cols))
		for j, col := range cols {
			vals[j] = normalizeValue(row[col])
		}
		rows[i] = vals
	}

	n, err := conn.CopyFrom(ctx, pgx.Identifier{w.tableName}, cols, pgx.CopyFromRows(rows))
	if err != nil {
		return n, fmt.Errorf("COPY %s: %w", table, err)
	}
	return n, nil
}

func drainBatch(ch <-chan sourcemongo.Row, n int) ([]sourcemongo.Row, bool) {
	batch := make([]sourcemongo.Row, 0, n)
	for len(batch) < n {
		select {
		case row, ok := <-ch:
			if !ok {
				return batch, true
			}
			batch = append(batch, row)
		default:
			if len(batch) > 0 {
				return batch, false
			}
			row, ok := <-ch
			if !ok {
				return batch, true
			}
			batch = append(batch, row)
		}
	}
	return batch, false
}
