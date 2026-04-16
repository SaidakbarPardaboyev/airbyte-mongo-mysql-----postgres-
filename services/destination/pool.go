package destination

import (
	"context"
	"fmt"
	"runtime"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool creates a pgxpool tuned for bulk COPY workloads.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	// For COPY workloads: one connection per CPU core is usually optimal.
	// Each COPY call fully saturates one connection with binary data.
	maxConns := int32(runtime.NumCPU())
	if maxConns < 2 {
		maxConns = 2
	}
	if maxConns > 8 {
		maxConns = 8 // Postgres starts thrashing above ~8 concurrent COPYs
	}

	cfg.MaxConns = maxConns
	cfg.MinConns = 1

	// pgx binary format is faster than text — enable it globally
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return pool, nil
}
