package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"airbyte-service/core"
	// airbytedbservice "airbyte-service/service/airbyte-database"
	// airbytefieldservice "airbyte-service/service/airbyte-field"
	// airbytetableservice "airbyte-service/service/airbyte-table"
)

// Job holds metadata about a single sync execution.
type Job struct {
	ID        int
	StartedAt time.Time
	EndedAt   time.Time
	Duration  time.Duration
	Status    core.Status
	Err       error
}

// Scheduler repeatedly runs all sync jobs at a fixed interval, similar to Airbyte's Basic Schedule.
type Scheduler struct {
	// databaseService airbytedbservice.Service
	// tableService    airbytetableservice.Service
	// fieldService    airbytefieldservice.Service

	interval time.Duration
	pool     *pgxpool.Pool
	sources  []Source
	logger   *slog.Logger
	mu       sync.Mutex
	history  []*Job
	nextID   int
	running  bool
}

// New creates a Scheduler. When cfg.Interval is 0, Run executes the jobs once and returns.
func New(
	interval time.Duration,
	pool *pgxpool.Pool,
	// databaseService airbytedbservice.Service,
	// tableService airbytetableservice.Service,
	// fieldService airbytefieldservice.Service,
	logger *slog.Logger,
) *Scheduler {
	return &Scheduler{
		interval:        interval,
		pool:            pool,
		// databaseService: databaseService,
		// tableService:    tableService,
		// fieldService:    fieldService,
		logger:          logger,
	}
}

// Run triggers the first sync immediately, then repeats every interval.
// Blocks until ctx is cancelled (or until the single run completes when interval == 0).
// If a sync is still running when the next tick fires, that tick is skipped.
func (s *Scheduler) Run(ctx context.Context) error {
	s.trigger(ctx)

	if s.interval == 0 {
		return nil
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.trigger(ctx)
		}
	}
}

func (s *Scheduler) trigger(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.logger.Warn("scheduler: previous sync still running, skipping tick")
		s.mu.Unlock()
		return
	}
	s.running = true
	s.nextID++
	job := &Job{ID: s.nextID, StartedAt: time.Now(), Status: core.StatusRunning}
	s.history = append(s.history, job)
	s.mu.Unlock()

	s.logger.Info("sync started", "job_id", job.ID, "started_at", job.StartedAt.Format(time.RFC3339))

	err := s.runSync(ctx)

	s.mu.Lock()
	job.EndedAt = time.Now()
	job.Duration = job.EndedAt.Sub(job.StartedAt)
	if err != nil {
		job.Status = core.StatusFailed
		job.Err = err
		s.logger.Error("sync failed", "job_id", job.ID, "duration", job.Duration, "error", err)
	} else {
		job.Status = core.StatusSucceeded
		s.logger.Info("sync succeeded", "job_id", job.ID, "duration", job.Duration)
	}
	s.running = false
	s.mu.Unlock()
}

// History returns a snapshot of all recorded sync jobs (oldest first).
func (s *Scheduler) History() []*Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Job, len(s.history))
	copy(out, s.history)
	return out
}
