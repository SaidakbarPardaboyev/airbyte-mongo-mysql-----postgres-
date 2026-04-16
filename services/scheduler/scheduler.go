package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Status string

const (
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

// Job holds metadata about a single sync execution.
type Job struct {
	ID        int
	StartedAt time.Time
	EndedAt   time.Time
	Duration  time.Duration
	Status    Status
	Err       error
}

// Scheduler repeatedly calls syncFn at a fixed interval, similar to Airbyte's Basic Schedule.
type Scheduler struct {
	interval time.Duration
	syncFn   func(ctx context.Context) error
	logger   *slog.Logger

	mu      sync.Mutex
	history []*Job
	nextID  int
	running bool
}

// New creates a Scheduler. When interval is 0, Run executes syncFn once and returns.
func New(interval time.Duration, syncFn func(ctx context.Context) error, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		interval: interval,
		syncFn:   syncFn,
		logger:   logger,
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
	job := &Job{ID: s.nextID, StartedAt: time.Now(), Status: StatusRunning}
	s.history = append(s.history, job)
	s.mu.Unlock()

	s.logger.Info("sync started", "job_id", job.ID, "started_at", job.StartedAt.Format(time.RFC3339))

	err := s.syncFn(ctx)

	s.mu.Lock()
	job.EndedAt = time.Now()
	job.Duration = job.EndedAt.Sub(job.StartedAt)
	if err != nil {
		job.Status = StatusFailed
		job.Err = err
		s.logger.Error("sync failed", "job_id", job.ID, "duration", job.Duration, "error", err)
	} else {
		job.Status = StatusSucceeded
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
