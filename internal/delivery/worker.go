package delivery

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

// Worker is the walking-skeleton delivery loop: one goroutine, one delivery
// at a time. Later stages replace it with a concurrent pool.
type Worker struct {
	store        *store.Store
	sender       *Sender
	log          *slog.Logger
	pollInterval time.Duration
}

// NewWorker builds a Worker.
func NewWorker(st *store.Store, sender *Sender, log *slog.Logger, pollInterval time.Duration) *Worker {
	return &Worker{store: st, sender: sender, log: log, pollInterval: pollInterval}
}

// Run processes deliveries until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	w.log.Info("delivery worker started", "poll_interval", w.pollInterval)
	defer w.log.Info("delivery worker stopped")
	for {
		// Drain everything that is due before sleeping.
		for {
			did, err := w.ProcessOne(ctx)
			if err != nil && ctx.Err() == nil {
				w.log.Error("process delivery", "err", err)
			}
			if !did || ctx.Err() != nil {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.pollInterval):
		}
	}
}

// ProcessOne delivers the next due job, if any. It reports whether a job was
// processed. Exported so tests can drive the worker step by step.
func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	job, err := w.store.NextDueJob(ctx)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	res := w.sender.Send(ctx, job)
	ok := res.Error == "" && Successful(res.StatusCode)

	// Record with a context that survives shutdown: the HTTP request already
	// happened, and forgetting its outcome would cause a duplicate delivery.
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := w.store.RecordAttempt(recordCtx, job, res, ok); err != nil {
		return true, err
	}

	log := w.log.With("delivery_id", job.DeliveryID, "event_id", job.EventID, "endpoint_id", job.EndpointID,
		"attempt", job.AttemptNumber, "status_code", res.StatusCode, "duration_ms", res.Duration.Milliseconds())
	if ok {
		log.Info("delivery succeeded")
	} else {
		log.Warn("delivery failed", "err", res.Error)
	}
	return true, nil
}
