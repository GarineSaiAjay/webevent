package delivery

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

// Worker is the single-goroutine delivery loop. Stage 6 replaces it with a
// concurrent pool.
type Worker struct {
	store        *store.Store
	sender       *Sender
	policy       RetryPolicy
	log          *slog.Logger
	pollInterval time.Duration
	disableAfter int
}

// WorkerOptions configures a Worker.
type WorkerOptions struct {
	Store        *store.Store
	Sender       *Sender
	Policy       RetryPolicy
	Logger       *slog.Logger
	PollInterval time.Duration
	// DisableEndpointAfter disables an endpoint after this many consecutive
	// dead deliveries (0 = never).
	DisableEndpointAfter int
}

// NewWorker builds a Worker.
func NewWorker(o WorkerOptions) *Worker {
	return &Worker{
		store: o.Store, sender: o.Sender, policy: o.Policy, log: o.Logger,
		pollInterval: o.PollInterval, disableAfter: o.DisableEndpointAfter,
	}
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
	dec := w.policy.Decide(job.RetryAttempt, res)

	// Record with a context that survives shutdown: the HTTP request already
	// happened, and forgetting its outcome would cause a duplicate delivery.
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	rr, err := w.store.RecordAttempt(recordCtx, store.AttemptRecord{
		Job: job, Result: res, Outcome: dec.Outcome, RetryIn: dec.Delay, DisableEndpointAfter: w.disableAfter,
	})
	if err != nil {
		return true, err
	}

	log := w.log.With("delivery_id", job.DeliveryID, "event_id", job.EventID, "endpoint_id", job.EndpointID,
		"attempt", job.AttemptNumber, "status_code", res.StatusCode, "duration_ms", res.Duration.Milliseconds())
	if !rr.Applied {
		log.Warn("delivery changed while in flight (endpoint deleted?); attempt logged, state left as is")
		return true, nil
	}
	switch dec.Outcome {
	case store.OutcomeSucceeded:
		log.Info("delivery succeeded")
	case store.OutcomeRetry:
		log.Warn("delivery attempt failed; will retry", "err", res.Error, "retry_in", dec.Delay.Round(time.Millisecond))
	case store.OutcomeDead:
		log.Error("delivery dead-lettered", "err", res.Error, "reason", dec.Reason)
	}
	if rr.EndpointDisabled {
		log.Error("endpoint disabled after repeated dead deliveries", "threshold", w.disableAfter)
	}
	return true, nil
}
