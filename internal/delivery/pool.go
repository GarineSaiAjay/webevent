package delivery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/GarineSaiAjay/webevent/internal/observability"
	"github.com/GarineSaiAjay/webevent/internal/store"
)

// PoolOptions configures a Pool.
type PoolOptions struct {
	Store  *store.Store
	Sender *Sender
	Policy RetryPolicy
	Logger *slog.Logger

	// Concurrency is the maximum number of deliveries in flight at once.
	Concurrency int
	// PollInterval is how often to look for due work when nothing wakes us.
	PollInterval time.Duration
	// LeaseDuration is how long a claimed delivery belongs to this worker.
	// It must comfortably exceed the HTTP timeout.
	LeaseDuration time.Duration
	// ReapInterval is how often to look for expired leases.
	ReapInterval time.Duration
	// DisableEndpointAfter disables an endpoint after this many consecutive
	// dead deliveries (0 = never).
	DisableEndpointAfter int
	// Wake, if set, receives a value whenever new work may be available
	// (fed by Postgres LISTEN/NOTIFY). Polling still runs as a safety net.
	Wake <-chan struct{}
	// WorkerID identifies this process in leases. Generated if empty.
	WorkerID string
	// Metrics may be nil.
	Metrics *observability.Metrics
}

// Pool runs many deliveries concurrently. Any number of Pools, in any number
// of processes, can run against the same database: claims use SKIP LOCKED,
// so they never take the same delivery twice.
type Pool struct {
	o    PoolOptions
	log  *slog.Logger
	done chan struct{} // a delivery finished: a slot is free
}

// NewPool validates options and builds a Pool.
func NewPool(o PoolOptions) (*Pool, error) {
	if o.Concurrency < 1 {
		return nil, fmt.Errorf("concurrency must be >= 1")
	}
	if o.LeaseDuration <= 0 || o.PollInterval <= 0 || o.ReapInterval <= 0 {
		return nil, fmt.Errorf("lease duration, poll interval and reap interval must be positive")
	}
	if o.WorkerID == "" {
		o.WorkerID = defaultWorkerID()
	}
	return &Pool{
		o:    o,
		log:  o.Logger.With("worker_id", o.WorkerID),
		done: make(chan struct{}, o.Concurrency),
	}, nil
}

// WorkerID returns the identifier this pool stamps on its leases.
func (p *Pool) WorkerID() string { return p.o.WorkerID }

func defaultWorkerID() string {
	host, _ := os.Hostname()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b))
}

// Run claims and delivers until ctx is cancelled, then waits for in-flight
// deliveries to finish before returning.
//
// Graceful shutdown: cancelling ctx stops new claims immediately. Deliveries
// already in flight keep running (their HTTP requests are bounded by the
// sender timeout) and record their results, so a normal deploy never
// abandons work. If the process is killed hard instead, leases expire and
// another instance's reaper recovers the work.
func (p *Pool) Run(ctx context.Context) {
	p.log.Info("delivery pool started",
		"concurrency", p.o.Concurrency, "lease", p.o.LeaseDuration, "poll_interval", p.o.PollInterval)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.reapLoop(ctx)
	}()

	inFlight := 0
	poll := time.NewTicker(p.o.PollInterval)
	defer poll.Stop()

	for {
		// Collect finished deliveries without blocking.
		for drained := false; !drained; {
			select {
			case <-p.done:
				inFlight--
			default:
				drained = true
			}
		}

		claimedFull := false
		if free := p.o.Concurrency - inFlight; free > 0 && ctx.Err() == nil {
			claimStart := time.Now()
			jobs, err := p.o.Store.ClaimJobs(ctx, p.o.WorkerID, free, p.o.LeaseDuration)
			if err != nil && ctx.Err() == nil {
				p.log.Error("claim deliveries", "err", err)
			}
			p.o.Metrics.Claimed(len(jobs), time.Since(claimStart))
			for _, job := range jobs {
				inFlight++
				wg.Add(1)
				go func(job store.Job) {
					defer wg.Done()
					defer func() { p.done <- struct{}{} }()
					// Detached from ctx on purpose: see the shutdown note above.
					p.process(context.WithoutCancel(ctx), job)
				}(job)
			}
			// A full batch suggests more work is waiting: loop again at once.
			claimedFull = len(jobs) == free
		}
		if claimedFull {
			continue
		}

		select {
		case <-ctx.Done():
			p.log.Info("delivery pool draining", "in_flight", inFlight)
			wg.Wait()
			p.log.Info("delivery pool stopped")
			return
		case <-p.done:
			inFlight--
		case <-p.o.Wake:
		case <-poll.C:
		}
	}
}

// RunOnce claims a single batch (up to Concurrency deliveries), processes it
// concurrently and waits for every attempt to be recorded. It returns how many
// deliveries it processed. Tests use it to drive the pool deterministically.
func (p *Pool) RunOnce(ctx context.Context) (int, error) {
	jobs, err := p.o.Store.ClaimJobs(ctx, p.o.WorkerID, p.o.Concurrency, p.o.LeaseDuration)
	if err != nil {
		return 0, err
	}
	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		go func(job store.Job) {
			defer wg.Done()
			p.process(ctx, job)
		}(job)
	}
	wg.Wait()
	return len(jobs), nil
}

func (p *Pool) reapLoop(ctx context.Context) {
	t := time.NewTicker(p.o.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := p.o.Store.ReapExpiredLeases(ctx, p.o.Policy.BaseDelay, 500)
		if err != nil && ctx.Err() == nil {
			p.log.Error("reap expired leases", "err", err)
		}
		if n > 0 {
			p.o.Metrics.Reaped(n)
			p.log.Warn("recovered deliveries with expired leases", "count", n)
		}
	}
}

// process performs one attempt and records the outcome.
func (p *Pool) process(ctx context.Context, job store.Job) {
	p.o.Metrics.InFlight(1)
	defer p.o.Metrics.InFlight(-1)

	// The delivery runs long after (and outside) the request that created
	// the event, so it is not a child of that span but LINKED to it.
	var opts []trace.SpanStartOption
	if sc := observability.SpanContextFromTraceParent(job.TraceParent); sc.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc}))
	}
	opts = append(opts, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("webevent.delivery_id", job.DeliveryID),
		attribute.String("webevent.event_id", job.EventID),
		attribute.String("webevent.event_type", job.EventType),
		attribute.String("webevent.endpoint_id", job.EndpointID),
		attribute.Int("webevent.attempt", job.AttemptNumber),
	))
	ctx, span := observability.Tracer().Start(ctx, "webhook.deliver", opts...)
	defer span.End()

	res := p.o.Sender.Send(ctx, job)
	dec := p.o.Policy.Decide(job.RetryAttempt, res)
	span.SetAttributes(attribute.Int("http.response.status_code", res.StatusCode), attribute.String("webevent.outcome", outcomeName(dec.Outcome)))
	if dec.Outcome != store.OutcomeSucceeded {
		span.SetStatus(codes.Error, res.Error)
	}

	recordCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rr, err := p.o.Store.RecordAttempt(recordCtx, store.AttemptRecord{
		Job: job, Result: res, Outcome: dec.Outcome, RetryIn: dec.Delay, DisableEndpointAfter: p.o.DisableEndpointAfter,
	})

	log := p.log.With("delivery_id", job.DeliveryID, "event_id", job.EventID, "endpoint_id", job.EndpointID,
		"attempt", job.AttemptNumber, "status_code", res.StatusCode, "duration_ms", res.Duration.Milliseconds(),
		"trace_id", span.SpanContext().TraceID().String())
	if err != nil {
		// The lease will expire and the reaper will retry the delivery.
		log.Error("record attempt", "err", err)
		return
	}
	if !rr.Applied {
		log.Warn("lost ownership of delivery (lease expired or endpoint deleted); result not applied")
		return
	}
	p.o.Metrics.Attempt(outcomeName(dec.Outcome), res.Duration)
	switch dec.Outcome {
	case store.OutcomeSucceeded:
		log.Info("delivery succeeded")
	case store.OutcomeRetry:
		log.Warn("delivery attempt failed; will retry", "err", res.Error, "retry_in", dec.Delay.Round(time.Millisecond))
	case store.OutcomeDead:
		log.Error("delivery dead-lettered", "err", res.Error, "reason", dec.Reason)
	}
	if rr.EndpointDisabled {
		p.o.Metrics.EndpointDisabled()
		log.Error("endpoint disabled after repeated dead deliveries", "threshold", p.o.DisableEndpointAfter)
	}
}

func outcomeName(o store.Outcome) string {
	switch o {
	case store.OutcomeSucceeded:
		return "succeeded"
	case store.OutcomeRetry:
		return "retry"
	case store.OutcomeDead:
		return "dead"
	}
	return "unknown"
}
