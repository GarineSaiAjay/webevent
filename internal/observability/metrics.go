// Package observability provides Prometheus metrics and OpenTelemetry
// tracing for the service.
package observability

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds every metric the service exports. All methods are safe to
// call on a nil *Metrics, so components work without metrics in tests.
type Metrics struct {
	Registry *prometheus.Registry

	httpRequests      *prometheus.CounterVec
	httpDuration      *prometheus.HistogramVec
	eventsIngested    prometheus.Counter
	deliveriesQueued  prometheus.Counter
	attempts          *prometheus.CounterVec
	attemptDuration   *prometheus.HistogramVec
	inFlight          prometheus.Gauge
	claimDuration     prometheus.Histogram
	claimed           prometheus.Counter
	leasesReaped      prometheus.Counter
	endpointsDisabled prometheus.Counter
}

// NewMetrics registers all metrics (plus Go runtime and process metrics) on
// a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{Registry: reg}
	m.httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "webevent_http_requests_total",
		Help: "API requests by method, route pattern and status code.",
	}, []string{"method", "route", "code"})
	m.httpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webevent_http_request_duration_seconds",
		Help:    "API request latency by method and route pattern.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"method", "route"})
	m.eventsIngested = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "webevent_events_ingested_total",
		Help: "Events accepted by POST /v1/events (idempotent replays excluded).",
	})
	m.deliveriesQueued = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "webevent_deliveries_enqueued_total",
		Help: "Deliveries created by event fan-out.",
	})
	m.attempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "webevent_delivery_attempts_total",
		Help: "Delivery attempts by outcome (succeeded, retry, dead).",
	}, []string{"outcome"})
	m.attemptDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webevent_delivery_attempt_duration_seconds",
		Help:    "Time from sending a webhook to receiving the response.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	}, []string{"outcome"})
	m.inFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "webevent_deliveries_in_flight",
		Help: "Deliveries currently being sent by this process.",
	})
	m.claimDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "webevent_claim_duration_seconds",
		Help:    "Time spent in one ClaimJobs transaction.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	})
	m.claimed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "webevent_deliveries_claimed_total",
		Help: "Deliveries claimed by this process.",
	})
	m.leasesReaped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "webevent_leases_reaped_total",
		Help: "Deliveries recovered after their worker's lease expired.",
	})
	m.endpointsDisabled = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "webevent_endpoints_disabled_total",
		Help: "Endpoints automatically disabled after repeated dead deliveries.",
	})
	reg.MustRegister(m.httpRequests, m.httpDuration, m.eventsIngested, m.deliveriesQueued, m.attempts,
		m.attemptDuration, m.inFlight, m.claimDuration, m.claimed, m.leasesReaped, m.endpointsDisabled)
	return m
}

// ObserveHTTP records one API request. route is the matched ServeMux
// pattern (never the raw path: ids in paths would explode cardinality).
func (m *Metrics) ObserveHTTP(method, route string, code int, d time.Duration) {
	if m == nil {
		return
	}
	if route == "" {
		route = "unmatched"
	}
	m.httpRequests.WithLabelValues(method, route, strconv.Itoa(code)).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(d.Seconds())
}

// EventIngested records an accepted event and its fan-out.
func (m *Metrics) EventIngested(deliveries int) {
	if m == nil {
		return
	}
	m.eventsIngested.Inc()
	m.deliveriesQueued.Add(float64(deliveries))
}

// Attempt records a finished delivery attempt.
func (m *Metrics) Attempt(outcome string, d time.Duration) {
	if m == nil {
		return
	}
	m.attempts.WithLabelValues(outcome).Inc()
	m.attemptDuration.WithLabelValues(outcome).Observe(d.Seconds())
}

// InFlight adjusts the in-flight gauge by delta.
func (m *Metrics) InFlight(delta int) {
	if m == nil {
		return
	}
	m.inFlight.Add(float64(delta))
}

// Claimed records one claim transaction.
func (m *Metrics) Claimed(n int, d time.Duration) {
	if m == nil {
		return
	}
	m.claimDuration.Observe(d.Seconds())
	m.claimed.Add(float64(n))
}

// Reaped records leases recovered by the reaper.
func (m *Metrics) Reaped(n int) {
	if m == nil {
		return
	}
	m.leasesReaped.Add(float64(n))
}

// EndpointDisabled records an automatic endpoint disable.
func (m *Metrics) EndpointDisabled() {
	if m == nil {
		return
	}
	m.endpointsDisabled.Inc()
}

// RegisterQueueCollector exports queue health read from the database at
// scrape time: deliveries per status and the age of the oldest due
// delivery (the single best "are we keeping up?" signal). Results are
// cached for cacheFor so frequent scrapes don't hammer Postgres.
func (m *Metrics) RegisterQueueCollector(pool *pgxpool.Pool, cacheFor time.Duration, log *slog.Logger) {
	if m == nil {
		return
	}
	m.Registry.MustRegister(&queueCollector{pool: pool, cacheFor: cacheFor, log: log})
}

var (
	queueDepthDesc = prometheus.NewDesc("webevent_queue_depth",
		"Deliveries in each non-final state (pending, delivering) plus dead letters.", []string{"status"}, nil)
	oldestDueDesc = prometheus.NewDesc("webevent_queue_oldest_due_seconds",
		"How long the oldest due pending delivery has been waiting. Growing = workers are falling behind.", nil, nil)
)

type queueCollector struct {
	pool     *pgxpool.Pool
	cacheFor time.Duration
	log      *slog.Logger

	mu        sync.Mutex
	fetchedAt time.Time
	depth     map[string]float64
	oldest    float64
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- queueDepthDesc
	ch <- oldestDueDesc
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.fetchedAt) > c.cacheFor {
		if err := c.refresh(); err != nil {
			c.log.Warn("queue metrics refresh failed", "err", err)
		} else {
			c.fetchedAt = time.Now()
		}
	}
	for status, n := range c.depth {
		ch <- prometheus.MustNewConstMetric(queueDepthDesc, prometheus.GaugeValue, n, status)
	}
	ch <- prometheus.MustNewConstMetric(oldestDueDesc, prometheus.GaugeValue, c.oldest)
}

func (c *queueCollector) refresh() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Each count uses one of the partial indexes, so it stays cheap even
	// with hundreds of millions of finished deliveries in the table.
	var pending, delivering, dead, oldest float64
	err := c.pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM deliveries WHERE status = 'pending'),
			(SELECT count(*) FROM deliveries WHERE status = 'delivering'),
			(SELECT count(*) FROM deliveries WHERE status = 'dead'),
			COALESCE((SELECT extract(epoch FROM now() - min(next_attempt_at))
			          FROM deliveries WHERE status = 'pending' AND next_attempt_at <= now()), 0)`).
		Scan(&pending, &delivering, &dead, &oldest)
	if err != nil {
		return err
	}
	c.depth = map[string]float64{"pending": pending, "delivering": delivering, "dead": dead}
	c.oldest = oldest
	return nil
}
