# webevent

A reliable webhook delivery service — a small version of what Stripe and
GitHub run internally. Your application publishes an event once; webevent
stores it durably and delivers it to every subscribed URL with retries,
signatures, rate limits and a dead-letter queue.

```
 producer ──POST /v1/events──▶ API ──one transaction──▶ Postgres
                                                          │  events, deliveries (the queue), attempts
                     ┌────────────── LISTEN/NOTIFY ◀──────┘
                     ▼
         delivery workers (any number of processes)
           claim with FOR UPDATE SKIP LOCKED + lease
           sign (HMAC-SHA256, Standard Webhooks)
           POST ──▶ receiver
           2xx → succeeded · error → retry with backoff+jitter · exhausted → dead
```

## Guarantees

- **No lost events.** An event and all of its deliveries are written in one
  transaction before the API answers `202 Accepted`.
- **At-least-once delivery.** A delivery is retried until it succeeds or its
  retry budget (default ~1 day) runs out; then it is dead-lettered and can be
  replayed. Every request carries a stable `webhook-id` so receivers can drop
  the rare duplicate (only possible when a worker dies mid-request).
- **Never stuck.** Claimed deliveries hold a time-bounded lease. If a worker
  crashes, the lease expires and another instance takes the delivery over.
- **Isolation.** One slow or failing receiver can use at most its
  `max_in_flight` workers; everyone else keeps flowing. Optional per-endpoint
  rate limits are enforced across all instances.
- **Secure by default.** HMAC signatures, secrets encrypted at rest, SSRF
  protection at connect time, no redirects, bounded response reads.

## Quick start

```bash
make db-up                         # Postgres 16 in Docker
cp .env.example .env               # dev settings (allows localhost endpoints)
make run                           # API on :8080, metrics on :9090

go build -o bin/webevent ./cmd/webevent
export WEBEVENT_API_KEY=dev-key-change-me-0123456789abcdef0123456789

bin/webevent endpoints create --url http://localhost:4000/hook --events user.created
# copy the printed signing secret, then in another terminal:
go run ./examples/receiver -secret whsec_... -fail-rate 0.3

bin/webevent events trigger user.created --data '{"id": 42}'
bin/webevent logs --follow
```

## Components

| Path                  | What                                                          |
| --------------------- | ------------------------------------------------------------- |
| `cmd/webevent-server` | the service: HTTP API + delivery workers + retention job      |
| `cmd/webevent`        | CLI (`endpoints`, `events`, `deliveries`, `logs`, `config`)   |
| `pkg/client`          | Go SDK for the API (the CLI is built on it)                   |
| `pkg/webhook`         | sign/verify helpers for **receivers** (Standard Webhooks)     |
| `examples/receiver`   | example consumer that verifies signatures and dedupes         |
| `tools/loadgen`       | throughput / end-to-end latency benchmark                     |
| `deploy/`             | Kubernetes manifests, Prometheus/Grafana/Jaeger stack, alerts |

## Configuration

All settings are environment variables. Only the first two are required.

| Variable                          | Default         | Meaning                                                       |
| --------------------------------- | --------------- | ------------------------------------------------------------- |
| `WEBEVENT_DATABASE_URL`           | —               | Postgres connection string                                    |
| `WEBEVENT_API_KEYS`               | —               | comma-separated API keys, each ≥ 32 chars                     |
| `WEBEVENT_HTTP_ADDR`              | `:8080`         | API listen address                                            |
| `WEBEVENT_ADMIN_ADDR`             | `:9090`         | `/metrics` + `/debug/pprof` (keep internal; `off` disables)   |
| `WEBEVENT_SECRET_KEY`             | —               | base64 32-byte key encrypting signing secrets at rest         |
| `WEBEVENT_ALLOW_PRIVATE_NETWORKS` | `false`         | allow private/loopback targets (dev only)                     |
| `WEBEVENT_WORKER_ENABLED`         | `true`          | run delivery workers in this process                          |
| `WEBEVENT_WORKER_CONCURRENCY`     | `32`            | max deliveries in flight per process                          |
| `WEBEVENT_WORKER_POLL_INTERVAL`   | `1s`            | fallback polling (NOTIFY wakes workers instantly)             |
| `WEBEVENT_DELIVERY_TIMEOUT`       | `15s`           | per-request timeout                                           |
| `WEBEVENT_LEASE_DURATION`         | `60s`           | claim lease; ≥ delivery timeout + 15s                         |
| `WEBEVENT_REAP_INTERVAL`          | `10s`           | how often expired leases are recovered                        |
| `WEBEVENT_MAX_ATTEMPTS`           | `15`            | attempts before a delivery is dead-lettered                   |
| `WEBEVENT_RETRY_BASE_DELAY`       | `10s`           | first retry delay (doubles each time)                         |
| `WEBEVENT_RETRY_MAX_DELAY`        | `6h`            | cap on a single retry delay                                   |
| `WEBEVENT_ENDPOINT_DISABLE_AFTER` | `10`            | consecutive dead deliveries before auto-disable (0 = never)   |
| `WEBEVENT_RETENTION`              | `720h`          | delete finished events older than this (0 = keep)             |
| `WEBEVENT_DB_MAX_CONNS`           | `20`            | connection pool size                                          |
| `WEBEVENT_AUTO_MIGRATE`           | `true`          | run migrations on start (use `false` + a migrate job in prod) |
| `WEBEVENT_SHUTDOWN_DELAY`         | `0s`            | report not-ready this long before shutting down               |
| `WEBEVENT_SHUTDOWN_TIMEOUT`       | `30s`           | max time to drain in-flight deliveries                        |
| `WEBEVENT_LOG_LEVEL` / `_FORMAT`  | `info` / `json` | logging                                                       |
| `OTEL_EXPORTER_OTLP_ENDPOINT`     | —               | enables OpenTelemetry tracing                                 |

## Documentation

- [`docs/API.md`](docs/API.md) — HTTP API reference
- [`docs/RECEIVERS.md`](docs/RECEIVERS.md) — how to consume webhooks correctly
- [`docs/RUNBOOK.md`](docs/RUNBOOK.md) — deploying and operating in production
- [`SECURITY.md`](SECURITY.md) — threat model and reporting

## Development

```bash
make help        # every target
make check       # fmt + tidy + lint + all tests (what CI runs)
make test-e2e    # real binary: crash recovery, rolling restart
make fuzz        # fuzz every target for 30s
make bench       # database benchmarks
```

## License

MIT — see [LICENSE](LICENSE).
