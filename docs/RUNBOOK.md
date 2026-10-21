# Operations runbook

## Deploying

1. **Database.** Postgres 13+ (16 recommended), with backups and PITR. Size
   connections: `replicas × WEBEVENT_DB_MAX_CONNS` must stay below
   `max_connections` (or put PgBouncer in _session_ mode in front — LISTEN
   needs session pooling; transaction pooling is not supported).
2. **Secrets.** Generate and store in your secret manager:
   - `WEBEVENT_API_KEYS`: `openssl rand -hex 32` (several, comma-separated, to rotate)
   - `WEBEVENT_SECRET_KEY`: `openssl rand -base64 32` — back it up: losing it
     makes encrypted endpoint secrets unreadable.
3. **Migrate** with the one-off job (`webevent-server migrate`) before rolling
   out a new version; run servers with `WEBEVENT_AUTO_MIGRATE=false`.
   Migrations are forward-only and additive, so the previous version keeps
   working against the new schema during a rolling deploy.
4. **Roll out** (Kubernetes: `kubectl apply -k deploy/k8s`). Rolling updates
   are safe: SIGTERM stops claiming, finishes in-flight deliveries, then
   exits. Keep `terminationGracePeriodSeconds` >
   `WEBEVENT_SHUTDOWN_DELAY + WEBEVENT_SHUTDOWN_TIMEOUT`.
5. **Scale.** API and workers scale horizontally with no coordination. For
   heavy ingest you can run API-only replicas (`WEBEVENT_WORKER_ENABLED=false`)
   and worker-only replicas behind no load balancer.

## Rotating API keys

Add the new key to `WEBEVENT_API_KEYS` (comma-separated), roll out, move
clients over, remove the old key, roll out again.

## Dashboards and alerts

Key metrics (`:9090/metrics`):

| Metric                                                        | Healthy       | Meaning when bad              |
| ------------------------------------------------------------- | ------------- | ----------------------------- |
| `webevent_queue_oldest_due_seconds`                           | < 5s          | workers falling behind / down |
| `webevent_queue_depth{status="pending"}`                      | stable        | backlog growing               |
| `rate(webevent_delivery_attempts_total{outcome="dead"}[15m])` | ≈ 0           | receivers failing for a day+  |
| `webevent_deliveries_in_flight`                               | < concurrency | at the limit = add workers    |
| `webevent_claim_duration_seconds` p99                         | < 50ms        | DB contention / bloat         |
| `webevent_leases_reaped_total`                                | 0             | workers crashing / OOM-killed |

Alert rules: `deploy/observability/alerts.yml`.

## Incident playbooks

### "Deliveries are delayed" (`WebeventDeliveriesFallingBehind`)

1. Are workers running? `kubectl get pods`; check `webevent_deliveries_in_flight`.
2. In flight == concurrency on every pod → capacity: scale replicas or raise
   `WEBEVENT_WORKER_CONCURRENCY` (and `WEBEVENT_DB_MAX_CONNS`).
3. In flight low but queue growing → claims failing: look for `claim
deliveries` errors in logs; check database health and locks:
   `SELECT * FROM pg_stat_activity WHERE wait_event_type = 'Lock';`
4. One endpoint hogging? `SELECT endpoint_id, count(*) FROM deliveries WHERE
status='pending' GROUP BY 1 ORDER BY 2 DESC LIMIT 10;` — pause it with
   `webevent endpoints pause <id>` if it is causing harm.

### "A customer says they did not get webhooks"

1. `webevent deliveries list --endpoint <id> --status dead`
2. `webevent deliveries get <dlv>` — the attempt log shows status codes,
   errors and response bodies.
3. Once they have fixed their side: `webevent endpoints resume <id>` (if it
   was disabled), then `webevent endpoints replay <id> --since 24h`.

### "Workers keep crashing" (`WebeventLeasesExpiring`)

Expired leases mean a worker died mid-delivery (expect a few duplicates).
Check for OOM kills (`kubectl describe pod`), panics in logs, and memory
(`/debug/pprof/heap` on the admin port).

### Database is down

The API returns `500` and `/readyz` fails; nothing is lost that was already
acknowledged. Workers retry claiming every poll interval. When the database
returns, everything resumes; leases that expired meanwhile are reaped.

## Data retention

Events older than `WEBEVENT_RETENTION` whose deliveries are all finished are
deleted in batches every 10 minutes (one instance at a time, via an advisory
lock), cascading to deliveries and attempts. After deleting a lot of rows,
autovacuum reclaims space; for very high volume, consider time-partitioning
`events`/`deliveries`/`delivery_attempts` and dropping old partitions.
Note: an `Idempotency-Key` can be reused once its event is deleted.

## Capacity notes

- Every event costs roughly: 1 event row + N delivery rows + ≥ N attempt
  rows, where N is the number of subscribed endpoints.
- The claim query walks endpoints with due work in round-robin order. With
  very many (100k+) endpoints that all have backlogs, consider sharding
  workers by endpoint hash.
