# Changelog

All notable changes are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/) and the project uses
[Semantic Versioning](https://semver.org/).

## [1.1.0] - 2026-10-18

### Added

- `webevent listen`: forward deliveries to a local server without a tunnel,
  through listen sessions (`/v1/listen`) that expire when abandoned.

## [1.0.0] - 2026-10-18

### Added

- Event ingestion with idempotency keys and transactional fan-out.
- Concurrent delivery workers using `FOR UPDATE SKIP LOCKED` with leases,
  crash recovery and LISTEN/NOTIFY wake-ups.
- Retries with exponential backoff and equal jitter; `Retry-After` support.
- Dead-letter state, single and bulk replay, automatic endpoint disabling.
- Standard Webhooks HMAC signatures with zero-downtime secret rotation;
  secrets encrypted at rest.
- SSRF protection at connect time.
- Per-endpoint rate limits and concurrency caps with fair round-robin claiming.
- Prometheus metrics, OpenTelemetry tracing, pprof, alert rules.
- `webevent` CLI and Go client SDK.
- Retention job, graceful draining, Docker image, Kubernetes manifests.
