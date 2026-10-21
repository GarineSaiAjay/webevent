# HTTP API reference

Base URL: wherever `webevent-server` listens (`WEBEVENT_HTTP_ADDR`).
All `/v1` routes require `Authorization: Bearer <api key>`.
Request and response bodies are JSON (`Content-Type: application/json`).

## Conventions

**Errors** always use one envelope:

```json
{
  "error": {
    "code": "validation_failed",
    "message": "request failed validation",
    "fields": { "url": "must use http or https" }
  }
}
```

| HTTP | `code`                   | When                                                            |
| ---- | ------------------------ | --------------------------------------------------------------- |
| 400  | `bad_request`            | malformed JSON, unknown field, bad query parameter              |
| 401  | `unauthorized`           | missing/invalid API key                                         |
| 404  | `not_found`              | unknown id or route                                             |
| 409  | `conflict`               | idempotency key reused with another body; replay in wrong state |
| 413  | `payload_too_large`      | body over 512 KiB                                               |
| 415  | `unsupported_media_type` | non-JSON content type                                           |
| 422  | `validation_failed`      | field-level errors in `fields`                                  |
| 500  | `internal`               | bug or database failure (see logs by `X-Request-Id`)            |

**Request ids.** Every response carries `X-Request-Id` (yours if you sent a
safe one). Quote it when reporting problems.

**Pagination.** Lists return newest first:
`{"data": [...], "next_cursor": "..."}`. Pass `?cursor=<next_cursor>` for the
next page and `?limit=1..100` (default 20). No `next_cursor` = last page.

**Ids** are prefixed and time-sortable: `ep_…`, `evt_…`, `dlv_…`, `att_…`.

**Timestamps** are RFC 3339 in UTC.

## Health (no auth)

| Method | Path       | Response                                                                           |
| ------ | ---------- | ---------------------------------------------------------------------------------- |
| GET    | `/healthz` | `200 {"status":"ok","version":"…"}` — process is alive                             |
| GET    | `/readyz`  | `200 {"status":"ready"}`; `503` if the DB is unreachable or the server is draining |

## Endpoints

### Create — `POST /v1/endpoints`

```json
{
  "url": "https://example.com/webhooks",
  "description": "billing service",
  "event_types": ["invoice.paid", "invoice.failed"],
  "rate_limit_per_sec": 20,
  "max_in_flight": 10
}
```

- `url` — absolute `http`/`https`, no credentials or fragment, ≤ 2048 chars.
  Private, loopback and link-local addresses are rejected unless
  `WEBEVENT_ALLOW_PRIVATE_NETWORKS=true`.
- `event_types` — 1–100 names (`[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}`) or `"*"`.
- `rate_limit_per_sec` — optional, 1–10000; omitted/0 = unlimited.
- `max_in_flight` — optional, 1–1000, default 10.

`201` → the endpoint **including `secret`** (`whsec_…`). Store it in your
receiver. It is not part of later reads; use `GET …/secret`.

### List — `GET /v1/endpoints`

### Get — `GET /v1/endpoints/{id}`

Endpoint object:

```json
{
  "id": "ep_06gm…",
  "url": "https://example.com/webhooks",
  "description": "billing service",
  "event_types": ["invoice.paid"],
  "status": "active",
  "rate_limit_per_sec": 20,
  "max_in_flight": 10,
  "consecutive_failures": 0,
  "created_at": "…",
  "updated_at": "…"
}
```

`status`: `active`, `paused` (by you; deliveries queue up), `disabled` (by the
system after `WEBEVENT_ENDPOINT_DISABLE_AFTER` consecutive dead deliveries;
`disabled_reason` explains). Disabled endpoints receive no new deliveries.

### Update — `PATCH /v1/endpoints/{id}`

Any subset of `url`, `description`, `event_types`, `status`
(`active`|`paused`), `rate_limit_per_sec` (0 removes the limit),
`max_in_flight`. Setting `status` resets the failure counter, so
`{"status":"active"}` re-enables a disabled endpoint; its parked deliveries
resume.

### Delete — `DELETE /v1/endpoints/{id}` → `204`

Soft-deletes and cancels pending and in-flight deliveries.

### Reveal secret — `GET /v1/endpoints/{id}/secret` → `{"secret":"whsec_…"}`

### Rotate secret — `POST /v1/endpoints/{id}/rotate-secret`

Body (optional): `{"grace_period":"24h"}` (0s–168h, default 24h).
During the grace period every request is signed with both secrets.
→ `{"secret":"whsec_new…","previous_secret_valid_until":"…"}`

### Bulk replay — `POST /v1/endpoints/{id}/replay`

Body (optional): `{"since":"…","until":"…"}`, default the last 7 days.
Re-queues every **dead** delivery of the endpoint that died in the window.
→ `202 {"replayed": 42, "since": "…", "until": "…"}`

## Events

### Publish — `POST /v1/events`

```http
POST /v1/events
Idempotency-Key: order-1234-paid
Content-Type: application/json

{"event_type": "order.paid", "payload": {"order_id": 1234, "amount": 4200}}
```

- `event_type` — same rules as subscriptions (no `*`).
- `payload` — any JSON value except `null`, ≤ 256 KiB.
- `Idempotency-Key` (optional, ≤ 255 printable ASCII chars). Retrying with the
  same key and body returns the original event with `200` and
  `Idempotent-Replayed: true`; the same key with a different body is `409`.

`202` → `{"id":"evt_…","event_type":"order.paid","payload":{…},"created_at":"…","delivery_count":2}`

### List — `GET /v1/events?event_type=order.paid`

### Get — `GET /v1/events/{id}`

## Deliveries

One delivery = one event to one endpoint.

| status       | meaning                                                        |
| ------------ | -------------------------------------------------------------- |
| `pending`    | waiting for its next attempt (`next_attempt_at`)               |
| `delivering` | claimed by a worker right now                                  |
| `succeeded`  | receiver answered 2xx                                          |
| `dead`       | retries exhausted, `410 Gone`, or permanent error — replayable |
| `cancelled`  | endpoint was deleted                                           |

### List — `GET /v1/deliveries?event_id=&endpoint_id=&status=`

### Get — `GET /v1/deliveries/{id}` (includes `attempts`)

### Replay — `POST /v1/deliveries/{id}/replay`

Allowed for `dead` and `succeeded` deliveries of live endpoints (`409`
otherwise). Restarts the retry budget; attempt numbering continues.
→ `202` with the delivery.

## Attempt log

### `GET /v1/attempts?endpoint_id=&event_id=&delivery_id=`

Every HTTP attempt across deliveries, newest first, with `event_type`,
`endpoint_id` and `url`. With `?after=<attempt id>` it returns only newer
attempts, **oldest first**, and `next_cursor` is the next `after` value —
this is how `webevent logs --follow` tails.

## What receivers get

```http
POST /your/path
Content-Type: application/json
User-Agent: webevent/1.0.0
Webhook-Id: evt_06gm…                     (stable across retries: dedupe on it)
Webhook-Timestamp: 1760787000
Webhook-Signature: v1,K5oZfzN95Z9UVu1EsfQmfVNQhnkZ2pj9o9NDN/H/pI4=
Traceparent: 00-…                         (if tracing is enabled)

{"id":"evt_06gm…","type":"order.paid","created_at":"…","data":{"order_id":1234,"amount":4200}}
```

Answer any `2xx` quickly. Anything else (including timeouts and redirects)
is retried; `410 Gone` stops retries immediately. See `docs/RECEIVERS.md`.
