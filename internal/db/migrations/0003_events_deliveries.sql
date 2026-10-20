-- 0003_events_deliveries: events, the per-endpoint delivery queue, and the
-- attempt log.

CREATE TABLE events (
    id              text        PRIMARY KEY,
    event_type      text        NOT NULL,
    payload         jsonb       NOT NULL,
    -- Optional client-supplied key that makes POST /v1/events safe to retry.
    idempotency_key text,
    -- SHA-256 of (event_type, payload) so a reused key with a different body
    -- can be rejected instead of silently returning the wrong event.
    request_hash    bytea       NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX events_idempotency_key_idx ON events (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX events_type_idx ON events (event_type, id);

-- One row per (event, endpoint): this table IS the job queue.
CREATE TABLE deliveries (
    id               text        PRIMARY KEY,
    event_id         text        NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    endpoint_id      text        NOT NULL REFERENCES endpoints (id),
    status           text        NOT NULL DEFAULT 'pending',
    attempt_count    integer     NOT NULL DEFAULT 0,
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    last_attempt_at  timestamptz,
    last_status_code integer,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    completed_at     timestamptz,

    CONSTRAINT deliveries_status_check CHECK (status IN ('pending', 'succeeded', 'failed')),
    CONSTRAINT deliveries_event_endpoint_unique UNIQUE (event_id, endpoint_id)
);

CREATE TRIGGER deliveries_set_updated_at
    BEFORE UPDATE ON deliveries
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- The hot path: "what is due?" Partial index stays small because finished
-- deliveries drop out of it.
CREATE INDEX deliveries_due_idx ON deliveries (next_attempt_at) WHERE status = 'pending';
CREATE INDEX deliveries_endpoint_idx ON deliveries (endpoint_id, id);
CREATE INDEX deliveries_event_idx ON deliveries (event_id);

-- Append-only log of every HTTP attempt, for debugging and the CLI `logs` command.
CREATE TABLE delivery_attempts (
    id             text        PRIMARY KEY,
    delivery_id    text        NOT NULL REFERENCES deliveries (id) ON DELETE CASCADE,
    attempt_number integer     NOT NULL,
    status_code    integer,
    error          text,
    response_body  text,
    duration_ms    integer     NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX delivery_attempts_delivery_idx ON delivery_attempts (delivery_id, attempt_number);
