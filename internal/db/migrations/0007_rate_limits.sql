-- 0007_rate_limits: per-endpoint rate limiting, concurrency caps and fair
-- round-robin claiming.

ALTER TABLE endpoints
    -- Deliveries per second. NULL = unlimited.
    ADD COLUMN rate_limit_per_sec integer,
    -- Maximum simultaneous in-flight requests to this endpoint.
    ADD COLUMN max_in_flight      integer NOT NULL DEFAULT 10,
    -- Token bucket state, maintained by the claimer while it holds the row lock.
    ADD COLUMN rl_tokens          double precision,
    ADD COLUMN rl_updated_at      timestamptz,
    -- Round-robin cursor: endpoints visited least recently are served first.
    ADD COLUMN last_claimed_at    timestamptz;

ALTER TABLE endpoints
    ADD CONSTRAINT endpoints_rate_limit_check CHECK (rate_limit_per_sec IS NULL OR rate_limit_per_sec BETWEEN 1 AND 10000),
    ADD CONSTRAINT endpoints_max_in_flight_check CHECK (max_in_flight BETWEEN 1 AND 1000);

-- Claim step 1 walks active endpoints in round-robin order.
CREATE INDEX endpoints_round_robin_idx ON endpoints (last_claimed_at NULLS FIRST)
    WHERE status = 'active' AND deleted_at IS NULL;

-- Claim step 1 asks "does this endpoint have due work?"; step 2 takes it.
CREATE INDEX deliveries_endpoint_due_idx ON deliveries (endpoint_id, next_attempt_at)
    WHERE status = 'pending';

-- Counting an endpoint's in-flight deliveries.
CREATE INDEX deliveries_endpoint_delivering_idx ON deliveries (endpoint_id)
    WHERE status = 'delivering';
