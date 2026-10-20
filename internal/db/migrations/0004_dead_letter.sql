-- 0004_dead_letter: dead-letter state, cancellation, replay bookkeeping and
-- automatic endpoint disabling.

-- 'failed' becomes 'dead' (a dead letter you can inspect and replay), and a
-- 'cancelled' state is added for deliveries whose endpoint was deleted.
ALTER TABLE deliveries DROP CONSTRAINT deliveries_status_check;
UPDATE deliveries SET status = 'dead' WHERE status = 'failed';
ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
    CHECK (status IN ('pending', 'succeeded', 'dead', 'cancelled'));

-- Replays restart the retry budget without renumbering attempts:
-- the retry policy sees (attempt_count - attempt_offset).
ALTER TABLE deliveries
    ADD COLUMN attempt_offset integer NOT NULL DEFAULT 0,
    ADD COLUMN replay_count   integer NOT NULL DEFAULT 0;

-- Dead-letter browsing and bulk replay by endpoint and time window.
CREATE INDEX deliveries_dead_idx ON deliveries (endpoint_id, completed_at) WHERE status = 'dead';

-- Endpoints that keep failing are disabled automatically.
ALTER TABLE endpoints DROP CONSTRAINT endpoints_status_check;
ALTER TABLE endpoints ADD CONSTRAINT endpoints_status_check
    CHECK (status IN ('active', 'paused', 'disabled'));
ALTER TABLE endpoints
    ADD COLUMN consecutive_failures integer NOT NULL DEFAULT 0,
    ADD COLUMN disabled_reason      text;
