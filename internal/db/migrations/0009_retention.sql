-- 0009_retention: support the background job that deletes old events.
-- Deleting an event cascades to its deliveries and their attempts.
CREATE INDEX events_created_at_idx ON events (created_at);
