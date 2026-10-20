-- 0002_endpoints: the URLs that receive webhooks.

CREATE TABLE endpoints (
    id          text        PRIMARY KEY,
    url         text        NOT NULL,
    description text        NOT NULL DEFAULT '',
    -- Event types this endpoint subscribes to. '*' means "everything".
    event_types text[]      NOT NULL,
    -- active: receives deliveries; paused: deliveries queue up but are not sent.
    status      text        NOT NULL DEFAULT 'active',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    -- Soft delete: history (deliveries, attempts) keeps pointing at the row.
    deleted_at  timestamptz,

    CONSTRAINT endpoints_status_check CHECK (status IN ('active', 'paused')),
    CONSTRAINT endpoints_event_types_not_empty CHECK (cardinality(event_types) > 0)
);

CREATE TRIGGER endpoints_set_updated_at
    BEFORE UPDATE ON endpoints
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Fan-out looks up live endpoints by subscribed type; GIN makes `type = ANY(event_types)` cheap.
CREATE INDEX endpoints_event_types_idx ON endpoints USING gin (event_types) WHERE deleted_at IS NULL;
