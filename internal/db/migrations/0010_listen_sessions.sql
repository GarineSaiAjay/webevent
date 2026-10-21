-- 0010_listen_sessions: endpoints of kind 'listen' are served to the
-- `webevent listen` CLI over long-polling instead of being POSTed to by the
-- workers. They expire when the CLI stops polling.
ALTER TABLE endpoints
    ADD COLUMN kind              text NOT NULL DEFAULT 'http',
    ADD COLUMN listen_expires_at timestamptz;

ALTER TABLE endpoints ADD CONSTRAINT endpoints_kind_check CHECK (kind IN ('http', 'listen'));

CREATE INDEX endpoints_listen_expiry_idx ON endpoints (listen_expires_at)
    WHERE kind = 'listen' AND deleted_at IS NULL;
