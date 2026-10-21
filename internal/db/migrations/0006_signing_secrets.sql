-- 0006_signing_secrets: per-endpoint HMAC signing secrets with rotation.
--
-- secret holds the current "whsec_..." secret, possibly sealed with
-- WEBEVENT_SECRET_KEY ("enc:v1:..."). During a rotation the old secret stays
-- in previous_secret until previous_secret_expires_at, and requests are
-- signed with both so receivers can switch over without downtime.

ALTER TABLE endpoints
    ADD COLUMN secret                     text,
    ADD COLUMN previous_secret            text,
    ADD COLUMN previous_secret_expires_at timestamptz;

-- Backfill existing endpoints with a random secret. gen_random_uuid() is
-- built into Postgres 13+ and uses a cryptographically secure generator;
-- two UUIDs give 32 random-ish bytes (244 bits of entropy) without needing
-- the pgcrypto extension.
UPDATE endpoints SET secret = 'whsec_' || encode(
    decode(replace(gen_random_uuid()::text, '-', '') || replace(gen_random_uuid()::text, '-', ''), 'hex'),
    'base64')
WHERE secret IS NULL;

ALTER TABLE endpoints ALTER COLUMN secret SET NOT NULL;
