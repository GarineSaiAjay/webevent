-- 0001_baseline: shared helpers used by later migrations.

-- Keeps updated_at honest without trusting every UPDATE statement to set it.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
