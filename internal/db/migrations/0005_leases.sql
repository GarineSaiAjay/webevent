-- 0005_leases: safe concurrent claiming with time-bounded leases.
--
-- A worker claims a delivery by moving it to 'delivering' and stamping it with
-- a lease: who holds it (locked_by), a random fencing token (lease_token) and
-- when the lease runs out (locked_until). If the worker dies, the lease
-- expires and the reaper puts the delivery back to 'pending'.

ALTER TABLE deliveries DROP CONSTRAINT deliveries_status_check;
ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
    CHECK (status IN ('pending', 'delivering', 'succeeded', 'dead', 'cancelled'));

ALTER TABLE deliveries
    ADD COLUMN locked_by    text,
    ADD COLUMN lease_token  text,
    ADD COLUMN locked_until timestamptz;

ALTER TABLE deliveries ADD CONSTRAINT deliveries_lease_consistent CHECK (
    (status = 'delivering') = (lease_token IS NOT NULL AND locked_until IS NOT NULL AND locked_by IS NOT NULL)
);

-- The reaper's query: leases that have run out.
CREATE INDEX deliveries_lease_idx ON deliveries (locked_until) WHERE status = 'delivering';
