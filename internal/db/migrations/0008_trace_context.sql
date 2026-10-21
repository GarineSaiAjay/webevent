-- 0008_trace_context: remember the W3C traceparent of the request that
-- created an event, so the asynchronous delivery spans can link back to it.
ALTER TABLE events ADD COLUMN trace_parent text;
