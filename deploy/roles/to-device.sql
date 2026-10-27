-- Writing PostgreSQL role for the gosync worker's to-device deletion.
--
-- This is the ONE grant this worker holds that is not a SELECT, and it exists
-- for one reason: Synapse's /sync deletes the to-device messages a device has
-- acknowledged (handlers/sync.py -> delete_messages_for_device, bounded by
-- since_token.to_device_key). A worker that serves the to_device section
-- without deleting hands a client the same room keys on every sync, for ever.
--
-- The grant is deliberately narrow, and the worker CHECKS the narrowness at
-- startup (internal/deviceinbox.Open) rather than trusting this file was run
-- as written. It warns if the role can DELETE from `events` or INSERT into
-- device_inbox, and refuses to start if it is read-only or cannot delete.
--
-- The main connection keeps using the read-only role (readonly-role.sql, or
-- gopro_ro on aguiarvieira.pt). Only internal/deviceinbox uses this one, over
-- a separate pool, so a bug anywhere else still cannot write.
--
-- Run as a superuser against the Synapse database:
--   psql -h /var/sockets -U synapse -d synapse-db -f device-inbox-role.sql
--
-- To undo:  DROP OWNED BY gosync_inbox; DROP ROLE gosync_inbox;

BEGIN;

CREATE ROLE gosync_inbox WITH LOGIN;

GRANT CONNECT ON DATABASE "synapse-db" TO gosync_inbox;
GRANT USAGE ON SCHEMA public TO gosync_inbox;

-- SELECT as well as DELETE: each deletion batch first reads MAX(stream_id)
-- over its window, exactly as Synapse's delete_messages_for_device_between
-- does, because PostgreSQL cannot put a LIMIT on a DELETE.
GRANT SELECT, DELETE ON device_inbox TO gosync_inbox;

-- Deliberately NOT granted: INSERT or UPDATE on device_inbox (only Synapse's
-- inbox writer may put messages there), anything at all on any other table,
-- and any default privileges on future tables. A new Synapse migration should
-- widen the read-only role, never this one.

-- A query that outlives its request is pure waste.
ALTER ROLE gosync_inbox SET statement_timeout = '60s';

COMMIT;

-- Verify: the first two succeed, the rest must all fail.
--   psql -h /var/sockets -U gosync_inbox -d synapse-db \
--     -c 'select count(*) from device_inbox;'
--   psql -h /var/sockets -U gosync_inbox -d synapse-db \
--     -c "delete from device_inbox where user_id = '@nobody:invalid';"
--   psql -h /var/sockets -U gosync_inbox -d synapse-db -c 'select count(*) from events;'
--   psql -h /var/sockets -U gosync_inbox -d synapse-db \
--     -c "insert into device_inbox values (1,'@a:b','C','{}');"
--   psql -h /var/sockets -U gosync_inbox -d synapse-db -c 'delete from events;'
