-- Writing PostgreSQL role and schema for sliding sync's per-connection state.
--
-- Sliding sync is not a read-only workload and cannot be made into one. Every
-- response records what it sent so the next request can send only the
-- difference, and even READING that state writes: Synapse's
-- `_get_and_clear_connection_positions_txn` bumps `last_used_ts`, deletes every
-- other position on the connection, and rewrites lazy-member rows before it
-- returns anything. The `pos` token a client carries is literally a sequence
-- value from `sliding_sync_connection_positions` -- it cannot be minted without
-- an INSERT.
--
-- So this worker gets a second writing role, its second ever, and the same
-- containment argument as device-inbox-role.sql applies: the grant is narrow,
-- it lives in one package (internal/slidingstore) behind its own pool, and the
-- worker CHECKS the narrowness at startup rather than trusting this file was
-- run as written, warning (not refusing) if the role can read `public`.
--
-- The narrowness here is a whole schema rather than one table. `gosync_ss`
-- owns everything in `gosync` and has NOTHING in `public` -- it cannot read
-- Synapse's tables, let alone write them. The main pool keeps the read-only
-- role and its check, so "every query in internal/store is a SELECT" stays
-- literally true.
--
-- THE TABLES ARE NOT HERE. The worker creates them at startup when they are
-- missing (internal/slidingstore/schema.sql), as whichever role it connects as,
-- so that role owns them. This file only makes the role and hands it the schema.
--
-- SINGLE DATABASE USER: skip this file entirely if that user has CREATE on the
-- database -- the worker creates the schema too. Otherwise run it with gosync_ss
-- replaced by that user, and DELETE the two ALTER ROLE lines: on a role Synapse
-- also uses they would redirect Synapse's own queries away from `public`.
--
-- WHY OUR OWN TABLES RATHER THAN SYNAPSE'S
--
-- Synapse has these six tables already, and writing to them would let a client
-- move between this worker and Synapse's with its `pos` intact. That was
-- considered and rejected (docs/decisions.md, 2026-09-03): it means a bug in
-- our connection bookkeeping corrupts the state of real clients on the real
-- hostname. Ours are separate, so the blast radius of a mistake is the split
-- host and nothing else. The cost is that our `pos` is not interchangeable
-- with Synapse's -- a client switching hosts mid-connection gets
-- M_UNKNOWN_POS and re-bootstraps, which is a supported path that 9.3% of live
-- requests already exercise.
--
-- Run as a superuser against the Synapse database:
--   psql -h /var/sockets -U synapse -d synapse-db -f sliding-sync-role.sql
--
-- To undo:  DROP SCHEMA gosync CASCADE; DROP ROLE gosync_ss;

BEGIN;

-- Idempotent throughout, so re-running is safe. CREATE ROLE has no IF NOT
-- EXISTS.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gosync_ss') THEN
        CREATE ROLE gosync_ss WITH LOGIN;
    END IF;
END $$;

GRANT CONNECT ON DATABASE "synapse-db" TO gosync_ss;

-- Owning the schema is what lets the role create the tables in it. A schema
-- created by an earlier version of this file already holds tables owned by
-- gosync_ss (that version transferred them), so nothing changes for it.
CREATE SCHEMA IF NOT EXISTS gosync AUTHORIZATION gosync_ss;

-- Deliberately NOT granted: USAGE on schema public. Without it this role
-- cannot so much as name Synapse's tables, which is the guarantee the startup
-- check verifies. The worker also pins search_path = gosync on its own
-- connections, so this setting is belt and braces.
ALTER ROLE gosync_ss SET search_path = gosync;
ALTER ROLE gosync_ss SET statement_timeout = '60s';

COMMIT;

-- Verify, after the worker has started once: the first succeeds, the rest must
-- all fail.
--   psql -h /var/sockets -U gosync_ss -d synapse-db \
--     -c 'select count(*) from sliding_sync_connections;'
--   psql -h /var/sockets -U gosync_ss -d synapse-db -c 'select count(*) from public.events;'
--   psql -h /var/sockets -U gosync_ss -d synapse-db \
--     -c 'select count(*) from public.sliding_sync_connections;'
--   psql -h /var/sockets -U gosync_ss -d synapse-db -c 'delete from public.device_inbox;'
