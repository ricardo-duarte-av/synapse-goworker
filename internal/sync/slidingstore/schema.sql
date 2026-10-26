-- Sliding sync's per-connection state, created by slidingstore.Open when it is
-- missing. The role and the schema's ownership are deploy/sliding-sync-role.sql's
-- business; this file is only the tables, so that whichever role the worker
-- connects as owns them.
--
-- Every name is qualified with `gosync.`: Synapse has tables of the SAME NAMES
-- in `public`. Idempotent throughout, and executed as one implicit transaction.
--
-- The shape mirrors Synapse's (schema deltas 87/02 and 93/02) so the port stays
-- readable against the original. Where it differs, it says so.

-- One row per logical client connection. Element X runs THREE per device
-- (conn_id "room-list", "notifications", and ""), so this is keyed by the
-- triple and not by the device.
CREATE TABLE IF NOT EXISTS gosync.sliding_sync_connections (
    connection_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id TEXT NOT NULL,
    effective_device_id TEXT NOT NULL,
    conn_id TEXT NOT NULL,
    created_ts BIGINT NOT NULL,
    last_used_ts BIGINT
);
CREATE INDEX IF NOT EXISTS sliding_sync_connections_idx
    ON gosync.sliding_sync_connections (user_id, effective_device_id, conn_id);
CREATE INDEX IF NOT EXISTS sliding_sync_connections_ts_idx
    ON gosync.sliding_sync_connections (created_ts);
CREATE INDEX IF NOT EXISTS sliding_sync_connections_last_used_ts_idx
    ON gosync.sliding_sync_connections (last_used_ts) WHERE last_used_ts IS NOT NULL;

-- The `pos` token's left half. Identity, because the client only ever hands it
-- back to us and it must be unguessable-adjacent and monotonic; ownership is
-- checked against the connection triple on every use, since a `pos` is a
-- user-supplied value.
--
-- Normally at most two rows live per connection: the one the client is using
-- and the one we just wrote. Reading a position deletes the others, which is
-- what keeps these tables from growing without bound -- see the note on
-- forking below.
CREATE TABLE IF NOT EXISTS gosync.sliding_sync_connection_positions (
    connection_position BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    connection_key BIGINT NOT NULL
        REFERENCES gosync.sliding_sync_connections (connection_key) ON DELETE CASCADE,
    created_ts BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS sliding_sync_connection_positions_key
    ON gosync.sliding_sync_connection_positions (connection_key);
CREATE INDEX IF NOT EXISTS sliding_sync_connection_positions_ts_idx
    ON gosync.sliding_sync_connection_positions (created_ts);

-- required_state configs, deduplicated per connection. Many rooms share one
-- config, and on the live server this is the largest of the six tables by row
-- count even so.
CREATE TABLE IF NOT EXISTS gosync.sliding_sync_connection_required_state (
    required_state_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    connection_key BIGINT NOT NULL
        REFERENCES gosync.sliding_sync_connections (connection_key) ON DELETE CASCADE,
    required_state TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS sliding_sync_connection_required_state_conn_pos
    ON gosync.sliding_sync_connection_required_state (connection_key);

-- What we told the connection about each room, per position. Rows are copied
-- forward from the previous position and then the changes upserted, so each
-- position is a complete snapshot. Measured on the live server: ~248 rows per
-- position for a 654-room connection.
CREATE TABLE IF NOT EXISTS gosync.sliding_sync_connection_room_configs (
    connection_position BIGINT NOT NULL
        REFERENCES gosync.sliding_sync_connection_positions (connection_position) ON DELETE CASCADE,
    room_id TEXT NOT NULL,
    timeline_limit BIGINT NOT NULL,
    required_state_id BIGINT NOT NULL
        REFERENCES gosync.sliding_sync_connection_required_state (required_state_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS sliding_sync_connection_room_configs_idx
    ON gosync.sliding_sync_connection_room_configs (connection_position, room_id);
CREATE INDEX IF NOT EXISTS sliding_sync_connection_room_configs_required_state_id_idx
    ON gosync.sliding_sync_connection_room_configs (required_state_id);

-- "Have I sent this room down this stream, and if so up to what point?"
-- room_status is 'live' or 'previously'; last_token is set only for
-- 'previously'. Same copy-forward-then-upsert as room_configs; ~725 rows per
-- position for that same 654-room connection.
CREATE TABLE IF NOT EXISTS gosync.sliding_sync_connection_streams (
    connection_position BIGINT NOT NULL
        REFERENCES gosync.sliding_sync_connection_positions (connection_position) ON DELETE CASCADE,
    stream TEXT NOT NULL,
    room_id TEXT NOT NULL,
    room_status TEXT NOT NULL,
    last_token TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS sliding_sync_connection_streams_idx
    ON gosync.sliding_sync_connection_streams (connection_position, room_id, stream);

-- Which lazily-loaded memberships this connection has already been given.
--
-- Explicitly a CACHE: dropping a row costs a member event sent twice, never a
-- wrong answer, so entries may be evicted freely. connection_position is NULL
-- for entries a client has confirmed (they hold for every future position on
-- the connection) and set for entries written against a position not yet
-- acknowledged, which is how a forked request is unwound.
--
-- Created LAST: Open treats its existence as "the schema is complete".
CREATE TABLE IF NOT EXISTS gosync.sliding_sync_connection_lazy_members (
    connection_key BIGINT NOT NULL
        REFERENCES gosync.sliding_sync_connections (connection_key) ON DELETE CASCADE,
    connection_position BIGINT
        REFERENCES gosync.sliding_sync_connection_positions (connection_position) ON DELETE CASCADE,
    room_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    last_seen_ts BIGINT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS sliding_sync_connection_lazy_members_idx
    ON gosync.sliding_sync_connection_lazy_members (connection_key, room_id, user_id);
CREATE INDEX IF NOT EXISTS sliding_sync_connection_lazy_members_conn_pos_idx
    ON gosync.sliding_sync_connection_lazy_members (connection_position)
    WHERE connection_position IS NOT NULL;
CREATE INDEX IF NOT EXISTS sliding_sync_connection_lazy_members_pos_idx
    ON gosync.sliding_sync_connection_lazy_members (connection_key, connection_position)
    WHERE connection_position IS NOT NULL;
