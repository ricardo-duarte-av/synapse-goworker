-- Synapse 1.164 DDL for the tables GET /rooms/{id}/state reads, from
-- pg_dump --schema-only of a Synapse database (triggers dropped: their
-- functions are not part of the dump).
CREATE TABLE public.access_tokens (
    id bigint NOT NULL,
    user_id text NOT NULL,
    device_id text,
    token text NOT NULL,
    valid_until_ms bigint,
    puppets_user_id text,
    last_validated bigint,
    refresh_token_id bigint,
    used boolean
);
CREATE TABLE public.blocked_rooms (
    room_id text NOT NULL,
    user_id text NOT NULL
);
CREATE TABLE public.current_state_events (
    event_id text NOT NULL,
    room_id text NOT NULL,
    type text NOT NULL,
    state_key text NOT NULL,
    membership text,
    event_stream_ordering bigint
);
CREATE TABLE public.devices (
    user_id text NOT NULL,
    device_id text NOT NULL,
    display_name text,
    last_seen bigint,
    ip text,
    user_agent text,
    hidden boolean DEFAULT false
);
CREATE TABLE public.event_json (
    event_id text NOT NULL,
    room_id text NOT NULL,
    internal_metadata text NOT NULL,
    "json" text NOT NULL,
    format_version integer
);
CREATE TABLE public.event_to_state_groups (
    event_id text NOT NULL,
    state_group bigint NOT NULL
);
CREATE TABLE public.events (
    topological_ordering bigint NOT NULL,
    event_id text NOT NULL,
    type text NOT NULL,
    room_id text NOT NULL,
    content text,
    unrecognized_keys text,
    processed boolean NOT NULL,
    outlier boolean NOT NULL,
    depth bigint DEFAULT 0 NOT NULL,
    origin_server_ts bigint,
    received_ts bigint,
    sender text,
    contains_url boolean,
    instance_name text,
    stream_ordering bigint,
    state_key text,
    rejection_reason text
);
CREATE TABLE public.local_current_membership (
    room_id text NOT NULL,
    user_id text NOT NULL,
    event_id text NOT NULL,
    membership text NOT NULL,
    event_stream_ordering bigint
);
CREATE TABLE public.partial_state_rooms (
    room_id text NOT NULL,
    device_lists_stream_id bigint DEFAULT 0 NOT NULL,
    join_event_id text,
    joined_via text
);
CREATE TABLE public.redactions (
    event_id text NOT NULL,
    redacts text NOT NULL,
    have_censored boolean DEFAULT false NOT NULL,
    received_ts bigint,
    recheck boolean DEFAULT true NOT NULL
);
CREATE TABLE public.rejections (
    event_id text NOT NULL,
    reason text NOT NULL,
    last_check text NOT NULL
);
CREATE TABLE public.room_memberships (
    event_id text NOT NULL,
    user_id text NOT NULL,
    sender text NOT NULL,
    room_id text NOT NULL,
    membership text NOT NULL,
    forgotten integer DEFAULT 0,
    display_name text,
    avatar_url text,
    event_stream_ordering bigint,
    participant boolean DEFAULT false
);
CREATE TABLE public.rooms (
    room_id text NOT NULL,
    is_public boolean,
    creator text,
    room_version text,
    has_auth_chain_index boolean
);
CREATE TABLE public.state_group_edges (
    state_group bigint NOT NULL,
    prev_state_group bigint NOT NULL
);
CREATE TABLE public.state_groups (
    id bigint NOT NULL,
    room_id text NOT NULL,
    event_id text NOT NULL
);
CREATE TABLE public.state_groups_state (
    state_group bigint NOT NULL,
    room_id text NOT NULL,
    type text NOT NULL,
    state_key text NOT NULL,
    event_id text NOT NULL
);
ALTER TABLE ONLY public.state_groups_state ALTER COLUMN state_group SET (n_distinct=-0.02);
CREATE TABLE public.users (
    name text,
    password_hash text,
    creation_ts bigint,
    admin smallint DEFAULT 0 NOT NULL,
    upgrade_ts bigint,
    is_guest smallint DEFAULT 0 NOT NULL,
    appservice_id text,
    consent_version text,
    consent_server_notice_sent text,
    user_type text,
    deactivated smallint DEFAULT 0 NOT NULL,
    shadow_banned boolean,
    consent_ts bigint,
    approved boolean,
    locked boolean DEFAULT false NOT NULL,
    suspended boolean DEFAULT false NOT NULL
);
ALTER TABLE ONLY public.access_tokens
    ADD CONSTRAINT access_tokens_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.access_tokens
    ADD CONSTRAINT access_tokens_token_key UNIQUE (token);
ALTER TABLE ONLY public.current_state_events
    ADD CONSTRAINT current_state_events_event_id_key UNIQUE (event_id);
ALTER TABLE ONLY public.current_state_events
    ADD CONSTRAINT current_state_events_room_id_type_state_key_key UNIQUE (room_id, type, state_key);
ALTER TABLE ONLY public.devices
    ADD CONSTRAINT device_uniqueness UNIQUE (user_id, device_id);
ALTER TABLE ONLY public.event_json
    ADD CONSTRAINT event_json_event_id_key UNIQUE (event_id);
ALTER TABLE ONLY public.event_to_state_groups
    ADD CONSTRAINT event_to_state_groups_event_id_key UNIQUE (event_id);
ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_event_id_key UNIQUE (event_id);
ALTER TABLE ONLY public.partial_state_rooms
    ADD CONSTRAINT partial_state_rooms_pkey PRIMARY KEY (room_id);
ALTER TABLE ONLY public.redactions
    ADD CONSTRAINT redactions_event_id_key UNIQUE (event_id);
ALTER TABLE ONLY public.rejections
    ADD CONSTRAINT rejections_event_id_key UNIQUE (event_id);
ALTER TABLE ONLY public.room_memberships
    ADD CONSTRAINT room_memberships_event_id_key UNIQUE (event_id);
ALTER TABLE ONLY public.rooms
    ADD CONSTRAINT rooms_pkey PRIMARY KEY (room_id);
ALTER TABLE ONLY public.state_groups
    ADD CONSTRAINT state_groups_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_name_key UNIQUE (name);
CREATE INDEX access_tokens_device_id ON public.access_tokens USING btree (user_id, device_id);
CREATE INDEX access_tokens_refresh_token_id_idx ON public.access_tokens USING btree (refresh_token_id);
CREATE UNIQUE INDEX blocked_rooms_idx ON public.blocked_rooms USING btree (room_id);
CREATE INDEX current_state_events_member_index ON public.current_state_events USING btree (state_key) WHERE (type = 'm.room.member'::text);
CREATE INDEX current_state_events_members_room_index ON public.current_state_events USING btree (room_id, membership) WHERE (type = 'm.room.member'::text);
CREATE INDEX current_state_events_stream_ordering_idx ON public.current_state_events USING btree (event_stream_ordering);
CREATE INDEX event_contains_url_index ON public.events USING btree (room_id, topological_ordering, stream_ordering) WHERE ((contains_url = true) AND (outlier = false));
CREATE INDEX event_to_state_groups_sg_index ON public.event_to_state_groups USING btree (state_group);
CREATE INDEX events_jump_to_date_idx ON public.events USING btree (room_id, origin_server_ts) WHERE (NOT outlier);
CREATE INDEX events_order_room ON public.events USING btree (room_id, topological_ordering, stream_ordering);
CREATE INDEX events_room_stream ON public.events USING btree (room_id, stream_ordering);
CREATE UNIQUE INDEX events_stream_ordering ON public.events USING btree (stream_ordering);
CREATE INDEX events_ts ON public.events USING btree (origin_server_ts, stream_ordering);
CREATE UNIQUE INDEX local_current_membership_idx ON public.local_current_membership USING btree (user_id, room_id);
CREATE INDEX local_current_membership_room_idx ON public.local_current_membership USING btree (room_id);
CREATE INDEX local_current_membership_stream_ordering_idx ON public.local_current_membership USING btree (event_stream_ordering);
CREATE INDEX public_room_index ON public.rooms USING btree (is_public);
CREATE INDEX received_ts_idx ON public.events USING btree (received_ts) WHERE (type = 'm.room.member'::text);
CREATE INDEX redactions_have_censored_ts ON public.redactions USING btree (received_ts) WHERE (NOT have_censored);
CREATE INDEX redactions_redacts ON public.redactions USING btree (redacts);
CREATE INDEX room_membership_user_room_idx ON public.room_memberships USING btree (user_id, room_id);
CREATE INDEX room_memberships_room_id ON public.room_memberships USING btree (room_id);
CREATE INDEX room_memberships_stream_ordering_idx ON public.room_memberships USING btree (event_stream_ordering);
CREATE INDEX room_memberships_user_id ON public.room_memberships USING btree (user_id);
CREATE INDEX room_memberships_user_room_forgotten ON public.room_memberships USING btree (user_id, room_id) WHERE (forgotten = 1);
CREATE INDEX state_group_edges_prev_idx ON public.state_group_edges USING btree (prev_state_group);
CREATE UNIQUE INDEX state_group_edges_unique_idx ON public.state_group_edges USING btree (state_group, prev_state_group);
CREATE INDEX state_groups_room_id_idx ON public.state_groups USING btree (room_id);
CREATE INDEX state_groups_state_type_idx ON public.state_groups_state USING btree (state_group, type, state_key);
CREATE INDEX users_creation_ts ON public.users USING btree (creation_ts);
ALTER TABLE ONLY public.current_state_events
    ADD CONSTRAINT event_stream_ordering_fkey FOREIGN KEY (event_stream_ordering) REFERENCES public.events(stream_ordering) NOT VALID;
ALTER TABLE ONLY public.local_current_membership
    ADD CONSTRAINT event_stream_ordering_fkey FOREIGN KEY (event_stream_ordering) REFERENCES public.events(stream_ordering) NOT VALID;
ALTER TABLE ONLY public.room_memberships
    ADD CONSTRAINT event_stream_ordering_fkey FOREIGN KEY (event_stream_ordering) REFERENCES public.events(stream_ordering) NOT VALID;
ALTER TABLE ONLY public.partial_state_rooms
    ADD CONSTRAINT partial_state_rooms_join_event_id_fkey FOREIGN KEY (join_event_id) REFERENCES public.events(event_id);
ALTER TABLE ONLY public.partial_state_rooms
    ADD CONSTRAINT partial_state_rooms_room_id_fkey FOREIGN KEY (room_id) REFERENCES public.rooms(room_id);
