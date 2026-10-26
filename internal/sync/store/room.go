package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// ErrNotFound is returned when a room or event does not exist.
var ErrNotFound = errors.New("not found")

// RoomInfo is what the handler needs to know about a room before serving it.
type RoomInfo struct {
	RoomVersion string
	Blocked     bool
}

// RoomInfo loads a room's version and whether it has been blocked.
//
// Synapse checks `is_room_blocked` first and answers 403 before doing anything
// else, so this is one query rather than two.
// Cached without invalidation: a room's version is fixed at creation, and an
// upgrade produces a NEW room with a new id rather than changing this one.
//
// `blocked` can in principle be set later by an admin. It is cached with the
// version anyway because the pair comes from one row, and a room blocked
// mid-sync is corrected on the next purge; treating it as live would mean a
// query per room per sync for a column that is almost always false.
func (s *Store) RoomInfo(ctx context.Context, roomID string) (RoomInfo, error) {
	if info, ok := s.derived.roomInfo.Get(roomID); ok {
		return info, nil
	}
	info, err := s.roomInfo(ctx, roomID)
	if err != nil {
		return RoomInfo{}, err
	}
	s.derived.roomInfo.Add(roomID, info)
	return info, nil
}

func (s *Store) roomInfo(ctx context.Context, roomID string) (RoomInfo, error) {
	const q = `
		SELECT r.room_version,
		       EXISTS (SELECT 1 FROM blocked_rooms b WHERE b.room_id = r.room_id)
		  FROM rooms r
		 WHERE r.room_id = $1`
	var info RoomInfo
	err := s.queryRow(ctx, "RoomInfo", q, roomID).Scan(&info.RoomVersion, &info.Blocked)
	if errors.Is(err, pgx.ErrNoRows) {
		return RoomInfo{}, ErrNotFound
	}
	if err != nil {
		return RoomInfo{}, fmt.Errorf("store: room info: %w", err)
	}
	return info, nil
}

// HistoryVisibility reads a room's current m.room.history_visibility setting,
// or "" when it has none.
//
// Read on its own rather than through the full state map, because the one
// caller -- deciding whether a non-member may peek at a room over /events --
// needs this single key and nothing else.
//
// The JSON is parsed in Go rather than cast to jsonb in SQL. Events on this
// server contain escaped NUL characters that PostgreSQL cannot cast, and while
// a history_visibility event is an unlikely place to find one, a query that
// fails on some rooms and not others is a worse failure than a slightly longer
// one that never does.
// Cached per room and invalidated by any event in that room.
//
// Guarded, and the guard earns its keep here more than anywhere: this decides
// whether events are shown at all, so a stale answer does not merely look
// wrong, it either leaks history or hides it.
func (s *Store) HistoryVisibility(ctx context.Context, roomID string) (string, error) {
	use := s.derived.fresh(ctx, streamEvents)
	if use {
		if vis, ok := s.derived.historyVis.Get(roomID); ok {
			return vis, nil
		}
	}
	vis, err := s.historyVisibility(ctx, roomID)
	if err != nil {
		return "", err
	}
	if use {
		s.derived.historyVis.Add(roomID, vis)
	}
	return vis, nil
}

func (s *Store) historyVisibility(ctx context.Context, roomID string) (string, error) {
	const q = `
		SELECT ej.json
		  FROM current_state_events cse JOIN event_json ej USING (event_id)
		 WHERE cse.room_id = $1 AND cse.type = 'm.room.history_visibility'
		   AND cse.state_key = ''`
	var body []byte
	err := s.queryRow(ctx, "HistoryVisibility", q, roomID).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("store: history visibility: %w", err)
	}
	return gjson.GetBytes(body, "content.history_visibility").String(), nil
}

// Membership is a user's current membership in a room.
type Membership struct {
	// Membership is join, leave, invite, knock or ban. Empty if the user has
	// no membership event at all in the room.
	Membership string
	// EventID is the membership event. Empty when Membership is.
	EventID string
}

// CurrentMembership reads the user's membership from the room's current state.
//
// This is the current-state view, which is what Synapse's
// check_user_in_room_or_world_readable uses. It deliberately says nothing about
// what the membership was at some earlier point in the timeline; that is a
// different question, answered per event during visibility filtering.
func (s *Store) CurrentMembership(ctx context.Context, roomID, userID string) (Membership, error) {
	const q = `
		SELECT COALESCE(membership, ''), event_id
		  FROM current_state_events
		 WHERE room_id = $1 AND type = 'm.room.member' AND state_key = $2`
	var m Membership
	err := s.queryRow(ctx, "CurrentMembership", q, roomID, userID).Scan(&m.Membership, &m.EventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Membership{}, nil
	}
	if err != nil {
		return Membership{}, fmt.Errorf("store: current membership: %w", err)
	}
	return m, nil
}

// StateEvent is one entry of a room's current state.
type StateEvent struct {
	clientevent.Stored
	StateKey string
	// Sender is kept out of the serialised JSON path but is needed to work out
	// which users' presence to include.
	Sender string
}

// CurrentState loads every current state event of a room.
//
// Ordered by (type, state_key) so the result is stable. Synapse returns a dict
// and therefore has no order at all, which means the comparator must treat the
// state block as a set -- but an unstable order here would still make our own
// output differ run to run, which makes debugging needlessly hard.
func (s *Store) CurrentState(ctx context.Context, roomID, roomVersion string) ([]StateEvent, error) {
	const q = `
		SELECT cse.event_id, cse.type, cse.state_key, e.sender,
		       ej.json, ej.internal_metadata
		  FROM current_state_events cse
		  JOIN events e USING (event_id)
		  JOIN event_json ej USING (event_id)
		 WHERE cse.room_id = $1
		 ORDER BY cse.type, cse.state_key`
	rows, err := s.query(ctx, "CurrentState", q, roomID)
	if err != nil {
		return nil, fmt.Errorf("store: current state: %w", err)
	}
	defer rows.Close()

	var out []StateEvent
	for rows.Next() {
		var ev StateEvent
		if err := rows.Scan(&ev.EventID, &ev.Type, &ev.StateKey, &ev.Sender,
			&ev.JSON, &ev.InternalMetadata); err != nil {
			return nil, fmt.Errorf("store: current state: %w", err)
		}
		ev.RoomID = roomID
		ev.RoomVersion = roomVersion
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: current state: %w", err)
	}
	return out, nil
}

// TimelineEvent is one event of a room's recent history.
type TimelineEvent struct {
	clientevent.Stored
	Sender           string
	StreamOrdering   int64
	TopologicalOrder int64
	InstanceName     string
	// StateKey is empty for a non-state event; IsState distinguishes a state
	// event with an empty state key from one with none.
	StateKey string
	// IsState is true when the event has a state_key. Retention and the ignore
	// list treat state events differently from messages.
	IsState bool
}

// RecentEvents returns the most recent events in a room, in ascending order,
// along with a token pointing just before the earliest one returned.
//
// This is Synapse's get_recent_events_for_room ->
// _paginate_room_events_by_topological_ordering_txn, paginating BACKWARDS from
// end. Three details of that function are load-bearing:
//
//   - It selects `limit * 2` rows, because the result set is filtered
//     afterwards (by the vector-clock bound, and by visibility upstream) and it
//     would otherwise come up short.
//   - It orders by topological_ordering then stream_ordering, not by stream
//     alone. For live tokens these agree; for backfilled history they do not.
//   - The returned token is `stream - 1` of the OLDEST row, because tokens name
//     positions between events. Off by one here means a client paginating
//     backwards silently re-fetches or skips an event.
func (s *Store) RecentEvents(ctx context.Context, roomID, roomVersion string, limit int,
	end streamtoken.RoomKey) ([]TimelineEvent, streamtoken.RoomKey, error) {

	if limit <= 0 {
		return nil, end, nil
	}

	// Synapse bounds by the highest position any writer reached and filters the
	// rows afterwards, because a writer ahead of the agreed minimum has already
	// persisted events the token covers.
	var (
		sql  string
		args []any
	)
	if end.IsHistorical() {
		sql = `
			SELECT e.event_id, e.type, e.sender, e.stream_ordering,
			       e.topological_ordering, COALESCE(e.instance_name, ''),
			       COALESCE(e.state_key, ''), e.state_key IS NOT NULL, ej.json, ej.internal_metadata,
			       e.rejection_reason IS NULL
			  FROM events e JOIN event_json ej USING (event_id)
			 WHERE e.outlier = FALSE AND e.room_id = $1
			   AND (e.topological_ordering < $2
			        OR (e.topological_ordering = $2 AND e.stream_ordering <= $3))
			 ORDER BY e.topological_ordering DESC, e.stream_ordering DESC
			 LIMIT $4`
		args = []any{roomID, end.Topological, end.Stream, limit * 2}
	} else {
		sql = `
			SELECT e.event_id, e.type, e.sender, e.stream_ordering,
			       e.topological_ordering, COALESCE(e.instance_name, ''),
			       COALESCE(e.state_key, ''), e.state_key IS NOT NULL, ej.json, ej.internal_metadata,
			       e.rejection_reason IS NULL
			  FROM events e JOIN event_json ej USING (event_id)
			 WHERE e.outlier = FALSE AND e.room_id = $1
			   AND e.stream_ordering <= $2
			 ORDER BY e.topological_ordering DESC, e.stream_ordering DESC
			 LIMIT $3`
		args = []any{roomID, end.MaxStreamPos(), limit * 2}
	}

	rows, err := s.query(ctx, "RecentEvents", sql, args...)
	if err != nil {
		return nil, end, fmt.Errorf("store: recent events: %w", err)
	}
	defer rows.Close()

	// Descending: newest first.
	var (
		descending []TimelineEvent
		accepted   []bool
	)
	for rows.Next() {
		var (
			ev TimelineEvent
			ok bool
		)
		if err := rows.Scan(&ev.EventID, &ev.Type, &ev.Sender, &ev.StreamOrdering,
			&ev.TopologicalOrder, &ev.InstanceName, &ev.StateKey, &ev.IsState,
			&ev.JSON, &ev.InternalMetadata, &ok); err != nil {
			return nil, end, fmt.Errorf("store: recent events: %w", err)
		}
		ev.RoomID = roomID
		ev.RoomVersion = roomVersion
		descending = append(descending, ev)
		accepted = append(accepted, ok)
	}
	if err := rows.Err(); err != nil {
		return nil, end, fmt.Errorf("store: recent events: %w", err)
	}

	if len(descending) > limit {
		descending = descending[:limit]
		accepted = accepted[:limit]
	}
	if len(descending) == 0 {
		return nil, end, nil
	}

	// A token names a position *between* events, so the start of this chunk is
	// one before the oldest event PAGED -- rejected events took their slot in
	// the limit before being dropped, exactly as they do upstream.
	oldest := descending[len(descending)-1]
	start := streamtoken.Historical(oldest.TopologicalOrder, oldest.StreamOrdering-1)

	ascending := make([]TimelineEvent, 0, len(descending))
	for i := len(descending) - 1; i >= 0; i-- {
		if accepted[i] {
			ascending = append(ascending, descending[i])
		}
	}
	return ascending, start, nil
}

// IgnoredUsers reads the caller's m.ignored_user_list.
//
// Needed once per sync rather than per room, because it decides which INVITES
// are reported at all: Synapse drops an invite whose sender the caller has
// ignored, in both the initial and the incremental path. Ignoring someone and
// still being shown their invitations is the one case where ignoring plainly
// does not work.
// Cached per user and invalidated by that user's account data.
//
// This one FEEDS A DELTA -- it decides which invites are reported, and an
// invite is reported once, from the membership changes in the window. Drop an
// invite because the ignore list was a moment stale and no later sync mentions
// it again. Hence the guard: below the horizon we ask the database.
func (s *Store) IgnoredUsers(ctx context.Context, userID string) (map[string]bool, error) {
	use := s.derived.fresh(ctx, streamAccountData)
	if use {
		if ignored, ok := s.derived.ignoredUsers.Get(userID); ok {
			return copyStringSet(ignored), nil
		}
	}
	ignored, err := s.ignoredUsers(ctx, userID)
	if err != nil {
		return nil, err
	}
	if use {
		s.derived.ignoredUsers.Add(userID, ignored)
	}
	return copyStringSet(ignored), nil
}

func (s *Store) ignoredUsers(ctx context.Context, userID string) (map[string]bool, error) {
	const q = `
		SELECT content FROM account_data
		 WHERE user_id = $1 AND account_data_type = 'm.ignored_user_list'`
	var raw *string
	err := s.queryRow(ctx, "IgnoredUsers", q, userID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: ignored users: %w", err)
	}
	if raw == nil {
		return nil, nil
	}
	var parsed struct {
		IgnoredUsers map[string]json.RawMessage `json:"ignored_users"`
	}
	if err := json.Unmarshal([]byte(*raw), &parsed); err != nil {
		return nil, fmt.Errorf("store: parsing ignore list: %w", err)
	}
	if len(parsed.IgnoredUsers) == 0 {
		return nil, nil
	}
	out := make(map[string]bool, len(parsed.IgnoredUsers))
	for user := range parsed.IgnoredUsers {
		out[user] = true
	}
	return out, nil
}

// VisibilityExtras are the per-request facts a visibility decision needs
// beyond the room state resolved at each event.
type VisibilityExtras struct {
	// ReturnSoftFailed and ReturnPolicySpammy come from a server admin's
	// `io.element.synapse.admin_client_config`. They have no effect for anyone
	// else. See VisibilityExtras' query.
	ReturnSoftFailed   bool
	ReturnPolicySpammy bool

	IgnoredSenders         map[string]bool
	ErasedSenders          map[string]bool
	RetentionMaxLifetimeMS int64
}

// VisibilityExtras loads the caller's ignore list, which of the given senders
// have been erased, and the room's retention policy.
//
// One query rather than three: none of them is large, and a visibility
// decision needs all three before it can judge a single event.
func (s *Store) VisibilityExtras(ctx context.Context, roomID, userID string,
	senders []string) (VisibilityExtras, error) {

	const q = `
		SELECT
			(SELECT content FROM account_data
			  WHERE user_id = $1 AND account_data_type = 'm.ignored_user_list'),
			ARRAY(SELECT user_id FROM erased_users WHERE user_id = ANY($2)),
			(SELECT max_lifetime FROM room_retention
			  WHERE room_id = $3 ORDER BY event_id DESC LIMIT 1),
			COALESCE((SELECT admin FROM users WHERE name = $1), 0),
			(SELECT content FROM account_data
			  WHERE user_id = $1
			    AND account_data_type = 'io.element.synapse.admin_client_config')`

	var (
		ignoredRaw  *string
		erased      []string
		maxLifetime *int64
		isAdmin     int
		adminCfgRaw *string
	)
	if err := s.queryRow(ctx, "VisibilityExtras", q, userID, senders, roomID).Scan(
		&ignoredRaw, &erased, &maxLifetime, &isAdmin, &adminCfgRaw); err != nil {
		return VisibilityExtras{}, fmt.Errorf("store: visibility extras: %w", err)
	}

	out := VisibilityExtras{}
	if ignoredRaw != nil {
		var parsed struct {
			IgnoredUsers map[string]json.RawMessage `json:"ignored_users"`
		}
		if err := json.Unmarshal([]byte(*ignoredRaw), &parsed); err != nil {
			return VisibilityExtras{}, fmt.Errorf("store: parsing ignore list: %w", err)
		}
		if len(parsed.IgnoredUsers) > 0 {
			out.IgnoredSenders = make(map[string]bool, len(parsed.IgnoredUsers))
			for user := range parsed.IgnoredUsers {
				out.IgnoredSenders[user] = true
			}
		}
	}
	if len(erased) > 0 {
		out.ErasedSenders = make(map[string]bool, len(erased))
		for _, user := range erased {
			out.ErasedSenders[user] = true
		}
	}
	if maxLifetime != nil {
		out.RetentionMaxLifetimeMS = *maxLifetime
	}

	// A SERVER ADMIN can ask to be shown soft-failed events, and Synapse honours
	// it (visibility.py: get_admin_client_config_for_user). Both halves are
	// required -- the account data alone does nothing for an ordinary user --
	// which is why the admin flag is read here rather than trusted from the
	// setting's presence.
	//
	// Found on 2026-09-03, and only because a sliding sync comparison ran
	// against the server owner's account: the test accounts are not admins, so
	// classic sync had matched for weeks without this. One event differed.
	if isAdmin != 0 && adminCfgRaw != nil {
		var cfg struct {
			ReturnSoftFailed   bool `json:"return_soft_failed_events"`
			ReturnPolicySpammy bool `json:"return_policy_server_spammy_events"`
		}
		if err := json.Unmarshal([]byte(*adminCfgRaw), &cfg); err == nil {
			out.ReturnSoftFailed = cfg.ReturnSoftFailed
			out.ReturnPolicySpammy = cfg.ReturnPolicySpammy
		}
	}
	return out, nil
}

// AttachPrevContent fills in `unsigned.prev_content` and `unsigned.prev_sender`
// for state events that replace an earlier one.
//
// Synapse does this in the storage layer, not the serialiser
// (events_worker.py:790, `get_prev_content`), which matters for ordering: the
// fields are in `unsigned` *before* the client format transform runs, so the
// v1 format lifts `prev_content` to the top level along with the other five
// copy keys. Doing it after serialisation would produce `unsigned.prev_content`
// with no top-level twin, and differ from Synapse on every membership change.
//
// The replaced events are fetched in one query rather than one per event: a
// room's timeline can carry many membership changes, and Synapse's own
// per-event get_event calls are served from a cache this worker does not have.
func (s *Store) AttachPrevContent(ctx context.Context, events []*clientevent.Stored) error {
	type target struct {
		event    *clientevent.Stored
		replaces string
	}
	var targets []target
	idSet := map[string]bool{}
	for _, ev := range events {
		unsigned := gjson.GetBytes(ev.JSON, "unsigned")
		replaces := unsigned.Get("replaces_state")
		if !replaces.Exists() || replaces.Type != gjson.String {
			continue
		}
		// Synapse skips events that already carry both fields.
		if unsigned.Get("prev_content").Exists() && unsigned.Get("prev_sender").Exists() {
			continue
		}
		targets = append(targets, target{event: ev, replaces: replaces.String()})
		idSet[replaces.String()] = true
	}
	if len(targets) == 0 {
		return nil
	}

	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}

	const q = `
		SELECT e.event_id, e.sender, ej.json
		  FROM events e JOIN event_json ej USING (event_id)
		 WHERE e.event_id = ANY($1)`
	rows, err := s.query(ctx, "AttachPrevContent", q, ids)
	if err != nil {
		return fmt.Errorf("store: prev content: %w", err)
	}
	defer rows.Close()

	type prev struct {
		sender  string
		content json.RawMessage
	}
	prevs := make(map[string]prev, len(ids))
	for rows.Next() {
		var id, sender string
		var body []byte
		if err := rows.Scan(&id, &sender, &body); err != nil {
			return fmt.Errorf("store: prev content: %w", err)
		}
		prevs[id] = prev{sender: sender, content: json.RawMessage(gjson.GetBytes(body, "content").Raw)}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: prev content: %w", err)
	}

	for _, t := range targets {
		p, ok := prevs[t.replaces]
		if !ok {
			// Synapse passes allow_none=True: a replaced event that has been
			// purged simply yields no prev_content.
			continue
		}
		body, err := sjson.SetRawBytes(t.event.JSON, "unsigned.prev_content", p.content)
		if err != nil {
			return fmt.Errorf("store: prev content: %w", err)
		}
		body, err = sjson.SetBytes(body, "unsigned.prev_sender", p.sender)
		if err != nil {
			return fmt.Errorf("store: prev content: %w", err)
		}
		t.event.JSON = body
	}
	return nil
}

// AdminWantsSoftFailedEvents reports whether a caller is a server admin who has
// asked to be shown soft-failed events.
//
// Both halves are required: the account data setting does nothing for an
// ordinary user. See VisibilityExtras, which reads the same pair for the
// visibility decision -- this is the serialisation side, needed where the event
// config is built rather than where the filter runs, so the two are separate
// lookups of the same fact.
func (s *Store) AdminWantsSoftFailedEvents(ctx context.Context, userID string) (bool, error) {
	const q = `
		SELECT COALESCE((SELECT admin FROM users WHERE name = $1), 0),
		       (SELECT content FROM account_data
		         WHERE user_id = $1
		           AND account_data_type = 'io.element.synapse.admin_client_config')`
	var isAdmin int
	var raw *string
	if err := s.queryRow(ctx, "AdminWantsSoftFailedEvents", q, userID).Scan(&isAdmin, &raw); err != nil {
		return false, fmt.Errorf("store: admin client config: %w", err)
	}
	if isAdmin == 0 || raw == nil {
		return false, nil
	}
	var cfg struct {
		ReturnSoftFailed   bool `json:"return_soft_failed_events"`
		ReturnPolicySpammy bool `json:"return_policy_server_spammy_events"`
	}
	if err := json.Unmarshal([]byte(*raw), &cfg); err != nil {
		return false, nil
	}
	return cfg.ReturnSoftFailed || cfg.ReturnPolicySpammy, nil
}
