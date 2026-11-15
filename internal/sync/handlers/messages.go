package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/eventfilter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/filter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// roomMessagesPath is RoomMessageListRestServlet's pattern (client_patterns
// with v1), matched against the raw path.
var roomMessagesPath = regexp.MustCompile(
	`^/_matrix/client/(?:api/v1|r0|v3|unstable)/rooms/([^/]*)/messages$`)

// Constants of the pagination handler and of backfill.
const (
	// messagesMaxLimit is PaginationConfig's MAX_LIMIT.
	messagesMaxLimit = 1000
	// numberOfEventsToBackfill is NUMBER_OF_EVENTS_TO_BACKFILL, the padding
	// _maybe_backfill_inner adds to the depth it looks for backfill points
	// near.
	numberOfEventsToBackfill = 100
	// maxDepth is MAX_DEPTH (CANONICALJSON_MAX_INT).
	maxDepth = int64(1)<<53 - 1
)

// RoomMessages serves GET /rooms/{roomId}/messages: RoomMessageListRestServlet
// over PaginationHandler.get_messages, for every request in which Synapse
// would not backfill.
//
// Backfill stays Synapse's. get_messages calls maybe_backfill on every
// backwards page, in the foreground or the background; a request for which
// _maybe_backfill_inner would go further than its database checks (a
// backfill point near the page, not in its failed-pull backoff; or one
// further on, which it backfills in the background at MAX_DEPTH), or would
// wait on a purge, is handed to Synapse whole, so that Synapse decides,
// fetches and answers as it does today. The rest never contacts another
// server in Synapse either, and is served here.
//
// Also left to Synapse: a query Synapse would refuse (its 400 texts) or
// read by Python's rules (a parameter not a plain integer, a filter of a
// type Synapse coerces, a token with an instance map, whose names need the
// instance_map table), a room whose state is partial, a room without a
// known version, an event whose sender is erased (Synapse serves it
// pruned), and a page whose first event has no state group when its
// members are lazy-loaded.
func RoomMessages(d Deps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ann := server.Annotate(r.Context())
		setEndpoint(ann, "room_messages")
		m := roomMessagesPath.FindStringSubmatch(r.URL.EscapedPath())
		ok := m != nil
		var roomID string
		if ok {
			roomID, ok = unquote(m[1], ok)
		}
		if !ok {
			handBack(w, r, d, ann, "path")
			return
		}

		verdict, ok := authenticate(w, r, d, ann)
		if !ok {
			return
		}

		req, back := parseMessagesRequest(r, d)
		if back != "" {
			handBack(w, r, d, ann, back)
			return
		}

		body, status, mxErr, back := roomMessages(r.Context(), d, verdict, roomID, req)
		switch {
		case back != "":
			handBack(w, r, d, ann, back)
		case mxErr != nil:
			refuse(w, ann, status, *mxErr)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		}
	})
}

// messagesRequest is PaginationConfig and the servlet's own parameters.
type messagesRequest struct {
	from, to  *streamtoken.Token
	backwards bool
	limit     int
	// filter is nil when there is none (an absent or falsy `filter`).
	filter *filter.Filter
	raw    bool
}

var plainInt = regexp.MustCompile(`^[0-9]+$`)

// parseMessagesRequest reads the query as PaginationConfig.from_request and
// the servlet do, or names why it is Synapse's to answer.
func parseMessagesRequest(r *http.Request, d Deps) (messagesRequest, string) {
	req := messagesRequest{limit: defaultPaginationLimit}
	// Twisted and Go split a query differently on ';'.
	if strings.Contains(r.URL.RawQuery, ";") {
		return req, "query"
	}
	q := r.URL.Query()

	// parse_enum(request, "dir", Direction, default=FORWARDS).
	if v, present := q["dir"]; present {
		switch v[0] {
		case "b":
			req.backwards = true
		case "f":
		default:
			return req, "dir"
		}
	}

	parseTok := func(s string) (*streamtoken.Token, bool) {
		tok, err := streamtoken.Parse(s)
		if err != nil || hasInstanceMap(tok) {
			return nil, false
		}
		return &tok, true
	}
	// "END" is a from of None, for backwards compatibility; so is "".
	if s := q.Get("from"); s != "" && s != "END" {
		tok, ok := parseTok(s)
		if !ok {
			return req, "from"
		}
		req.from = tok
	}
	if s := q.Get("to"); s != "" {
		tok, ok := parseTok(s)
		if !ok {
			return req, "to"
		}
		req.to = tok
	}

	if v, present := q["limit"]; present {
		if !plainInt.MatchString(v[0]) {
			return req, "limit"
		}
		n, err := strconv.Atoi(v[0])
		if err != nil {
			return req, "limit"
		}
		req.limit = min(n, messagesMaxLimit)
	}

	// parse_json(request, "filter"); Filter(hs, filter_json) if truthy.
	if v, present := q["filter"]; present {
		doc := strings.TrimSpace(v[0])
		switch {
		case doc == "null" || doc == "{}":
		case strings.HasPrefix(doc, "{") && gjson.Valid(doc):
			f, format, err := filter.ParseRoomEventFilter([]byte(doc), d.filterFeatures())
			if err != nil {
				return req, "filter"
			}
			// Both tables in one query name relation_type: PostgreSQL
			// refuses it, and Synapse's 500 is not ours to copy.
			if len(f.RelatedByRelTypes) > 0 && (len(f.RelTypes) > 0 || len(f.NotRelTypes) > 0) {
				return req, "filter"
			}
			req.filter = &f
			if format == "federation" {
				req.raw = true
			}
		default:
			return req, "filter"
		}
	}
	if _, present := q["raw"]; present {
		req.raw = true
	}
	return req, ""
}

// hasInstanceMap reports a token any of whose streams lists writers.
func hasInstanceMap(t streamtoken.Token) bool {
	return len(t.Room.Instances) > 0 || len(t.Receipt.Instances) > 0 ||
		len(t.DeviceList.Instances) > 0 || len(t.QuarantinedMedia.Instances) > 0
}

// pageFilter is the filter as filter_to_clause reads it.
func pageFilter(f *filter.Filter) *store.PageFilter {
	if f == nil {
		return nil
	}
	return &store.PageFilter{
		Types: f.Types, NotTypes: f.NotTypes,
		Senders: f.Senders, NotSenders: f.NotSenders,
		Rooms: f.Rooms, NotRooms: f.NotRooms,
		ContainsURL:       f.ContainsURL != nil && *f.ContainsURL,
		Labels:            f.Labels,
		RelatedBySenders:  f.RelatedBySenders,
		RelatedByRelTypes: f.RelatedByRelTypes,
		RelTypes:          f.RelTypes, NotRelTypes: f.NotRelTypes,
	}
}

func roomMessages(ctx context.Context, d Deps, verdict clientauth.Verdict, roomID string,
	req messagesRequest) (body []byte, status int, mxErr *matrixerr.Error, back string) {

	ctx = store.WithRequestCache(ctx)
	fail := func(what string, err error) ([]byte, int, *matrixerr.Error, string) {
		return nil, http.StatusInternalServerError, internalError(d, what, err), ""
	}

	partial, err := d.Store.IsPartialStateRoom(ctx, roomID)
	if err != nil {
		return fail("partial state", err)
	}
	if partial {
		return nil, 0, nil, "partial_state"
	}

	// check_user_in_room_or_world_readable(allow_departed_users=True).
	if !isMine(verdict.UserID, d.ServerName) {
		return nil, 0, nil, "non_local_user"
	}
	m, err := d.Store.LocalMembership(ctx, roomID, verdict.UserID)
	if err != nil {
		return fail("membership", err)
	}
	inRoom := m.Membership == "join"
	if m.Membership == "leave" {
		forgot, err := d.Store.DidForget(ctx, verdict.UserID, roomID)
		if err != nil {
			return fail("forgotten", err)
		}
		inRoom = !forgot
	}
	membership, memberEventID := m.Membership, m.EventID
	if !inRoom {
		vis, err := d.Store.HistoryVisibility(ctx, roomID)
		if err != nil {
			return fail("history visibility", err)
		}
		if vis != "world_readable" {
			return nil, http.StatusForbidden, &matrixerr.Error{ErrCode: matrixerr.CodeForbidden,
				Error: fmt.Sprintf("User %s not in room %s, and room previews are disabled",
					verdict.UserID, roomID)}, ""
		}
		membership, memberEventID = "join", ""
	} else if membership != "join" {
		// A user who left a world-readable server announcement room reads
		// it as one who never joined.
		announce, err := announcementRooms(ctx, d, verdict.UserID)
		if err != nil {
			return fail("announcement rooms", err)
		}
		if announce.all[roomID] {
			vis, err := d.Store.HistoryVisibility(ctx, roomID)
			if err != nil {
				return fail("history visibility", err)
			}
			if vis == "world_readable" {
				membership, memberEventID = "join", ""
			}
		}
	}

	info, err := d.Store.RoomInfo(ctx, roomID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, 0, nil, "room_version"
	}
	if err != nil {
		return fail("room info", err)
	}
	if !clientevent.IsKnownRoomVersion(info.RoomVersion) {
		return nil, 0, nil, "room_version"
	}

	// The from token: the client's, the start for forwards, the room's
	// current position for backwards.
	var from streamtoken.Token
	switch {
	case req.from != nil:
		from = *req.from
	case !req.backwards:
		from = streamtoken.Token{Room: streamtoken.Live(0)} // StreamToken.START
	default:
		cur, err := currentToken(ctx, d)
		if err != nil {
			return fail("current token", err)
		}
		topo, err := d.Store.MaxTopological(ctx, roomID)
		if err != nil {
			return fail("max topological", err)
		}
		from = streamtoken.Token{Room: streamtoken.Historical(topo, cur.Room.Stream)}
	}

	// Backfill is ours when write mode can do it and has not lately left
	// this room's to Synapse; otherwise a page that would backfill is
	// Synapse's whole.
	bf := d.Backfill.get()
	if bf != nil && d.Backfill.handedBack(roomID) {
		bf = nil
	}
	var currTopo int64
	if req.backwards {
		// The depth backfill is decided from, before any clamp.
		currTopo = from.Room.Topological
		if !from.Room.IsHistorical() || currTopo == 0 {
			currTopo, err = d.Store.CurrentTopological(ctx, roomID, from.Room.Stream)
			if err != nil {
				return fail("current topological", err)
			}
		}

		// A user who left reads no further than their leave.
		if membership == "leave" {
			leave, err := d.Store.TopologicalTokenForEvent(ctx, memberEventID)
			if errors.Is(err, store.ErrNoEvent) {
				return nil, 0, nil, "leave_event"
			}
			if err != nil {
				return fail("leave token", err)
			}
			if leave.Topological < currTopo {
				from = from.WithRoomKey(leave)
			}
		}

		if bf == nil {
			would, err := wouldBackfill(ctx, d, roomID, info.RoomVersion, currTopo, req.limit)
			if err != nil {
				return fail("backfill points", err)
			}
			if would {
				return nil, 0, nil, "backfill"
			}
		}
	}

	var toKey *streamtoken.RoomKey
	if req.to != nil {
		k := req.to.Room
		toKey = &k
	}
	paginate := func() ([]store.PageEvent, streamtoken.RoomKey, bool, error) {
		return d.Store.PaginateTopological(ctx, roomID, info.RoomVersion,
			from.Room, toKey, req.backwards, req.limit, pageFilter(req.filter))
	}
	page, nextKey, limited, err := paginate()
	if err != nil {
		return fail("paginate", err)
	}

	if req.backwards && bf != nil {
		depths := make([]int64, len(page))
		for i, ev := range page {
			depths[i] = ev.Depth
		}
		if backfillInForeground(depths, req.limit) {
			did, err := bf.MaybeBackfill(ctx, roomID, currTopo, req.limit)
			if err != nil {
				// Left to Synapse (or failed here): Synapse backfills and
				// answers, and has this room's backfills for a while.
				d.Backfill.handBack(roomID)
				if !errors.Is(err, ErrBackfillHandBack) {
					d.Log.Warn().Err(err).Str("room_id", roomID).Msg("backfill failed here; the request is handed back")
				}
				return nil, 0, nil, "backfill"
			}
			if did {
				if page, nextKey, limited, err = paginate(); err != nil {
					return fail("paginate", err)
				}
			}
		} else {
			// maybe_backfill_in_the_background.
			go func() {
				_, err := bf.MaybeBackfill(context.WithoutCancel(ctx), roomID, currTopo, req.limit)
				if err != nil {
					d.Backfill.handBack(roomID)
					if !errors.Is(err, ErrBackfillHandBack) {
						d.Log.Warn().Err(err).Str("room_id", roomID).Msg("background backfill failed here; the room's next ones are handed back")
					}
				}
			}()
		}
	}
	next := from.WithRoomKey(nextKey)

	if !limited && len(page) == 0 {
		return messagesResponse(nil, from, nil, nil)
	}

	events := make([]store.TimelineEvent, len(page))
	for i, ev := range page {
		events[i] = ev.TimelineEvent
	}
	// get_events_as_list(get_prev_content=True), at load.
	if err := withPrevContent(ctx, d, events); err != nil {
		return fail("prev content", err)
	}

	// event_filter.filter: the filter's own check, then its relations.
	if req.filter != nil {
		kept := events[:0:0]
		for _, ev := range events {
			if req.filter.Check(timelineFilterEvent(roomID, ev)) {
				kept = append(kept, ev)
			}
		}
		events, err = applyRelationFilter(ctx, d, req.filter, kept)
		if err != nil {
			return fail("relations", err)
		}
	}

	timeNow := time.Now().UnixMilli()
	res, err := eventfilter.ForClient(ctx, d.Store, roomID, verdict.UserID, events,
		memberEventID == "", timeNow, d.MSC4354Enabled, nil)
	if err != nil {
		return fail("visibility", err)
	}
	if len(res.Pruned) > 0 {
		return nil, 0, nil, "erased_sender"
	}
	if len(res.Events) == 0 {
		return messagesResponse(nil, from, &next, nil)
	}
	visible := res.Events

	admin, err := d.Store.IsServerAdmin(ctx, verdict.UserID)
	if err != nil {
		return fail("server admin", err)
	}
	format := clientevent.FormatV1
	if req.raw {
		format = clientevent.FormatRaw
	}
	cfg := clientevent.Config{
		Format: format,
		Requester: clientevent.Requester{UserID: verdict.UserID, DeviceID: verdict.DeviceID,
			TokenID: verdict.TokenID, IsGuest: verdict.IsGuest, AppServiceID: verdict.AppServiceID},
		MSC4354Enabled:       d.MSC4354Enabled,
		IncludeAdminMetadata: admin,
	}

	// The members of the page's senders, at its first event.
	var state []store.StateEvent
	stateBlock := false
	if req.filter != nil && req.filter.LazyLoadMembers {
		seen := map[string]bool{}
		var senders []string
		for _, ev := range visible {
			if !seen[ev.Sender] {
				seen[ev.Sender] = true
				senders = append(senders, ev.Sender)
			}
		}
		state, stateBlock, err = d.Store.MemberStateAtEvent(ctx, visible[0].EventID, info.RoomVersion, senders)
		if errors.Is(err, store.ErrNoStateGroup) {
			return nil, 0, nil, "no_state_group"
		}
		if err != nil {
			return fail("lazy members", err)
		}
	}

	ids := timelineEventIDs(visible)
	for _, ev := range state {
		ids = append(ids, ev.EventID)
	}
	redactions, err := d.Store.Redactions(ctx, ids)
	if err != nil {
		return fail("redactions", err)
	}

	// get_bundled_aggregations, always.
	vis, err := d.Store.VisibilityExtras(ctx, roomID, verdict.UserID, nil)
	if err != nil {
		return fail("ignored users", err)
	}
	aggs, nestedIDs, err := bundleAggregations(ctx, d, verdict.UserID, visible, vis.IgnoredSenders)
	if err != nil {
		return fail("aggregations", err)
	}
	var nested map[string]store.StateEvent
	if len(aggs) > 0 {
		want := append([]string(nil), nestedIDs...)
		for _, a := range aggs {
			if a.ReplaceID() != "" {
				want = append(want, a.ReplaceID())
			}
		}
		nested, err = d.Store.EventsByID(ctx, want, info.RoomVersion)
		if err != nil {
			return fail("aggregated events", err)
		}
	}

	chunk := make([]json.RawMessage, 0, len(visible))
	for i, ev := range visible {
		stored := ev.Stored
		stored.Membership = res.Memberships[i]
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return fail("redaction", err)
		}
		b, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return fail("serialise", err)
		}
		if agg, ok := aggs[ev.EventID]; ok {
			if b, err = attachAggregations(b, agg, aggs, nested, timeNow, cfg); err != nil {
				return fail("aggregations", err)
			}
		}
		chunk = append(chunk, b)
	}

	var stateJSON []json.RawMessage
	if stateBlock {
		stateJSON = make([]json.RawMessage, 0, len(state))
		for _, ev := range state {
			// FilteredEvent.state: no membership. What Synapse's cached copy
			// carries (prevCache).
			stored := ev.Stored
			d.Store.AttachCachedPrevContent(&stored)
			if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
				return fail("redaction", err)
			}
			b, err := clientevent.Serialize(stored, timeNow, cfg)
			if err != nil {
				return fail("serialise state", err)
			}
			stateJSON = append(stateJSON, b)
		}
	}
	return messagesResponse(chunk, from, &next, stateJSON)
}

// wouldBackfill is whether maybe_backfill(room, currentDepth, limit) would
// do more than read the database: wait on a purge, try backfill points
// near the page, or start the background attempt at MAX_DEPTH. It stops
// where _maybe_backfill_inner starts checking each point's visibility,
// which is Synapse's to decide along with the rest.
func wouldBackfill(ctx context.Context, d Deps, roomID, roomVersion string, currentDepth int64,
	limit int) (bool, error) {
	if clientevent.LookupRoomVersion(roomVersion).MSC4242StateDags {
		return false, nil // not backfilled in Synapse
	}
	now := time.Now().UnixMilli()
	locked, err := d.Store.PurgeLocked(ctx, roomID, now)
	if err != nil || locked {
		return locked, err
	}
	points, err := d.Store.BackfillPoints(ctx, roomID, currentDepth+numberOfEventsToBackfill, now, 50)
	if err != nil {
		return false, err
	}
	if len(points) == 0 {
		if currentDepth == maxDepth {
			return false, nil
		}
		later, err := d.Store.BackfillPoints(ctx, roomID, maxDepth, now, 1)
		return len(later) > 0, err
	}
	// Closest first, those in the past before those after; sorted is
	// stable, so the query's order breaks ties.
	sort.SliceStable(points, func(i, j int) bool {
		di, dj := absInt64(currentDepth-points[i].Depth), absInt64(currentDepth-points[j].Depth)
		if di != dj {
			return di < dj
		}
		return pastFirst(currentDepth, points[i].Depth) < pastFirst(currentDepth, points[j].Depth)
	})
	return currentDepth-2*int64(limit) <= points[0].Depth, nil
}

func absInt64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

func pastFirst(current, depth int64) int {
	if current >= depth {
		return 0
	}
	return 1
}

// messagesResponse is encode_messages_response, keys in canonical order:
// chunk, end (when there is one), start, state (when there is one).
func messagesResponse(chunk []json.RawMessage, start streamtoken.Token, end *streamtoken.Token,
	state []json.RawMessage) ([]byte, int, *matrixerr.Error, string) {
	var b bytes.Buffer
	writeList := func(evs []json.RawMessage) {
		b.WriteByte('[')
		for i, ev := range evs {
			if i > 0 {
				b.WriteByte(',')
			}
			b.Write(ev)
		}
		b.WriteByte(']')
	}
	writeStr := func(s string) {
		enc, _ := json.Marshal(s)
		b.Write(enc)
	}
	b.WriteString(`{"chunk":`)
	writeList(chunk)
	if end != nil {
		b.WriteString(`,"end":`)
		writeStr(end.String())
	}
	b.WriteString(`,"start":`)
	writeStr(start.String())
	if state != nil {
		b.WriteString(`,"state":`)
		writeList(state)
	}
	b.WriteByte('}')
	return b.Bytes(), http.StatusOK, nil, ""
}
