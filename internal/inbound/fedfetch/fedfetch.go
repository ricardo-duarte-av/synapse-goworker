// Package fedfetch pulls events from remote servers for write mode, as
// Synapse's FederationClient does (federation/federation_client.py):
// /get_missing_events, /state_ids, /event, /state and /event_auth, each pulled PDU
// checked like _check_sigs_and_hash_for_pulled_events_and_fetch checks it.
//
// Where Synapse's handling depends on what we do not reproduce -- a key it
// would fetch, a bad signature it would try to fetch a good copy of, a
// content hash it would redact on -- the pull fails with ErrUncertain, and
// the event it was for is left to Synapse's worker.
package fedfetch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/federation"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/backoff"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/keyring"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/pduverify"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/fedclient"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdustore"
)

// ErrUncertain is a pull whose outcome Synapse would decide in a way we do
// not reproduce.
var ErrUncertain = errors.New("fedfetch: not reproducible")

// Pulled is a checked PDU from a remote server: Raw is the copy Synapse
// would hold (event_from_pdu_json, with any policy server signature added),
// Metadata its internal metadata.
type Pulled struct {
	ID       string
	Raw      []byte
	Event    *pdu.Event
	Metadata map[string]any
}

// Client pulls events.
type Client struct {
	fed    *federation.Client
	st     *pdustore.Store
	v      *pduverify.Verifier
	policy pduverify.PolicyAsker
	server string
	keys   *keyring.Keyring
	// OnFailure records a failed pull attempt (record_failure_callback):
	// a pulled event whose signature or content hash failed.
	OnFailure func(ctx context.Context, roomID, eventID, cause string)
	// Backoff is the destination retry limiter around every request; nil
	// for none.
	Backoff *backoff.Limiter

	// get_pdu's per-process state: when each destination was last asked
	// for each event (pdu_destination_tried), and the events it returned
	// (_get_pdu_cache).
	pduMu    sync.Mutex
	pduTried map[string]map[string]time.Time
	pduCache map[string]cachedPDU
}

type cachedPDU struct {
	p  *Pulled
	at time.Time
}

// get_pdu's limits: a destination is asked for an event at most once a
// minute (PDU_RETRY_TIME_MS), and a fetched event is reused for two
// minutes, at most 1000 of them.
const (
	pduRetryTime   = time.Minute
	pduCacheExpiry = 2 * time.Minute
	pduCacheMax    = 1000
)

// How check treats a bad signature: in a batch, look for a good copy
// (_check_sigs_and_hash_and_fetch_one); for one event, fail (get_pdu).
type checkMode int

const (
	batch checkMode = iota
	single
)

// New builds a client over a signed federation client. keys, when not nil,
// finds signing keys Synapse does not hold yet.
func New(fed *federation.Client, st *pdustore.Store, v *pduverify.Verifier, policy pduverify.PolicyAsker, server string, keys *keyring.Keyring) *Client {
	return &Client{fed: fed, st: st, v: v, policy: policy, server: server, keys: keys}
}

// do is MatrixFederationHttpClient's request with Synapse's timeout:
// timeout for one attempt, or, when Synapse's caller gives none (zero), the
// default with its retries.
func (c *Client) do(ctx context.Context, timeout time.Duration, server, method string, path []any, query url.Values, body, out any) error {
	send := func(ctx context.Context) (*http.Response, error) {
		return c.send(ctx, server, method, path, query, body, out)
	}
	// The limiter is around the whole retry loop, as in _send_request.
	return c.Backoff.Do(ctx, server, backoff.Options{}, func() (*http.Response, error) {
		if timeout == 0 {
			return retrying(ctx, send)
		}
		return attempt(ctx, timeout, send)
	})
}

func (c *Client) send(ctx context.Context, server, method string, path []any, query url.Values, body, out any) (*http.Response, error) {
	_, resp, err := c.fed.MakeFullRequest(ctx, federation.RequestParams{
		ServerName:   server,
		Method:       method,
		Path:         federation.URLPath(path),
		Query:        query,
		Authenticate: true,
		RequestJSON:  body,
		ResponseJSON: out,
	})
	return resp, err
}

// retrying is fedclient.Retrying; attempt is fedclient.Attempt.
func retrying(ctx context.Context, send func(context.Context) (*http.Response, error)) (*http.Response, error) {
	return fedclient.Retrying(ctx, send)
}

func attempt(ctx context.Context, timeout time.Duration, send func(context.Context) (*http.Response, error)) (*http.Response, error) {
	return fedclient.Attempt(ctx, timeout, send)
}

// MissingEvents is get_missing_events: up to limit events between earliest
// and latest, from origin, checked. Synapse asks with a 60s timeout.
func (c *Client) MissingEvents(ctx context.Context, origin, roomID string, rv *pdu.RoomVersion, earliest, latest []string, limit, minDepth int) ([]*Pulled, error) {
	var resp struct {
		Events []json.RawMessage `json:"events"`
	}
	body := map[string]any{"limit": limit, "min_depth": minDepth, "earliest_events": earliest, "latest_events": latest}
	if err := c.do(ctx, 60*time.Second, origin, http.MethodPost, []any{"v1", "get_missing_events", roomID}, nil, body, &resp); err != nil {
		return nil, err
	}
	return c.checkAll(ctx, resp.Events, rv, roomID, origin)
}

// StateIDs is get_room_state_ids: the state at eventID and its auth chain.
func (c *Client) StateIDs(ctx context.Context, dest, roomID, eventID string) (state, auth []string, err error) {
	var resp struct {
		PDUIDs  json.RawMessage `json:"pdu_ids"`
		AuthIDs json.RawMessage `json:"auth_chain_ids"`
	}
	q := url.Values{"event_id": {eventID}}
	// get_room_state_ids: no timeout.
	if err := c.do(ctx, 0, dest, http.MethodGet, []any{"v1", "state_ids", roomID}, q, nil, &resp); err != nil {
		return nil, nil, err
	}
	// "invalid response from /state_ids" unless both are lists; a list
	// of anything but strings we leave to Synapse.
	if err := json.Unmarshal(resp.PDUIDs, &state); err != nil {
		return nil, nil, fmt.Errorf("%w: /state_ids pdu_ids: %v", ErrUncertain, err)
	}
	if len(resp.AuthIDs) > 0 && string(resp.AuthIDs) != "null" {
		if err := json.Unmarshal(resp.AuthIDs, &auth); err != nil {
			return nil, nil, fmt.Errorf("%w: /state_ids auth_chain_ids: %v", ErrUncertain, err)
		}
	}
	return state, auth, nil
}

// Event is get_pdu from one destination, without a timeout as
// _get_events_and_persist asks: the event, checked, or nil when the server
// does not have it.
func (c *Client) Event(ctx context.Context, dest, eventID, roomID string, rv *pdu.RoomVersion) (*Pulled, error) {
	return c.event(ctx, 0, dest, eventID, roomID, rv)
}

// event is get_pdu from one destination with its caller's timeout (zero:
// none): a cached copy, else nothing if the destination was asked for it
// less than a minute ago, else the destination's copy, checked. A
// destination in backoff is skipped without counting as asked.
func (c *Client) event(ctx context.Context, timeout time.Duration, dest, eventID, roomID string, rv *pdu.RoomVersion) (*Pulled, error) {
	now := time.Now()
	c.pduMu.Lock()
	if e, ok := c.pduCache[eventID]; ok && now.Sub(e.at) < pduCacheExpiry {
		c.pduMu.Unlock()
		return e.p, nil
	}
	if at, ok := c.pduTried[eventID][dest]; ok && now.Sub(at) < pduRetryTime {
		c.pduMu.Unlock()
		return nil, nil
	}
	c.pduMu.Unlock()
	p, err := c.eventFrom(ctx, timeout, dest, eventID, roomID, rv)
	var nr *backoff.NotRetrying
	if errors.As(err, &nr) || errors.Is(err, ErrUncertain) || ctx.Err() != nil {
		return p, err
	}
	c.pduMu.Lock()
	defer c.pduMu.Unlock()
	if c.pduTried == nil {
		c.pduTried, c.pduCache = map[string]map[string]time.Time{}, map[string]cachedPDU{}
	}
	if c.pduTried[eventID] == nil {
		c.pduTried[eventID] = map[string]time.Time{}
	}
	c.pduTried[eventID][dest] = now
	if p != nil {
		c.prunePDUs(now)
		c.pduCache[eventID] = cachedPDU{p: p, at: now}
	}
	return p, err
}

// prunePDUs drops what has expired, as the minute's _clear_tried_cache and
// the ExpiringCache do, and makes room for one more cached event.
func (c *Client) prunePDUs(now time.Time) {
	for id, dests := range c.pduTried {
		for d, at := range dests {
			if now.Sub(at) >= pduRetryTime {
				delete(dests, d)
			}
		}
		if len(dests) == 0 {
			delete(c.pduTried, id)
		}
	}
	var oldest string
	for id, e := range c.pduCache {
		if now.Sub(e.at) >= pduCacheExpiry {
			delete(c.pduCache, id)
		} else if oldest == "" || e.at.Before(c.pduCache[oldest].at) {
			oldest = id
		}
	}
	if len(c.pduCache) >= pduCacheMax {
		delete(c.pduCache, oldest)
	}
}

// eventFrom is get_pdu_from_destination_raw.
func (c *Client) eventFrom(ctx context.Context, timeout time.Duration, dest, eventID, roomID string, rv *pdu.RoomVersion) (*Pulled, error) {
	var resp struct {
		PDUs []json.RawMessage `json:"pdus"`
	}
	if err := c.do(ctx, timeout, dest, http.MethodGet, []any{"v1", "event", eventID}, nil, nil, &resp); err != nil {
		return nil, err
	}
	if len(resp.PDUs) == 0 {
		return nil, nil
	}
	p, err := c.check(ctx, resp.PDUs[0], rv, roomID, dest, single)
	if err != nil || p == nil {
		return nil, err
	}
	if p.ID != eventID {
		return nil, fmt.Errorf("%w: asked for %s, got %s", ErrUncertain, eventID, p.ID)
	}
	return p, nil
}

// State is get_room_state: the complete state at eventID and its auth chain,
// checked.
func (c *Client) State(ctx context.Context, dest, roomID, eventID string, rv *pdu.RoomVersion) (auth, state []*Pulled, err error) {
	var resp struct {
		AuthChain []json.RawMessage `json:"auth_chain"`
		PDUs      []json.RawMessage `json:"pdus"`
	}
	q := url.Values{"event_id": {eventID}}
	// get_room_state: ten minutes, tried once (it gives a timeout).
	if err := c.do(ctx, 10*time.Minute, dest, http.MethodGet, []any{"v1", "state", roomID}, q, nil, &resp); err != nil {
		return nil, nil, err
	}
	// Two batches, the auth chain then the state, as get_room_state checks
	// them. The streaming parser builds them with make_event_from_dict:
	// none of event_from_pdu_json's checks, unsigned kept as received.
	asReceived := func(raws []json.RawMessage) ([]*parsedPDU, error) {
		out := make([]*parsedPDU, 0, len(raws))
		for _, raw := range raws {
			pp, err := parseReceived(raw, rv)
			if err != nil {
				return nil, err
			}
			out = append(out, pp)
		}
		return out, nil
	}
	pa, err := asReceived(resp.AuthChain)
	if err != nil {
		return nil, nil, err
	}
	ps, err := asReceived(resp.PDUs)
	if err != nil {
		return nil, nil, err
	}
	if auth, err = c.checkParsed(ctx, pa, rv, roomID, dest); err != nil {
		return nil, nil, err
	}
	if state, err = c.checkParsed(ctx, ps, rv, roomID, dest); err != nil {
		return nil, nil, err
	}
	return auth, state, nil
}

// EventAuth is get_event_auth: the auth chain of eventID, checked as
// _check_sigs_and_hash_for_pulled_events_and_fetch checks it.
func (c *Client) EventAuth(ctx context.Context, dest, roomID, eventID string, rv *pdu.RoomVersion) ([]*Pulled, error) {
	var resp struct {
		AuthChain []json.RawMessage `json:"auth_chain"`
	}
	// get_event_auth: no timeout.
	if err := c.do(ctx, 0, dest, http.MethodGet, []any{"v1", "event_auth", roomID, eventID}, nil, nil, &resp); err != nil {
		return nil, err
	}
	return c.checkAll(ctx, resp.AuthChain, rv, roomID, dest)
}

// check is event_from_pdu_json, then _check_sigs_and_hash_and_fetch_one for
// the outcomes we reproduce: valid signatures and hash, and the policy
// server's verdict. A PDU event_from_pdu_json rejects is dropped (nil).
func (c *Client) check(ctx context.Context, raw []byte, rv *pdu.RoomVersion, roomID, origin string, mode checkMode) (*Pulled, error) {
	pp, err := parsePulled(raw, rv)
	if err != nil || pp == nil {
		return nil, err
	}
	return c.verify(ctx, pp, rv, roomID, origin, mode, c.lookup(ctx))
}

// checkAll is _check_sigs_and_hash_for_pulled_events_and_fetch over a
// list: every PDU parsed, the keys all their signatures need fetched as
// the one batch Synapse's concurrent checks make, then each checked in
// order. What is dropped is left out; the first error ends it.
func (c *Client) checkAll(ctx context.Context, raws []json.RawMessage, rv *pdu.RoomVersion, roomID, origin string) ([]*Pulled, error) {
	parsed := make([]*parsedPDU, 0, len(raws))
	for _, raw := range raws {
		pp, err := parsePulled(raw, rv)
		if err != nil {
			return nil, err
		}
		if pp != nil {
			parsed = append(parsed, pp)
		}
	}
	return c.checkParsed(ctx, parsed, rv, roomID, origin)
}

// checkParsed is checkAll after parsing.
func (c *Client) checkParsed(ctx context.Context, parsed []*parsedPDU, rv *pdu.RoomVersion, roomID, origin string) ([]*Pulled, error) {
	lookup := c.lookup(ctx)
	if c.keys != nil && len(parsed) > 1 {
		var reqs []keyring.KeyRequest
		for _, pp := range parsed {
			r, err := pdu.SignatureKeyRequests(pp.ev, pp.raw)
			if err != nil {
				continue // the check below fails it the same way
			}
			reqs = append(reqs, r...)
		}
		lookup = c.keys.Batch(ctx, reqs)
	}
	out := make([]*Pulled, 0, len(parsed))
	for _, pp := range parsed {
		p, err := c.verify(ctx, pp, rv, roomID, origin, batch, lookup)
		if err != nil {
			return nil, err
		}
		if p != nil {
			out = append(out, p)
		}
	}
	return out, nil
}

// ErrBackfillFailed is a backfill from one server that Synapse gives up
// on with a SynapseError, an InvalidResponseError or any other exception
// try_backfill logs before trying the next server.
var ErrBackfillFailed = errors.New("fedfetch: backfill failed")

// canonicalJSONMaxInt is CANONICALJSON_MAX_INT; MIN_INT is its negative.
const canonicalJSONMaxInt = 1<<53 - 1

// Backfill is FederationClient.backfill: GET /backfill/{roomId} with the
// extremities and the limit (get_json, retried, a 400 M_UNRECOGNIZED
// tried again with a trailing slash), the PDUs filtered for a valid depth
// and parsed (parse_events_from_pdu_json), then checked as one batch.
// Errors are the request's (an HTTP error keeps its status), or
// ErrBackfillFailed for what Synapse raises after the request.
func (c *Client) Backfill(ctx context.Context, dest, roomID string, rv *pdu.RoomVersion, extremities []string, limit int) ([]*Pulled, error) {
	if len(extremities) == 0 {
		return nil, nil
	}
	q := url.Values{"v": extremities, "limit": {strconv.Itoa(limit)}}
	var resp json.RawMessage
	path := []any{"v1", "backfill", roomID}
	err := c.do(ctx, 0, dest, http.MethodGet, path, q, nil, &resp)
	var he mautrix.HTTPError
	if errors.As(err, &he) && he.Response != nil && he.Response.StatusCode == http.StatusBadRequest &&
		he.RespError != nil && he.RespError.ErrCode == "M_UNRECOGNIZED" {
		resp = nil
		err = c.do(ctx, 0, dest, http.MethodGet, append(path, ""), q, nil, &resp)
	}
	if err != nil {
		return nil, err
	}
	data := gjson.ParseBytes(resp)
	if !data.IsObject() {
		return nil, fmt.Errorf("%w: Backfill transaction_data is not a dict.", ErrBackfillFailed)
	}
	pdus := data.Get("pdus")
	if !pdus.IsArray() {
		return nil, fmt.Errorf("%w: transaction_data.pdus is not a list.", ErrBackfillFailed)
	}
	var parsed []*parsedPDU
	for _, item := range pdus.Array() {
		raw := []byte(item.Raw)
		// filter_pdus_for_valid_depth.
		depth := item.Get("depth")
		if !item.IsObject() || !depth.Exists() {
			if !item.IsObject() {
				return nil, fmt.Errorf("%w: a PDU is not an object", ErrUncertain)
			}
			continue
		}
		if depth.Type != gjson.Number {
			return nil, fmt.Errorf("%w: a PDU's depth is %s", ErrUncertain, depth.Raw)
		}
		if strings.ContainsAny(depth.Raw, ".eE") {
			return nil, fmt.Errorf("%w: a PDU's depth is %s", ErrUncertain, depth.Raw)
		}
		d, perr := strconv.ParseInt(depth.Raw, 10, 64)
		if perr != nil || d < -canonicalJSONMaxInt || d > canonicalJSONMaxInt {
			continue
		}
		// event_from_pdu_json raises, for the whole response.
		if err := pdu.CheckPDUJSON(raw, rv); errors.Is(err, pdu.ErrIgnoredPDU) {
			return nil, fmt.Errorf("%w: %v", ErrBackfillFailed, err)
		}
		pp, err := parsePulled(raw, rv)
		if err != nil {
			return nil, err
		}
		if pp != nil {
			parsed = append(parsed, pp)
		}
	}
	return c.checkParsed(ctx, parsed, rv, roomID, dest)
}

// lookup is the Keyring, or without one the keys Synapse holds.
func (c *Client) lookup(ctx context.Context) pdu.KeyFetch {
	if c.keys != nil {
		return c.keys.Fetch(ctx)
	}
	return func(server string, keyIDs []string, _ int64) (map[string]pdu.VerifyKey, error) {
		return c.st.VerifyKeys(ctx, server, keyIDs)
	}
}

// parsedPDU is a pulled PDU as event_from_pdu_json leaves it, or, with
// received set, as make_event_from_dict does.
type parsedPDU struct {
	raw      []byte
	id       string
	ev       *pdu.Event
	hashOK   bool
	received bool
}

// parseReceived is make_event_from_dict for a /state response's PDU.
func parseReceived(raw []byte, rv *pdu.RoomVersion) (*parsedPDU, error) {
	if !gjson.ParseBytes(raw).IsObject() {
		return nil, fmt.Errorf("%w: a /state PDU is not an object", ErrUncertain)
	}
	pp, err := parseEvent(raw, rv)
	if err != nil {
		return nil, err
	}
	pp.received = true
	return pp, nil
}

// parsePulled is event_from_pdu_json and the content hash: nil for a PDU
// it drops.
func parsePulled(raw []byte, rv *pdu.RoomVersion) (*parsedPDU, error) {
	if err := pdu.CheckPDUJSON(raw, rv); errors.Is(err, pdu.ErrIgnoredPDU) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUncertain, err)
	}
	return parseEvent(raw, rv)
}

// parseEvent is the event ID, the parse and the content hash.
func parseEvent(raw []byte, rv *pdu.RoomVersion) (*parsedPDU, error) {
	id := ""
	if rv.EventFormat == pdu.FormatV1V2 {
		var v struct {
			EventID string `json:"event_id"`
		}
		_ = json.Unmarshal(raw, &v)
		id = v.EventID
	} else {
		var err error
		if id, err = pdu.CalculateEventID(raw, rv); err != nil {
			return nil, fmt.Errorf("%w: event id: %v", ErrUncertain, err)
		}
	}
	ev, err := pdu.ParseEvent(raw, id, rv, false)
	if err != nil || id == "" {
		return nil, fmt.Errorf("%w: unparseable pulled event", ErrUncertain)
	}
	hashOK, err := pdu.CheckContentHash(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: pulled event %s: content hash: %v", ErrUncertain, id, err)
	}
	return &parsedPDU{raw: raw, id: id, ev: ev, hashOK: hashOK}, nil
}

// verify is the rest of _check_sigs_and_hash_and_fetch_one, with the keys
// from lookup.
func (c *Client) verify(ctx context.Context, pp *parsedPDU, rv *pdu.RoomVersion, roomID, origin string, mode checkMode, lookup pdu.KeyFetch) (*Pulled, error) {
	raw, id, ev, hashOK := pp.raw, pp.id, pp.ev, pp.hashOK
	if err := pdu.CheckSignaturesWith(ev, raw, lookup); err != nil {
		return c.badSignature(ctx, err, ev, id, rv, roomID, origin, mode)
	}
	p := &Pulled{ID: id, Event: ev, Metadata: map[string]any{}}
	if !hashOK {
		// The pruned copy kept, no policy server asked; recorded as
		// "Event content has been tampered with" only when pruning changed
		// which keys the event or its content has: one with the same keys
		// "seems to have been redacted".
		pulled, err := pduverify.PulledEvent(raw, "", nil)
		if err == nil {
			p.Raw, err = pdu.Redact(pulled, rv)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrUncertain, err)
		}
		if !sameKeys(raw, p.Raw) {
			c.failed(ctx, ev.RoomID, id, "Event content has been tampered with")
		}
		p.Metadata["redacted"] = true
		return p, nil
	}
	v, err := c.v.Policy(ctx, raw, ev, c.server)
	if err != nil {
		return nil, err
	}
	if v.Skip != "" || v.Raise != "" {
		return nil, fmt.Errorf("%w: policy: %s%s", ErrUncertain, v.Skip, v.Raise)
	}
	var sigs map[string]string
	if v.Verdict == pduverify.PolicyAsk {
		if c.policy == nil {
			return nil, fmt.Errorf("%w: policy server not asked", ErrUncertain)
		}
		a := c.v.AskPolicy(ctx, c.policy, raw, time.Now(), rv, v)
		if a.Sigs == nil {
			p.Metadata["soft_failed"] = true
			p.Metadata["policy_server_spammy"] = true
		} else {
			sigs = a.Sigs
		}
	}
	// No request time: any age is dropped; a /state copy keeps unsigned.
	if pp.received {
		p.Raw, err = pduverify.ReceivedEvent(raw, v.Server, sigs)
	} else {
		p.Raw, err = pduverify.PulledEvent(raw, v.Server, sigs)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUncertain, err)
	}
	return p, nil
}

// sameKeys is _check_sigs_and_hash's test of a redacted copy: the event's
// keys (unsigned aside, as EventBase holds it apart) and its content's are
// those of the pruned event.
func sameKeys(raw, pruned []byte) bool {
	keySet := func(v gjson.Result, topLevel bool) map[string]bool {
		out := map[string]bool{}
		v.ForEach(func(k, _ gjson.Result) bool {
			if !topLevel || k.Str != "unsigned" {
				out[k.Str] = true
			}
			return true
		})
		return out
	}
	eq := func(a, b map[string]bool) bool {
		if len(a) != len(b) {
			return false
		}
		for k := range a {
			if !b[k] {
				return false
			}
		}
		return true
	}
	r, p := gjson.ParseBytes(raw), gjson.ParseBytes(pruned)
	return eq(keySet(r, true), keySet(p, true)) &&
		eq(keySet(r.Get("content"), false), keySet(p.Get("content"), false))
}

func (c *Client) failed(ctx context.Context, roomID, eventID, cause string) {
	if c.OnFailure != nil {
		c.OnFailure(ctx, roomID, eventID, cause)
	}
}

// badSignature is _check_sigs_and_hash raising InvalidEventSignatureError:
// recorded as a failed pull, then for one event (get_pdu) an error, and in a
// batch a look for a good copy, ours or the sender's server's.
func (c *Client) badSignature(ctx context.Context, err error, ev *pdu.Event, id string, rv *pdu.RoomVersion, roomID, origin string, mode checkMode) (*Pulled, error) {
	var se *pdu.SignatureError
	if !errors.As(err, &se) || se.Kind == pdu.SigNoKey && c.keys == nil {
		// A lookup that failed, or one that did not look as far as
		// Synapse's Keyring: a key it might yet find.
		return nil, fmt.Errorf("%w: pulled event %s: %v", ErrUncertain, id, err)
	}
	text, ok := se.SynapseText()
	if !ok {
		return nil, fmt.Errorf("%w: pulled event %s: %v", ErrUncertain, id, err)
	}
	c.failed(ctx, ev.RoomID, id, text)
	if mode == single {
		return nil, fmt.Errorf("event id %s: %s", id, text)
	}
	held, _, err := c.st.GetStoredEvents(ctx, []string{id})
	if err != nil {
		return nil, err
	}
	if se := held[id]; se != nil {
		if se.Outlier {
			// Synapse would go on with its stored outlier copy, which
			// _process_pulled_event then refuses; not reproduced.
			return nil, fmt.Errorf("%w: pulled event %s held as an outlier with a bad signature", ErrUncertain, id)
		}
		// Held in the timeline: _process_pulled_events skips it.
		return nil, nil
	}
	sender := ev.Sender
	if i := strings.IndexByte(sender, ':'); i >= 0 {
		sender = sender[i+1:]
	}
	if sender == origin {
		// "Failed to find copy of ... with valid signature": dropped.
		return nil, nil
	}
	// get_pdu(timeout=10000): one attempt. The timeout is the request's;
	// checking what comes back is not bounded by it.
	p, err := c.event(ctx, 10*time.Second, sender, id, roomID, rv)
	if errors.Is(err, ErrUncertain) {
		return nil, err
	}
	if err != nil {
		return nil, nil
	}
	return p, nil
}
