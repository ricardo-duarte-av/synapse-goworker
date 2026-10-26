// Package replication follows Synapse's replication stream over Redis.
//
// This is what lets the worker answer without being told what time it is. Every
// other source of a stream position is either a lagging checkpoint
// (`stream_positions`) or an approximation from table maxima that is wrong for
// three streams and impossible for a fourth -- typing is never persisted at
// all. See docs/tokens.md.
//
// It is SUBSCRIBE-only. A real Synapse worker also PUBLISHes a `REPLICATE`
// command on connect to ask for current positions; this one deliberately does
// not, because publishing to the shared bus would make every other worker
// broadcast POSITION rows on our account. Positions are seeded from the
// database and corrected as traffic arrives.
package replication

import (
	"context"
	"github.com/tidwall/gjson"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/metrics"
)

// Config describes how to reach Redis.
type Config struct {
	Enabled bool
	// Address is a unix socket path, or host:port.
	Address string
	// Channel MUST equal Synapse's server_name: the channel is named after it.
	// Subscribing to the wrong channel raises no error and delivers nothing, so
	// the worker would simply never wake up and would look merely idle.
	Channel  string
	Password string
	DB       int
}

// Stream names carried on the replication channel that matter to a sync.
const (
	StreamEvents              = "events"
	StreamPresence            = "presence"
	StreamTyping              = "typing"
	StreamReceipts            = "receipts"
	StreamAccountData         = "account_data"
	StreamPushRules           = "push_rules"
	StreamToDevice            = "to_device"
	StreamDeviceLists         = "device_lists"
	StreamUnPartialStatedRoom = "un_partial_stated_room"
	StreamThreadSubscriptions = "thread_subscriptions"
	StreamStickyEvents        = "sticky_events"
	StreamQuarantinedMedia    = "quarantined_media"
	StreamProfileUpdates      = "profile_updates"
	// StreamCaches carries Synapse's own cache invalidations, as
	// [cache_func, keys, invalidation_ts]. It is the only signal that
	// something we cached as immutable has been DELETED -- a purged room, a
	// deleted room -- rather than merely changed.
	StreamCaches = "caches"
	// StreamPresenceFederation carries [destination, user_id]: presence to be
	// sent to a remote server. Federation sender business, not ours.
	StreamPresenceFederation = "presence_federation"
	// StreamFederation carries the federation sender's outbound queue. Named
	// only so it can be classified below; nothing here consumes it.
	StreamFederation = "federation"
)

// resettableStreams are the streams whose position may legitimately go DOWN,
// and what each one's serial actually is.
//
// This is Synapse's own classification, read off `can_discard_position`
// (replication/tcp/streams/_base.py). A stream class that overrides it with
// "these streams can't go backwards" is backed by a database sequence and
// survives any restart; the three that do NOT override it keep their serial in
// a writer's memory, where a restart puts it back to zero. Synapse names the
// same set in `_process_position`: "to handle the case where the stream gets
// reset (e.g. for `caches` and `typing` after the writer's restart)" -- caches
// has since become database-backed, typing has not.
//
// For a stream in this map a lower position is news about a writer that
// restarted: the position is adopted as given and anything derived from the
// old serials is dropped, because those serials now name nothing. For every
// other stream a lower position is clamped away as it always was, in silence
// -- moving one backwards would ask a client to replay, and a writer
// announcing one is ordinary traffic rather than a fault. See advance.
//
// Adopting the position wholesale is safe for the one that matters: typing has
// a single writer by construction (Synapse refuses more than one entry in
// stream_writers.typing). The other two feed nothing here -- no token field,
// no derived state -- so their aggregate position is never read, and they are
// in this map to keep a federation sender's restart from being reported as
// something it is not.
var resettableStreams = map[string]string{
	StreamTyping:             "the typing handler's in-memory serial",
	StreamFederation:         "the federation sender's in-memory queue",
	StreamPresenceFederation: "the presence federation queue",
}

// silentStreams never wake a sync, with the reason each is here.
//
// Everything else keeps the conservative default -- a row naming nobody wakes
// everybody -- because an over-wake costs one recomputation and an under-wake
// costs a client its timeout. A stream only earns a place here on evidence:
// its position appears in no stream token, nothing it carries can change a
// response, and Synapse itself does not notify on it.
//
// Both entries were found by gosync_replication_rows_total{scope="global"}
// rather than by reading code, which is the argument for that metric existing.
var silentStreams = map[string]string{
	// Synapse invalidating its OWN caches: server keys, destination retry
	// timings, cross-signing keys. ReplicationDataHandler.on_rdata notifies
	// the notifier for an explicit list of streams and this is not among them.
	StreamCaches: "synapse's own cache invalidations",
	// The outbound copy of a presence change, addressed to a remote server.
	// The change itself arrives on the presence stream, which is targeted at
	// the user and does wake them; this row is the federation sender being
	// told to forward it. PresenceFederationQueue.process_replication_rows is
	// its only consumer and it returns immediately unless the worker is a
	// federation sender.
	StreamPresenceFederation: "outbound presence to remote servers",
}

// Listener is notified when a stream advances.
//
// roomIDs and userIDs name who the change concerns, so a waiting sync can be
// woken only if it cares. An empty pair means "everyone", which is the
// conservative answer when a row does not say.
type Listener interface {
	OnStreamAdvance(stream string, position int64, roomIDs, userIDs []string)
}

// Invalidator is told what each replication row makes stale.
//
// A second listener rather than an addition to Listener, because the two want
// opposite defaults. The notifier treats "no subjects" as "wake everybody",
// which is a harmless over-wake. An invalidator that treated it as "drop
// everything" would throw the caches away on every row it could not parse, and
// one that treated it as "drop nothing" would serve stale answers. It needs to
// make that choice itself, per stream.
type Invalidator interface {
	// OnRow is called for every row, in arrival order, before the position is
	// reported as applied.
	OnRow(stream string, pos int64, detail RowDetail)
	// OnRoomInvalidated says one room's derived data is stale.
	OnRoomInvalidated(roomID string)
	// OnPurge asks for everything to be dropped.
	OnPurge(reason string)
}

// Subscriber follows the replication channel.
type Subscriber struct {
	cfg         Config
	log         zerolog.Logger
	listener    Listener
	invalidator Invalidator

	// extra are listeners added after construction. Kept separate from
	// listener because that one is the notifier and the two run at different
	// moments: extra listeners RECORD a row before the position advances, and
	// the notifier WAKES clients after it. See record.
	extra []Listener

	mu sync.RWMutex
	// live is false whenever the subscription is not known to be healthy.
	// Positions and typing are only trustworthy while it is true.
	live      bool
	positions map[string]int64
	// typingSerial maps a room to the typing stream position at which it last
	// changed. /events needs it: unlike every other source, typing has no
	// table to ask "what changed since?", so the only record of when a room's
	// typists last moved is the one kept here as the rows arrive.
	typingSerial map[string]int64
	// typing maps a room to the users currently typing in it. Held only in
	// memory because that is the only place it exists anywhere: Synapse keeps
	// it in a counter on the typing worker and never writes it down.
	typing map[string][]string

	// lastByInstance is the last position seen from each writer of each
	// stream, as stream -> instance -> position.
	//
	// Separate from `positions`, which is one number per stream because that
	// is what a token wants. Whether a position went BACKWARDS can only be
	// judged per writer: a stream with several instances -- events, with two
	// persisters -- routinely reports a position below the maximum simply
	// because the other writer is ahead, and reading that as a reset would
	// cry restart on every second POSITION.
	//
	// Kept across reconnects on purpose. A writer that restarted while we were
	// disconnected is exactly the case worth catching, and the sample from
	// before the outage is the only evidence of it.
	lastByInstance map[string]map[string]int64

	// pending buffers the rows of a batch until the row that names the
	// batch's position arrives. See handleRDATA.
	pending map[string][]pendingRow

	// onDrop and onConnect run on the edges of the subscription's health. Set
	// by the caller rather than called directly so this package need not know
	// about the store.
	onDrop    func()
	onConnect func(positions map[string]int64)

	// abortSession ends the running session, so the Run loop reconnects.
	//
	// Something that goes wrong while reading the stream -- a batch that never
	// terminates is the one we have hit -- leaves the subscription unusable
	// but not closed: Redis is still happily delivering to a reader that can no
	// longer make sense of what it gets. Marking ourselves not-live is then a
	// statement with no way back, because only a NEW session ever marks us live
	// again. Cancelling the session is what turns that into a reconnect.
	abortSession context.CancelFunc
}

// SetInvalidator registers the cache invalidator.
func (s *Subscriber) SetInvalidator(inv Invalidator) { s.invalidator = inv }

// SetOnDrop registers a callback for the moment the subscription is lost.
//
// While we are following the stream, a purged or deleted room arrives as a
// `caches` invalidation we can act on. While we are not, rooms can be purged
// underneath us unseen -- so anything cached on the strength of "this row is
// immutable" has to be thrown away, because immutable is not the same as
// still there.
//
// Not called on the initial transition to live: there is nothing to purge
// before the first connection, and doing so would make startup log a discard
// of an empty cache.
func (s *Subscriber) SetOnDrop(f func()) { s.onDrop = f }

// SetOnConnect registers a callback for the moment the subscription becomes
// healthy, given the stream positions known at that instant.
//
// Those positions are the seed from the database plus anything already seen.
// A cache armed here starts empty, so claiming that everything up to them has
// been applied claims it of nothing -- and every entry added afterwards is
// read from a database that is at or beyond them.
func (s *Subscriber) SetOnConnect(f func(positions map[string]int64)) { s.onConnect = f }

// Positions returns a copy of every stream position currently known.
func (s *Subscriber) Positions() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int64, len(s.positions))
	for k, v := range s.positions {
		out[k] = v
	}
	return out
}

// New builds a Subscriber.
func New(cfg Config, log zerolog.Logger, listener Listener) *Subscriber {
	return &Subscriber{
		cfg:            cfg,
		log:            log,
		listener:       listener,
		positions:      map[string]int64{},
		typing:         map[string][]string{},
		typingSerial:   map[string]int64{},
		lastByInstance: map[string]map[string]int64{},
		pending:        map[string][]pendingRow{},
	}
}

// AddListener registers a further listener, told of each row BEFORE the
// stream's position advances (the notifier is told after).
//
// The stream-change caches are fed this way rather than by extending the
// notifier, because the two want opposite readings of the same row. A row that
// names nobody means "wake everyone" to the notifier -- a harmless over-wake --
// and means "we do not know what changed" to a cache, which is not harmless at
// all and has to be handled as a horizon reset. Each consumer decides for
// itself; see Invalidator for the same argument made once already.
//
// Not safe to call once Run has started.
func (s *Subscriber) AddListener(l Listener) {
	if l != nil {
		s.extra = append(s.extra, l)
	}
}

// record tells the added listeners -- the stream-change caches -- about a row.
//
// It runs BEFORE the position advances, because the position is what a now
// token is built from. In the other order a sync landing between the two gets a
// token that covers the row from a cache that has not heard of it, is told
// "nothing changed in this room since your since", and the change is lost for
// good: the client's next since is past it. This order costs nothing worse
// than a cache answering "changed" for a position nobody can ask about yet.
func (s *Subscriber) record(stream string, pos int64, roomIDs, userIDs []string) {
	for _, l := range s.extra {
		l.OnStreamAdvance(stream, pos, roomIDs, userIDs)
	}
}

// wake tells the notifier, AFTER the position has advanced, so that a woken
// sync computes a token that already covers the row.
func (s *Subscriber) wake(stream string, pos int64, roomIDs, userIDs []string) {
	if s.listener != nil {
		s.listener.OnStreamAdvance(stream, pos, roomIDs, userIDs)
	}
}

// Seed sets a starting position for a stream, from the database.
//
// Used at startup so the worker has an answer before any traffic arrives. A
// seeded position is a lower bound; the first RDATA for that stream replaces
// it with the truth.
func (s *Subscriber) Seed(positions map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for stream, pos := range positions {
		if pos > s.positions[stream] {
			s.positions[stream] = pos
		}
	}
}

// Position returns the last seen position of a stream.
func (s *Subscriber) Position(stream string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.positions[stream]
}

// Live reports whether the subscription is currently healthy.
//
// While it is false, typing is empty and positions may be stale. Callers that
// need to know whether an answer is authoritative ask this rather than assuming.
func (s *Subscriber) Live() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.live
}

// TypingIn returns the users currently typing in a room.
func (s *Subscriber) TypingIn(roomID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	users := s.typing[roomID]
	if len(users) == 0 {
		return nil
	}
	out := make([]string, len(users))
	copy(out, users)
	return out
}

// Run follows the channel until the context is cancelled, reconnecting with
// backoff.
func (s *Subscriber) Run(ctx context.Context) {
	if !s.cfg.Enabled {
		s.log.Info().Msg("replication disabled; stream positions will be approximated from the database")
		return
	}
	backoff := time.Second
	for ctx.Err() == nil {
		subscribed, err := s.session(ctx)
		if err != nil && ctx.Err() == nil {
			s.log.Warn().Err(err).Dur("retry_in", backoff).Msg("replication connection lost")
		}
		s.setLive(false)
		// A session that got as far as subscribing was a working connection,
		// so the next failure starts its backoff from the bottom again.
		// Without this the delay only ever grows, and a worker that has been
		// up long enough to see a handful of unrelated blips waits the full
		// thirty seconds before every subsequent reconnect.
		if subscribed {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (s *Subscriber) session(ctx context.Context) (subscribed bool, err error) {
	// Own cancel, so a fault found while reading can end this session and let
	// the Run loop build a fresh one.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.mu.Lock()
	s.abortSession = cancel
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.abortSession = nil
		s.mu.Unlock()
	}()

	opts := &redis.Options{Addr: s.cfg.Address, Password: s.cfg.Password, DB: s.cfg.DB}
	if strings.HasPrefix(s.cfg.Address, "/") {
		opts.Network = "unix"
	}
	client := redis.NewClient(opts)
	defer func() { _ = client.Close() }()

	// The main channel carries every stream. Synapse puts a few commands on
	// `<channel>/SUFFIX` sub-channels -- USER_IP is one -- which are none of
	// our business.
	pubsub := client.Subscribe(ctx, s.cfg.Channel)
	defer func() { _ = pubsub.Close() }()

	if _, err := pubsub.Receive(ctx); err != nil {
		return false, err
	}
	subscribed = true
	s.setLive(true)
	s.log.Info().Str("channel", s.cfg.Channel).Msg("following the replication stream")

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return subscribed, ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return subscribed, nil
			}
			s.handle(msg.Payload)
		}
	}
}

// abort ends the running session so Run reconnects and resyncs from scratch.
//
// Both halves matter. Clearing the liveness flag is what stops us serving from
// caches we can no longer keep current, and it has to happen now rather than
// whenever the session goroutine unwinds. Cancelling is what makes the flag
// recoverable: setLive(true) runs only when a new session subscribes, so
// clearing it without ending the session leaves the subscriber wedged for the
// life of the process -- which is exactly the bug this function was extracted
// to fix. Neither half is correct alone.
func (s *Subscriber) abort() {
	s.setLive(false)
	s.mu.Lock()
	cancel := s.abortSession
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Subscriber) setLive(live bool) {
	if live {
		metrics.ReplicationConnected.Set(1)
	} else {
		metrics.ReplicationConnected.Set(0)
	}
	s.mu.Lock()
	was := s.live
	if !live {
		// Typing exists only here, so a lost connection means we no longer
		// know who is typing. Claiming otherwise would leave a room showing a
		// typist forever.
		s.typing = map[string][]string{}
		s.typingSerial = map[string]int64{}
		// Half a batch is worse than none: its rows would be applied later
		// against whatever position the next connection happens to supply.
		s.pending = map[string][]pendingRow{}
	}
	s.live = live
	s.mu.Unlock()

	// Only on the edge: the reconnect loop calls setLive(false) on every
	// attempt, and purging the caches once per second while a Redis outage
	// lasts would turn a lost connection into a slow one.
	//
	// Outside the lock -- the callback reaches into the store, and holding a
	// subscriber mutex across it invites a deadlock that would only show up
	// under exactly the failure this exists to handle.
	if was && !live && s.onDrop != nil {
		s.onDrop()
	}
	if !was && live && s.onConnect != nil {
		s.onConnect(s.Positions())
	}
}

// handle parses one replication command.
//
// The wire format is `COMMAND <args>`, one per message. Only RDATA and POSITION
// carry stream positions; everything else -- REMOTE_SERVER_UP, LOCK_RELEASED,
// PING -- is ignored rather than treated as an error, because the channel
// carries traffic for every worker on the server.
func (s *Subscriber) handle(payload string) {
	switch {
	case strings.HasPrefix(payload, "RDATA "):
		s.handleRDATA(payload)
	case strings.HasPrefix(payload, "POSITION "):
		s.handlePosition(payload)
	}
}

// pendingRow is one row of a batch, held until its position is known.
type pendingRow struct {
	row    string
	detail RowDetail
}

// maxPendingBatch bounds the buffer. Synapse's batches are bounded by one
// persistence transaction and are nowhere near this; exceeding it means the
// row that ends the batch is never coming, which is a broken connection rather
// than a large one. Treated as such, so the existing reconnect path recovers.
const maxPendingBatch = 10000

func (s *Subscriber) handleRDATA(payload string) {
	// RDATA <stream> <instance> <token> <row json>
	parts := strings.SplitN(payload, " ", 5)
	if len(parts) < 5 {
		return
	}
	stream, instance, token, row := parts[1], parts[2], parts[3], parts[4]

	detail := rowDetails(stream, row)

	// "global" means the row named neither a room nor a user, so every parked
	// client gets woken. "silent" means the stream wakes nobody by design.
	// Recorded per stream because that is the only way to notice a busy stream
	// in the wrong bucket -- reading the code is how the last four were found,
	// and this metric is how `caches` was.
	//
	// Counted on arrival rather than on apply, because it measures wire volume.
	_, silent := silentStreams[stream]
	scope := "targeted"
	switch {
	case silent:
		scope = "silent"
	case len(detail.RoomIDs) == 0 && len(detail.UserIDs) == 0:
		scope = "global"
	}
	metrics.ReplicationRows.WithLabelValues(stream, scope).Inc()

	// A batched row carries the literal "batch" instead of a position, and the
	// position for every row of the batch is the one the LAST row names. So the
	// rows are held until it arrives and then applied together.
	//
	// The obvious shortcut -- substitute the stream's currently known position,
	// as updateTyping does -- is wrong for anything that records "X changed at
	// P". The known position is the one *before* the batch, so a change would
	// be filed below where it happened, and a client asking "anything since
	// that position?" would be told no. That is a false negative, which is the
	// one answer a stream-change cache may never give. It is tolerable in
	// updateTyping because typing is a last-writer-wins map rather than a
	// change record. Synapse buffers for the same reason:
	// ReplicationCommandHandler._pending_batches in replication/tcp/handler.py.
	if token == "batch" {
		s.mu.Lock()
		s.pending[stream] = append(s.pending[stream], pendingRow{row: row, detail: detail})
		overflow := len(s.pending[stream]) > maxPendingBatch
		if overflow {
			delete(s.pending, stream)
		}
		s.mu.Unlock()
		if overflow {
			s.log.Error().Str("stream", stream).Int("limit", maxPendingBatch).
				Msg("replication batch never terminated; dropping the connection to resync")
			s.abort()
		}
		return
	}

	pos, err := strconv.ParseInt(token, 10, 64)
	if err != nil {
		return
	}

	s.mu.Lock()
	batch := s.pending[stream]
	delete(s.pending, stream)
	s.mu.Unlock()
	batch = append(batch, pendingRow{row: row, detail: detail})

	if stream == StreamTyping {
		// The position moves BEFORE the rows are applied, unlike every other
		// stream below. A writer that has restarted announces itself by
		// sending a position lower than the one we hold, and the reset that
		// triggers must not discard the rows that arrived with it. Synapse
		// orders it the same way.
		s.advance(stream, instance, pos)
		for _, r := range batch {
			s.updateTyping(r.row, pos)
		}
	}

	// Invalidation runs before the position is advanced and before anything is
	// woken. A sync that starts between the two would find the cache already
	// dropped and the position not yet raised, which costs a query; the other
	// order costs a stale answer.
	if s.invalidator != nil {
		for _, r := range batch {
			if stream == StreamCaches {
				s.handleCacheInvalidation(r.row)
			} else {
				s.invalidator.OnRow(stream, pos, r.detail)
			}
		}
	}

	// A silent stream reaches nobody. See silentStreams for why each is there.
	if !silent {
		for _, r := range batch {
			s.record(stream, pos, r.detail.RoomIDs, r.detail.UserIDs)
		}
	}
	if pos > 0 && stream != StreamTyping {
		s.advance(stream, instance, pos)
	}
	if silent {
		return
	}
	for _, r := range batch {
		s.wake(stream, pos, r.detail.RoomIDs, r.detail.UserIDs)
	}
}

// Synapse's sentinel names for the invalidations that mean data has been
// destroyed rather than merely changed
// (storage/databases/main/cache.py). The row is
// [cache_func, keys, invalidation_ts].
const (
	cacheNamePurgeHistory = "ph_cache_fake"
	cacheNameDeleteRoom   = "dr_cache_fake"
	cacheNameCurrentState = "cs_cache_fake"
)

// handleCacheInvalidation acts on the `caches` stream.
//
// Only three names matter, and every other row is ignored on purpose. This
// stream is NOT quiet -- 24 rows in 45 seconds on this deployment -- and it is
// almost entirely Synapse invalidating its own caches for things this worker
// does not hold: get_server_key_json_for_remote, _get_server_keys_json,
// get_destination_retry_timings, _get_bare_e2e_cross_signing_keys. Treating
// any row as a reason to purge threw the state caches away every two seconds,
// which measured as 4,910 extra state-group queries on a single initial sync.
//
// An unparseable row is ignored rather than treated as destructive, for the
// same reason: if the row format ever changes, every row becomes unparseable,
// and a purge on each one is far more damaging than missing the invalidation
// of a room that has been deleted -- which the next restart or reconnect
// clears anyway.
func (s *Subscriber) handleCacheInvalidation(row string) {
	r := gjson.Parse(row)
	if !r.IsArray() {
		metrics.CacheInvalidationRows.WithLabelValues("unparsed").Inc()
		return
	}
	a := r.Array()
	if len(a) == 0 {
		metrics.CacheInvalidationRows.WithLabelValues("unparsed").Inc()
		return
	}

	name := a[0].String()
	switch name {
	case cacheNamePurgeHistory, cacheNameDeleteRoom:
		// Events and state groups have been deleted. Nothing here can map a
		// cached state group back to a room, so this is the one case that
		// genuinely has to drop everything -- and both are rare.
		metrics.CacheInvalidationRows.WithLabelValues("purge").Inc()
		s.invalidator.OnPurge(name)

	case cacheNameCurrentState:
		// A room's current state changed. Keyed by room, so this is targeted.
		metrics.CacheInvalidationRows.WithLabelValues("room").Inc()
		if len(a) >= 2 {
			for _, k := range a[1].Array() {
				s.invalidator.OnRoomInvalidated(k.String())
			}
		}

	default:
		metrics.CacheInvalidationRows.WithLabelValues("ignored").Inc()
	}
}

func (s *Subscriber) handlePosition(payload string) {
	// POSITION <stream> <instance> <prev> <new>
	parts := strings.Fields(payload)
	if len(parts) < 5 {
		return
	}
	stream, instance := parts[1], parts[2]
	prev, prevErr := strconv.ParseInt(parts[3], 10, 64)
	pos, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return
	}

	// `prev` is the position the writer believes we were at. If it is above
	// what we hold, the stream moved further than the rows we saw and we
	// cannot say what happened in between: we were disconnected, or a row was
	// unparseable. Synapse decides the same thing on the same two numbers --
	// `missing_updates = not (prev_token <= current_token <= new_token)` in
	// _process_position -- and then fetches the missing rows over HTTP. We
	// have no such path, and do not need one: everything served here is read
	// from the database at request time, and the one cache that claims to
	// know what changed gives up its horizon below, where the notification
	// naming no subjects reaches it.
	//
	// So this is recorded rather than acted on. It is the difference between
	// a worker that is behind and one that has quietly lost rows, and no
	// other metric can tell them apart.
	if ours := s.bestKnown(stream, instance); prevErr == nil && prev > ours {
		metrics.StreamDiscontinuities.WithLabelValues(stream, "gap").Inc()
		s.log.Info().Str("stream", stream).Str("instance", instance).
			Int64("their_prev", prev).Int64("ours", ours).
			Msg("replication gap; rows were missed and caches below this position are dropped")
	}

	// No room or user is named, so this reaches every listener as "we do not
	// know what changed" -- the notifier wakes everybody, and a stream-change
	// cache must reset its horizon rather than assume it saw the rows in
	// between. The cache hears it before the position moves; see record.
	s.record(stream, pos, nil, nil)
	s.advance(stream, instance, pos)
	s.wake(stream, pos, nil, nil)
}

// bestKnown is the highest position we can claim to have seen for one writer
// of one stream.
//
// The per-writer sample when there is one, and the stream's aggregate
// otherwise -- which is the case at startup, where the aggregate is the
// database seed and no writer has spoken yet. Falling back to it rather than
// to zero keeps a fresh worker from reporting a gap on every stream in its
// first seconds.
func (s *Subscriber) bestKnown(stream, instance string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if pos, ok := s.lastByInstance[stream][instance]; ok {
		return pos
	}
	return s.positions[stream]
}

// advance records a position reported by one writer of one stream.
//
// Three outcomes, and which one applies is decided by resettableStreams:
//
//   - The position moved forwards. The ordinary case.
//   - It moved backwards on a stream whose serial lives in a writer's memory.
//     That writer restarted; adopt the position and drop what we derived from
//     the serials it has forgotten.
//   - It moved backwards on a database-backed stream. Clamp it and say nothing:
//     a token that moves backwards would ask clients to replay, and a writer
//     announcing a lower position is ordinary traffic rather than a fault.
func (s *Subscriber) advance(stream, instance string, pos int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	last, seen := s.lastByInstance[stream][instance]
	backwards := seen && pos < last
	if s.lastByInstance[stream] == nil {
		s.lastByInstance[stream] = map[string]int64{}
	}
	// The highest this writer has claimed, not the latest -- see the redundant
	// case below. A writer that has restarted replaces it outright, further
	// down, because its old high-water mark now names nothing.
	if pos > last || !seen {
		s.lastByInstance[stream][instance] = pos
	}

	what, resettable := resettableStreams[stream]
	if !backwards || !resettable {
		// The ordinary case, and also the redundant one. A writer of a
		// multi-writer stream announces max(its own position, the position
		// everything is persisted up to), so the number it sends can be BELOW
		// one it sent a moment ago without anything being wrong: measured on
		// this deployment as av-inbound-federation-worker-1 announcing
		// caches 85749540 and then 85749539, fifteen minutes after a restart.
		// Synapse discards those silently -- can_discard_position, "we already
		// know the stream ID for the instance has advanced" -- so this cannot
		// be logged or counted as an anomaly without crying wolf on ordinary
		// traffic.
		if pos > s.positions[stream] {
			s.positions[stream] = pos
		}
		return
	}

	metrics.StreamDiscontinuities.WithLabelValues(stream, "reset").Inc()
	s.log.Info().Str("stream", stream).Str("instance", instance).
		Int64("from", last).Int64("to", pos).Str("serial", what).
		Msg("stream went backwards; the writer restarted, resetting")
	s.lastByInstance[stream][instance] = pos
	s.dropDerivedStateLocked(stream)
	s.positions[stream] = pos
}

// dropDerivedStateLocked forgets whatever a restarted writer's old serials
// described. Called with the lock held.
//
// Only typing has any: the other two resettable streams are a federation
// sender's business, and this worker keeps nothing from either. Their entry in
// resettableStreams is still worth having -- it is what stops a backwards
// position from them being reported as an anomaly.
func (s *Subscriber) dropDerivedStateLocked(stream string) {
	if stream != StreamTyping {
		return
	}
	s.typing = map[string][]string{}
	s.typingSerial = map[string]int64{}
}
