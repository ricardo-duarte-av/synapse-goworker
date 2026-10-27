// Package replication follows Synapse's replication stream over Redis.
//
// It only reads, from the worker's one subscription (package bus), and that is
// deliberate. A Python federation sender publishes two things, both of which
// this one leaves out:
//
//   - REPLICATE on connect, which makes every other worker on the bus broadcast
//     its stream positions on our account.
//   - FEDERATION_ACK after each transaction, which tells the main process it may
//     clear its in-memory queues up to min() across sender instances. An ack
//     from a worker that has not actually delivered anything could drop EDUs a
//     real sender had not yet taken.
//
// There is no code path here that publishes (TestPackageNeverPublishes).
//
// The consequence is that we never receive the POSITION broadcast a
// REPLICATE would have triggered. Positions are seeded from the database at
// startup instead and corrected as RDATA arrives; a seeded position is a lower
// bound, which means at worst re-examining events already examined.
package replication

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/bus"
)

// Stream names carried on the replication channel. The rest are here so an
// unrecognised stream can be reported as "not ours" rather than "unknown".
const (
	// StreamEvents is a poke only. The row says an event exists; the sender
	// re-reads the events table for the content
	// (replication/tcp/client.py:190 -> notifier -> notify_new_events).
	StreamEvents = "events"
	// StreamToDevice rows are [entity], where entity is a user id or a remote
	// server name. Only the server names concern a sender.
	StreamToDevice = "to_device"
	// StreamDeviceLists rows are [user_id, is_signature, hosts_calculated].
	// The destinations are not in the row; they come from
	// device_lists_outbound_pokes for that stream id.
	StreamDeviceLists = "device_lists"
	// StreamFederation carries EDUs relayed from the main process by
	// FederationRemoteSendQueue.
	//
	// Note what is NOT here: typing. build_and_send_edu is only ever called on
	// an instance that is itself a federation sender (typing.py:89), so in
	// practice nothing reaches this stream and typing must be built from
	// StreamTyping instead. Consumed anyway, because the queue exists and a
	// future Synapse could use it.
	StreamFederation = "federation"

	StreamReceipts = "receipts"
	// StreamTyping rows are [room_id, [user_id, ...]] -- the WHOLE set of users
	// typing in that room. A federation sender diffs it against the previous
	// set and builds the EDUs itself; see internal/sender.Typing.
	StreamTyping             = "typing"
	StreamPresence           = "presence"
	StreamPresenceFederation = "presence_federation"
	StreamCaches             = "caches"
)

// Command names on the wire (replication/tcp/commands.py).
const (
	cmdRDATA          = "RDATA"
	cmdPOSITION       = "POSITION"
	cmdRemoteServerUp = "REMOTE_SERVER_UP"
	cmdReplicate      = "REPLICATE"
)

// batchToken is the literal token Synapse sends for a row that is part of a
// batch. The next numeric token flushes the buffer plus itself as one batch
// (commands.py:114).
const batchToken = "batch"

// Row is one replication row, already split into its parts.
type Row struct {
	Stream string
	// Instance is the worker that produced the row.
	Instance string
	Position int64
	// JSON is the raw positional array, parsed by the handler that knows the
	// stream's shape.
	JSON string
}

// Handler receives replication events.
//
// Rows arrive in order, batched: OnRows is called once per batch with every row
// that shares a position, which is how Synapse groups them and what lets a
// handler treat a burst of device pokes as one unit of work.
type Handler interface {
	// OnRows is called for each batch of rows on a stream.
	OnRows(stream string, position int64, rows []Row)
	// OnPosition is called when a stream jumps to a new position without rows,
	// which happens when the writer skipped ahead over things we do not care
	// about.
	OnPosition(stream, instance string, position int64)
	// OnRemoteServerUp says a destination that was backing off has been seen
	// to work by somebody else; the sender wakes that destination
	// immediately.
	OnRemoteServerUp(server string)
	// OnReplicate says a Synapse process has (re)connected to the bus. It is
	// called on the subscriber's goroutine and must not block.
	OnReplicate()
}

// Config says where the stream comes from.
type Config struct {
	// Bus is this subscriber's view of the worker's one replication
	// subscription. Nil: no replication, so no new events are seen.
	Bus *bus.Consumer
	// InstanceName is ours, used to drop our own echo: rows this process's
	// inbound path published under the same name.
	InstanceName string
}

// Subscriber follows the replication channel.
type Subscriber struct {
	cfg     Config
	log     zerolog.Logger
	handler Handler

	mu sync.RWMutex
	// live is false whenever the subscription is not known to be healthy.
	// Positions are only trustworthy while it is true.
	live      bool
	positions map[string]int64

	// pending buffers the rows of a batch until the row carrying the batch's
	// position arrives. Keyed by stream, because two streams can be mid-batch
	// at once.
	pending map[string][]Row

	onConnect func()
	onDrop    func()
}

// New builds a Subscriber.
func New(cfg Config, log zerolog.Logger, handler Handler) *Subscriber {
	return &Subscriber{
		cfg:       cfg,
		log:       log,
		handler:   handler,
		positions: map[string]int64{},
		pending:   map[string][]Row{},
	}
}

// SetOnConnect and SetOnDrop register callbacks for the edges of the
// subscription's health, so the caller can seed or invalidate without this
// package knowing about the store.
func (s *Subscriber) SetOnConnect(f func()) { s.onConnect = f }

// SetOnDrop registers a callback for the moment the subscription is lost.
func (s *Subscriber) SetOnDrop(f func()) { s.onDrop = f }

// Seed sets a starting position for a stream, from the database.
//
// A seeded position is a lower bound; the first RDATA for that stream replaces
// it with the truth. This is how we get a starting point without publishing
// REPLICATE.
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

// Positions returns a copy of every known stream position.
func (s *Subscriber) Positions() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int64, len(s.positions))
	for k, v := range s.positions {
		out[k] = v
	}
	return out
}

// Live reports whether the subscription is currently healthy.
func (s *Subscriber) Live() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.live
}

// Run follows the stream until ctx ends.
func (s *Subscriber) Run(ctx context.Context) {
	if s.cfg.Bus == nil {
		s.log.Info().Msg("replication disabled; the worker will not see new events")
		return
	}
	for {
		ev, ok := s.cfg.Bus.Next(ctx)
		if !ok {
			s.setLive(false)
			return
		}
		switch ev.Kind {
		case bus.Connected:
			s.setLive(true)
			if s.onConnect != nil {
				s.onConnect()
			}
		case bus.Dropped:
			s.setLive(false)
		case bus.Message:
			s.handleLine(ev.Line)
		}
	}
}

func (s *Subscriber) setLive(live bool) {
	s.mu.Lock()
	was := s.live
	s.live = live
	if !live {
		// A dropped subscription means the buffered half of a batch will never
		// be completed by the row that names its position. Keeping it would
		// merge it into the next connection's first batch.
		s.pending = map[string][]Row{}
	}
	s.mu.Unlock()

	if was && !live && s.onDrop != nil {
		s.onDrop()
	}
}

// handleLine parses one published message.
//
// The format is "NAME rest-of-line", one line, no newlines
// (replication/tcp/redis.py:263).
func (s *Subscriber) handleLine(line string) {
	// REPLICATE is the one argument-less command that matters: every Synapse
	// process sends it as it connects, so it marks a restart -- the moment a
	// changed homeserver.yaml takes effect in Synapse.
	if strings.TrimSpace(line) == cmdReplicate {
		if s.handler != nil {
			s.handler.OnReplicate()
		}
		return
	}
	name, rest, ok := strings.Cut(line, " ")
	if !ok {
		return
	}
	switch name {
	case cmdRDATA:
		s.handleRDATA(rest)
	case cmdPOSITION:
		s.handlePOSITION(rest)
	case cmdRemoteServerUp:
		if s.handler != nil {
			s.handler.OnRemoteServerUp(strings.TrimSpace(rest))
		}
	}
}

// handleRDATA parses "RDATA <stream> <instance> <token|batch> <row_json>".
func (s *Subscriber) handleRDATA(rest string) {
	stream, rest, ok := strings.Cut(rest, " ")
	if !ok {
		return
	}
	instance, rest, ok := strings.Cut(rest, " ")
	if !ok {
		return
	}
	token, rowJSON, ok := strings.Cut(rest, " ")
	if !ok {
		return
	}

	// Synapse suppresses its own echo by instance name; so do we. The sender
	// never publishes, but the inbound path of this same process does
	// (caches rows, as originup), under the same name.
	if instance == s.cfg.InstanceName {
		return
	}

	row := Row{Stream: stream, Instance: instance, JSON: rowJSON}

	if token == batchToken {
		s.mu.Lock()
		s.pending[stream] = append(s.pending[stream], row)
		s.mu.Unlock()
		return
	}

	pos, err := strconv.ParseInt(token, 10, 64)
	if err != nil {
		s.log.Warn().Str("stream", stream).Str("token", token).Msg("unparseable RDATA token")
		return
	}
	row.Position = pos

	// The numeric token flushes the buffered rows plus itself as one batch.
	s.mu.Lock()
	batch := append(s.pending[stream], row)
	delete(s.pending, stream)
	for i := range batch {
		batch[i].Position = pos
	}
	if pos > s.positions[stream] {
		s.positions[stream] = pos
	}
	s.mu.Unlock()

	if s.handler != nil {
		s.handler.OnRows(stream, pos, batch)
	}
}

// handlePOSITION parses "POSITION <stream> <instance> <prev_token> <new_token>".
//
// Synapse ignores RDATA for a stream until a POSITION for it has arrived on the
// same connection. We deliberately do not: we never send REPLICATE, so no
// POSITION is broadcast on our account, and waiting for one would mean waiting
// for an unrelated worker to trigger it. Our positions are seeded from the
// database instead, so acting on RDATA immediately is safe -- the seed is a
// lower bound and the row is newer than it.
func (s *Subscriber) handlePOSITION(rest string) {
	fields := strings.Fields(rest)
	if len(fields) < 4 {
		return
	}
	stream, instance := fields[0], fields[1]
	pos, err := strconv.ParseInt(fields[3], 10, 64)
	if err != nil {
		return
	}
	if instance == s.cfg.InstanceName {
		return
	}

	s.mu.Lock()
	// A POSITION can jump the stream forward over rows nobody sent us, which
	// is exactly what it is for.
	if pos > s.positions[stream] {
		s.positions[stream] = pos
	}
	s.mu.Unlock()

	if s.handler != nil {
		s.handler.OnPosition(stream, instance, pos)
	}
}
