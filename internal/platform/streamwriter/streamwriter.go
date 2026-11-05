// Package streamwriter publishes the replication streams this process
// writes, as Synapse's ReplicationStreamer does (replication/tcp/resource.py)
// for the streams an instance is the source of
// (replication/tcp/handler.py:150).
//
// For each stream it holds how far it has published (Stream.last_token).
// A poke runs the loop: every stream whose current token is past that point
// has its updates fetched and published as RDATA, followed by a POSITION
// when the fetch ended past its last row or found none. REPLICATE (a
// process connecting and asking) is answered with a POSITION of every
// stream at the point published so far; a process that is behind it reads
// what it missed from get_repl_stream_updates (Endpoint), which a stream
// held only in memory, like typing, answers from the same updates.
//
// Unlike Synapse, positions are also announced when our own subscription
// to the bus reconnects: this process publishes and subscribes on separate
// connections, so a REPLICATE sent while the subscription was down is one
// we never heard. An extra POSITION costs a follower nothing.
//
// A line that cannot be published is retried, then dropped, as Synapse
// drops a command it cannot send; the next loop announces positions, which
// makes every follower that missed it fetch it.
package streamwriter

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/bus"
)

// TargetRowCount is how many rows one fetch asks for
// (_STREAM_UPDATE_TARGET_ROW_COUNT, streams/_base.py:47).
const TargetRowCount = 100

var (
	published = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_stream_writer_lines_total",
		Help: "Replication command lines published for the streams this process writes, by stream and command (RDATA, POSITION).",
	}, []string{"stream", "command"})
	publishFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_stream_writer_publish_failures_total",
		Help: "Lines dropped after every attempt to publish them failed, by stream. Followers catch up at the next POSITION.",
	}, []string{"stream"})
	position = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "goworker_stream_writer_position",
		Help: "How far each stream this process writes has been published.",
	}, []string{"stream"})
	fetchFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_stream_writer_fetch_failures_total",
		Help: "Times a stream's updates could not be read, by stream; they are read again shortly.",
	}, []string{"stream"})
	announcements = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_stream_writer_announcements_total",
		Help: "Times every stream's position was announced, by why: replicate (a process asked), connected (our subscription came back), start (a stream was added), failure (a line was dropped).",
	}, []string{"reason"})
)

// Update is one row of a stream at its token. Row is the row as Synapse
// puts it on the wire: a JSON list (attr.astuple of the stream's row type).
type Update struct {
	Token int64
	Row   json.RawMessage
}

// Stream is one stream this process writes.
type Stream interface {
	// Name is the stream's NAME.
	Name() string
	// CurrentToken is how far the stream has been written.
	CurrentToken() int64
	// UpdatesSince returns the rows with from < token <= upto, in token
	// order, at most about limit of them; the token to fetch on from; and
	// whether rows were left out (Stream.update_function). A stream in the
	// database reads them from there.
	UpdatesSince(ctx context.Context, from, upto int64, limit int) ([]Update, int64, bool, error)
}

// Publisher puts one line on the bus (bus.Publisher).
type Publisher interface {
	Publish(ctx context.Context, line string) error
}

// Options builds a Writer.
type Options struct {
	// Instance is this process's instance_name, which every line carries.
	Instance  string
	Publisher Publisher
	Log       zerolog.Logger
	// Attempts and RetryDelay bound publishing one line; defaults 3 and
	// 200ms.
	Attempts   int
	RetryDelay time.Duration
}

// Writer publishes its streams.
type Writer struct {
	opts Options

	mu       sync.Mutex
	streams  []*held
	announce string // non-empty: announce every position, for this reason

	poke chan struct{}
}

type held struct {
	s    Stream
	last int64
}

// New builds a Writer with no streams.
func New(opts Options) *Writer {
	if opts.Attempts <= 0 {
		opts.Attempts = 3
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = 200 * time.Millisecond
	}
	return &Writer{opts: opts, poke: make(chan struct{}, 1)}
}

// Add starts publishing s from its current token, as a Stream starts from
// its current token when the process starts (streams/_base.py: last_token),
// and announces its position.
func (w *Writer) Add(s Stream) {
	w.mu.Lock()
	for _, h := range w.streams {
		if h.s.Name() == s.Name() {
			w.mu.Unlock()
			panic("streamwriter: stream " + s.Name() + " added twice")
		}
	}
	last := s.CurrentToken()
	w.streams = append(w.streams, &held{s: s, last: last})
	position.WithLabelValues(s.Name()).Set(float64(last))
	w.announceLocked("start")
	w.mu.Unlock()
	w.Poke()
}

// Remove stops publishing a stream, when this process stops writing it.
func (w *Writer) Remove(name string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, h := range w.streams {
		if h.s.Name() == name {
			w.streams = append(w.streams[:i], w.streams[i+1:]...)
			return
		}
	}
}

// Stream returns the stream of that name this process writes, or nil.
func (w *Writer) Stream(name string) Stream {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, h := range w.streams {
		if h.s.Name() == name {
			return h.s
		}
	}
	return nil
}

// Positions is every stream's current token, as the response of a
// replication endpoint carries them (minimal_local_current_token).
func (w *Writer) Positions() map[string]int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]int64, len(w.streams))
	for _, h := range w.streams {
		out[h.s.Name()] = h.s.CurrentToken()
	}
	return out
}

// Poke says a stream has moved (notifier.notify_replication).
func (w *Writer) Poke() {
	select {
	case w.poke <- struct{}{}:
	default:
	}
}

// Announce asks for every position to be published, for reason.
func (w *Writer) Announce(reason string) {
	w.mu.Lock()
	w.announceLocked(reason)
	w.mu.Unlock()
	w.Poke()
}

func (w *Writer) announceLocked(reason string) {
	if w.announce == "" {
		w.announce = reason
	}
}

// Run publishes until ctx ends. replicate, when not nil, is a bus consumer:
// REPLICATE on it, and its subscription reconnecting, announce positions.
func (w *Writer) Run(ctx context.Context, replicate *bus.Consumer) {
	if replicate != nil {
		go func() {
			for {
				ev, ok := replicate.Next(ctx)
				if !ok {
					return
				}
				switch {
				case ev.Kind == bus.Connected:
					w.Announce("connected")
				case ev.Kind == bus.Message && bus.IsReplicate(ev.Line):
					w.Announce("replicate")
				}
			}
		}()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.poke:
		}
		w.loop(ctx)
	}
}

// loop is _run_notifier_loop: it runs while there is something to publish.
func (w *Writer) loop(ctx context.Context) {
	for pending := true; pending && ctx.Err() == nil; {
		pending = false

		w.mu.Lock()
		reason := w.announce
		w.announce = ""
		streams := append([]*held(nil), w.streams...)
		w.mu.Unlock()

		if reason != "" {
			announcements.WithLabelValues(reason).Inc()
			for _, h := range streams {
				last := w.lastOf(h)
				w.publish(ctx, h.s.Name(), "POSITION", positionLine(h.s.Name(), w.opts.Instance, last, last))
			}
		}

		for _, h := range streams {
			last := w.lastOf(h)
			current := h.s.CurrentToken()
			if last == current {
				continue
			}
			name := h.s.Name()
			updates, upto, limited, err := h.s.UpdatesSince(ctx, last, current, TargetRowCount)
			if err != nil {
				// Synapse's streamer gives up the loop ("Failed to handle
				// stream"); the stream stays where it was, and is tried again.
				fetchFailures.WithLabelValues(name).Inc()
				w.opts.Log.Warn().Err(err).Str("stream", name).Msg("could not read a stream's updates; trying again")
				time.AfterFunc(failureRetry, w.Poke)
				continue
			}
			pending = pending || limited
			w.setLast(h, upto)

			if len(updates) == 0 {
				w.publish(ctx, name, "POSITION", positionLine(name, w.opts.Instance, last, upto))
				continue
			}
			for i, u := range updates {
				token := strconv.FormatInt(u.Token, 10)
				if i+1 < len(updates) && updates[i+1].Token == u.Token {
					// Rows sharing a token: all but the last say "batch"
					// (_batch_updates).
					token = "batch"
				}
				w.publish(ctx, name, "RDATA", "RDATA "+name+" "+w.opts.Instance+" "+token+" "+string(u.Row))
			}
			if lastRow := updates[len(updates)-1].Token; lastRow < upto {
				w.publish(ctx, name, "POSITION", positionLine(name, w.opts.Instance, lastRow, upto))
			}
		}

		w.mu.Lock()
		if w.announce != "" && w.announce != "failure" {
			pending = true
		}
		w.mu.Unlock()
	}
}

func (w *Writer) lastOf(h *held) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return h.last
}

func (w *Writer) setLast(h *held, v int64) {
	w.mu.Lock()
	h.last = v
	w.mu.Unlock()
	position.WithLabelValues(h.s.Name()).Set(float64(v))
}

// positionLine is POSITION <stream> <instance> <prev_token> <new_token>
// (commands.py PositionCommand).
func positionLine(stream, instance string, prev, next int64) string {
	return "POSITION " + stream + " " + instance + " " + strconv.FormatInt(prev, 10) + " " + strconv.FormatInt(next, 10)
}

func (w *Writer) publish(ctx context.Context, stream, command, line string) {
	var err error
	for i := range w.opts.Attempts {
		if i > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.opts.RetryDelay):
			}
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = w.opts.Publisher.Publish(pctx, line)
		cancel()
		if err == nil {
			published.WithLabelValues(stream, command).Inc()
			return
		}
	}
	publishFailures.WithLabelValues(stream).Inc()
	w.opts.Log.Warn().Err(err).Str("stream", stream).Str("line", truncate(line)).
		Msg("could not publish a replication line; positions are announced again")
	w.mu.Lock()
	w.announceLocked("failure")
	w.mu.Unlock()
	// Not at once: the bus is down, and the loop would spin on it.
	time.AfterFunc(failureRetry, w.Poke)
}

// failureRetry is how long after a dropped line positions are announced.
const failureRetry = 5 * time.Second

func truncate(s string) string {
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}

// String is for logs.
func (u Update) String() string { return fmt.Sprintf("%d %s", u.Token, u.Row) }
