// Package cachestream is this process's side of Synapse's caches stream:
// the rows a write puts in cache_invalidation_stream_by_instance, in its
// own transaction (_invalidate_cache_and_stream), and their RDATA, which is
// how every other process drops what it cached.
//
// Every Synapse process writes the caches stream: its MultiWriterIdGenerator
// has writers=[], so there is no stream_positions row to keep, and a writer
// that then goes quiet holds nothing back ("nothing cares if a given
// worker's caches stream position lags", replication/tcp/resource.py).
//
// One Writer per process, because every part that writes caches rows does
// so under the one instance name: a process drops RDATA at or below the
// last token it saw from an instance, so ours must reach the bus in stream
// ID order. Do holds the stream from taking the IDs to publishing the rows.
package cachestream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/bus"
)

// StreamName is the stream's NAME.
const StreamName = "caches"

var published = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goworker_caches_stream_lines_total",
	Help: "Lines this process published on the caches stream, by command (RDATA, POSITION) and outcome (sent, failed).",
}, []string{"command", "outcome"})

// Row is one row of the stream, as written.
type Row struct {
	StreamID       int64
	Instance       string
	CacheFunc      string
	Keys           []string
	InvalidationTS int64
}

// Publisher publishes one replication command line on the bus.
type Publisher interface {
	Publish(ctx context.Context, line string) error
}

// Options builds a Writer.
type Options struct {
	Publisher Publisher
	// Instance is our instance name on the stream.
	Instance string
	// Start is the position a Synapse process starts the stream at
	// (LoadStart): what it announces before its first row. 0 says nothing
	// until then.
	Start int64
	Log   zerolog.Logger
}

// Writer publishes this process's caches rows in order.
type Writer struct {
	pub      Publisher
	instance string
	log      zerolog.Logger
	// RetryDelay is between publish attempts; shortened in tests.
	RetryDelay time.Duration

	mu sync.Mutex
	// last is the last stream ID we wrote; unannounced says its RDATA may
	// not have reached the bus.
	last        int64
	unannounced bool
}

const (
	publishAttempts = 3
	stepTimeout     = 10 * time.Second
)

// New builds a Writer.
func New(o Options) *Writer {
	return &Writer{pub: o.Publisher, instance: o.Instance, log: o.Log, RetryDelay: time.Second, last: o.Start}
}

// Instance is the instance name the rows carry.
func (w *Writer) Instance() string { return w.instance }

// PublishError is a write that committed but whose rows did not all reach
// the bus. The next Do announces our position first, which makes every
// process read the rows it missed from the database (_process_position).
type PublishError struct{ Err error }

func (e *PublishError) Error() string { return e.Err.Error() }
func (e *PublishError) Unwrap() error { return e.Err }

// Do runs write with the stream held. write takes its IDs and inserts its
// rows inside its own transaction (Insert), and has committed when it
// returns them; Do then publishes them in order. An error from write is
// returned as it is; a failed publish is a *PublishError, before write runs
// (the announcement of earlier rows) or after.
func (w *Writer) Do(write func() ([]Row, error)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.unannounced {
		if err := w.publish("POSITION", w.positionLine()); err != nil {
			return &PublishError{err}
		}
		w.unannounced = false
	}
	rows, err := write()
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	for _, r := range rows {
		w.last = max(w.last, r.StreamID)
	}
	w.unannounced = true
	for _, r := range rows {
		line, err := RDATALine(r)
		if err != nil {
			return &PublishError{err}
		}
		if err := w.publish("RDATA", line); err != nil {
			return &PublishError{err}
		}
	}
	w.unannounced = false
	return nil
}

func (w *Writer) publish(command, line string) error {
	var err error
	for i := range publishAttempts {
		if i > 0 {
			time.Sleep(w.RetryDelay)
		}
		ctx, cancel := context.WithTimeout(context.Background(), stepTimeout)
		err = w.pub.Publish(ctx, line)
		cancel()
		if err == nil {
			published.WithLabelValues(command, "sent").Inc()
			return nil
		}
	}
	published.WithLabelValues(command, "failed").Inc()
	return fmt.Errorf("cachestream: publishing %.40q: %w", line, err)
}

// OnReplicate answers a REPLICATE as every Synapse process does for the
// caches stream (each writes it), with its position, so a process that
// just connected catches up on rows it missed. It matters before our first
// row too: a process drops RDATA on a stream it has seen no POSITION for
// on its connection ("Discarding RDATA for unconnected stream"), and one
// that has seen none from us takes our position to be its own persisted
// one, which may already be past our next row.
func (w *Writer) OnReplicate() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.last == 0 {
		return
	}
	if err := w.publish("POSITION", w.positionLine()); err != nil {
		w.log.Warn().Err(err).Msg("Could not answer REPLICATE with our caches position")
		return
	}
	w.unannounced = false
}

// positionLine is POSITION with prev_token = new_token = our last ID, as an
// instance announces itself (commands.py PositionCommand).
func (w *Writer) positionLine() string {
	id := strconv.FormatInt(w.last, 10)
	return "POSITION caches " + w.instance + " " + id + " " + id
}

// RDATALine is the stream's RDATA for r: the row is
// CachesStreamRow(cache_func, keys, invalidation_ts) as a JSON list.
func RDATALine(r Row) (string, error) {
	row, err := json.Marshal([]any{r.CacheFunc, r.Keys, r.InvalidationTS})
	if err != nil {
		return "", err
	}
	return "RDATA caches " + r.Instance + " " + strconv.FormatInt(r.StreamID, 10) + " " + string(row), nil
}

// Listen calls onReplicate for every REPLICATE on c, and each time its
// subscription (re)connects, until ctx ends.
func Listen(ctx context.Context, c *bus.Consumer, onReplicate func()) {
	for {
		ev, ok := c.Next(ctx)
		if !ok {
			return
		}
		if ev.Kind == bus.Connected || ev.Kind == bus.Message && bus.IsReplicate(ev.Line) {
			onReplicate()
		}
	}
}

// qStart is _load_current_ids for a generator with writers=[]: no
// stream_positions rows, so the position is the table's greatest ID.
const qStart = `SELECT GREATEST(COALESCE(MAX(stream_id), 1), 1) FROM cache_invalidation_stream_by_instance`

// LoadStart reads the position a Synapse process starts the caches stream
// at, on a connection of its own (Exec mode, for pgcat).
func LoadStart(ctx context.Context, dsn string, timeout time.Duration) (int64, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return 0, err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	if timeout > 0 {
		cfg.ConnectTimeout = timeout
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var pos int64
	err = conn.QueryRow(ctx, qStart).Scan(&pos)
	return pos, err
}

const qNextIDs = `SELECT nextval('cache_invalidation_stream_seq') FROM generate_series(1, $1)`

const qInsert = `
	INSERT INTO cache_invalidation_stream_by_instance
		(stream_id, instance_name, cache_func, keys, invalidation_ts)
	VALUES ($1, $2, $3, $4, $5)`

// Insert is _send_invalidation_to_replication_bulk inside tx: a row per
// key tuple, IDs from get_next_mult_txn, one timestamp for them all.
func Insert(ctx context.Context, tx pgx.Tx, instance, cacheFunc string, keyTuples [][]string, nowMS int64) ([]Row, error) {
	if len(keyTuples) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, qNextIDs, len(keyTuples))
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, err
	}
	if len(ids) != len(keyTuples) {
		return nil, errors.New("cachestream: nextval returned too few IDs")
	}
	out := make([]Row, len(keyTuples))
	for i, keys := range keyTuples {
		out[i] = Row{StreamID: ids[i], Instance: instance, CacheFunc: cacheFunc, Keys: keys, InvalidationTS: nowMS}
		if _, err := tx.Exec(ctx, qInsert, ids[i], instance, cacheFunc, keys, nowMS); err != nil {
			return nil, err
		}
	}
	return out, nil
}
