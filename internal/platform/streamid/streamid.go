// Package streamid is Synapse's MultiWriterIdGenerator
// (storage/util/id_generators.py) for a stream this process writes alone:
// stream IDs from the stream's Postgres sequence, and the position up to
// which every ID has finished, which is what may be published.
//
// The stream's writers list (stream_writers) names this process only. The
// multi-writer half -- other writers' positions, an idle writer catching up
// to them -- is not here: a stream with two writers changes every follower's
// tokens into vector clocks, and this process does not write one yet.
//
// What it reproduces, because a mistake here loses or repeats rows for every
// follower without any error:
//
//   - At start, as _load_current_ids does for a writer: rows of
//     stream_positions for instances no longer in writers are deleted (a
//     writer added back later starts afresh), and our position is the
//     sequence's last_value (not our row, which may lag a restart).
//   - An ID is unfinished from the moment nextval is asked for it
//     (_in_flight_fetches), so the position cannot pass an ID still being
//     allocated.
//   - Finishing, successful or not, moves the position to the highest
//     finished ID below the lowest unfinished one; with nothing unfinished,
//     to the highest ID allocated (_max_position_of_local_instance). A
//     failure must finish its ID too, or the position is pinned forever.
//   - After a success, stream_positions is upserted to the position,
//     increasing only, in autocommit (_update_stream_positions_table_txn).
package streamid

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	positionGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "goworker_streamid_position",
		Help: "How far every stream ID has finished, by stream: what may be published.",
	}, []string{"stream"})
	unfinishedGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "goworker_streamid_unfinished",
		Help: "Stream IDs allocated and not yet finished, by stream. One that never finishes holds the stream still.",
	}, []string{"stream"})
	finishes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_streamid_finished_total",
		Help: "Stream IDs finished, by stream and outcome (ok, failed, position_write_failed).",
	}, []string{"stream", "outcome"})
)

// DB is the generator's database work.
type DB interface {
	// DeleteStale removes stream_positions rows of instances not in writers.
	DeleteStale(ctx context.Context, stream string, writers []string) error
	// LastValue is the sequence's last_value.
	LastValue(ctx context.Context, sequence string) (int64, error)
	// NextVal allocates one ID.
	NextVal(ctx context.Context, sequence string) (int64, error)
	// UpsertPosition records our position, never lowering it.
	UpsertPosition(ctx context.Context, stream, instance string, pos int64) error
}

// Options builds a Generator.
type Options struct {
	// Stream is the stream's name in stream_positions ("receipts").
	Stream string
	// Instance is this process's instance_name.
	Instance string
	// Sequence is the stream's Postgres sequence ("receipts_sequence").
	Sequence string
	// Writers is stream_writers for the stream; it must name Instance only.
	Writers []string
	// OnAdvance is called after an ID finishes, the position moved or not
	// (notify_replication).
	OnAdvance func()
}

// Generator allocates and tracks one stream's IDs.
type Generator struct {
	o  Options
	db DB

	mu         sync.Mutex
	unfinished map[int64]struct{}
	finished   map[int64]struct{}
	// inFlight counts nextval calls under way, by the highest ID allocated
	// when each began: none of them can return an ID at or below it.
	inFlight map[int64]int
	maxSeen  int64
	position int64
}

// Open loads the stream's position, as a writer's MultiWriterIdGenerator
// does at start.
func Open(ctx context.Context, db DB, o Options) (*Generator, error) {
	if len(o.Writers) != 1 || o.Writers[0] != o.Instance {
		return nil, fmt.Errorf("streamid: %s: writers %v is not this process alone (%s)", o.Stream, o.Writers, o.Instance)
	}
	if err := db.DeleteStale(ctx, o.Stream, o.Writers); err != nil {
		return nil, fmt.Errorf("streamid: %s: stale positions: %w", o.Stream, err)
	}
	last, err := db.LastValue(ctx, o.Sequence)
	if err != nil {
		return nil, fmt.Errorf("streamid: %s: %s: %w", o.Stream, o.Sequence, err)
	}
	g := &Generator{
		o: o, db: db,
		unfinished: map[int64]struct{}{}, finished: map[int64]struct{}{}, inFlight: map[int64]int{},
		maxSeen: last, position: last,
	}
	positionGauge.WithLabelValues(o.Stream).Set(float64(last))
	unfinishedGauge.WithLabelValues(o.Stream).Set(0)
	return g, nil
}

// Position is how far every ID has finished: the stream's current token,
// up to which rows may be published.
func (g *Generator) Position() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.position
}

// ID is one allocated stream ID. Finish it exactly once, whatever happens.
type ID struct {
	g     *Generator
	Value int64
	once  sync.Once
}

// Next allocates an ID (get_next's __aenter__).
func (g *Generator) Next(ctx context.Context) (*ID, error) {
	g.mu.Lock()
	floor := g.maxSeen
	g.inFlight[floor]++
	g.mu.Unlock()

	v, err := g.db.NextVal(ctx, g.o.Sequence)

	g.mu.Lock()
	if g.inFlight[floor]--; g.inFlight[floor] == 0 {
		delete(g.inFlight, floor)
	}
	if err == nil {
		g.unfinished[v] = struct{}{}
		g.maxSeen = max(g.maxSeen, v)
		unfinishedGauge.WithLabelValues(g.o.Stream).Set(float64(len(g.unfinished)))
	}
	g.mu.Unlock()
	if err != nil {
		// No ID came back, so none is held; the fetch is over either way.
		return nil, fmt.Errorf("streamid: %s: nextval: %w", g.o.Stream, err)
	}
	return &ID{g: g, Value: v}, nil
}

// Finish is get_next's __aexit__: the ID is done, ok or not; after a
// success our position is written to stream_positions. Its error is that
// write's, which Synapse raises after the row itself has committed.
func (id *ID) Finish(ctx context.Context, ok bool) error {
	var err error
	id.once.Do(func() {
		g := id.g
		g.markFinished(id.Value)
		if g.o.OnAdvance != nil {
			g.o.OnAdvance()
		}
		if !ok {
			finishes.WithLabelValues(g.o.Stream, "failed").Inc()
			return
		}
		if err = g.db.UpsertPosition(ctx, g.o.Stream, g.o.Instance, g.Position()); err != nil {
			finishes.WithLabelValues(g.o.Stream, "position_write_failed").Inc()
			err = fmt.Errorf("streamid: %s: stream_positions: %w", g.o.Stream, err)
			return
		}
		finishes.WithLabelValues(g.o.Stream, "ok").Inc()
	})
	return err
}

// markFinished is _mark_ids_as_finished with this process as the only
// writer.
func (g *Generator) markFinished(v int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.unfinished, v)
	g.finished[v] = struct{}{}

	var newCur int64
	if len(g.unfinished) > 0 || len(g.inFlight) > 0 {
		minUnfinished := int64(-1)
		for u := range g.unfinished {
			if minUnfinished < 0 || u < minUnfinished {
				minUnfinished = u
			}
		}
		for floor := range g.inFlight {
			// An ID being fetched is above the highest seen when it began.
			if minUnfinished < 0 || floor+1 < minUnfinished {
				minUnfinished = floor + 1
			}
		}
		for f := range g.finished {
			if f < minUnfinished {
				newCur = max(newCur, f)
				delete(g.finished, f)
			}
		}
	} else {
		for f := range g.finished {
			newCur = max(newCur, f)
		}
		clear(g.finished)
		// Nothing in flight: everything allocated has finished.
		newCur = max(newCur, g.maxSeen)
	}
	g.position = max(g.position, newCur)
	positionGauge.WithLabelValues(g.o.Stream).Set(float64(g.position))
	unfinishedGauge.WithLabelValues(g.o.Stream).Set(float64(len(g.unfinished)))
}

// PG is DB over a pool, in autocommit as Synapse runs these: a sequence
// ignores transactions, and the upsert holds its row only for itself.
type PG struct{ Pool *pgxpool.Pool }

func (p PG) DeleteStale(ctx context.Context, stream string, writers []string) error {
	_, err := p.Pool.Exec(ctx,
		`DELETE FROM stream_positions WHERE stream_name = $1 AND NOT (instance_name = ANY($2))`,
		stream, writers)
	return err
}

func (p PG) LastValue(ctx context.Context, sequence string) (int64, error) {
	var v int64
	err := p.Pool.QueryRow(ctx, `SELECT last_value FROM `+pgx.Identifier{sequence}.Sanitize()).Scan(&v)
	return v, err
}

func (p PG) NextVal(ctx context.Context, sequence string) (int64, error) {
	var v int64
	err := p.Pool.QueryRow(ctx, `SELECT nextval($1)`, sequence).Scan(&v)
	return v, err
}

func (p PG) UpsertPosition(ctx context.Context, stream, instance string, pos int64) error {
	_, err := p.Pool.Exec(ctx, `
		INSERT INTO stream_positions (stream_name, instance_name, stream_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (stream_name, instance_name)
		DO UPDATE SET stream_id = EXCLUDED.stream_id
			WHERE stream_positions.stream_id < EXCLUDED.stream_id`,
		stream, instance, pos)
	return err
}
