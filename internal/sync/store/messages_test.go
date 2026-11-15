package store

import (
	"reflect"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// filter_to_clause's text, for the forms whose meaning is in the SQL: a
// '*' becomes LIKE's '%' anywhere in the type (so '_' is LIKE's too),
// empty lists add nothing, contains_url only when true.
func TestFilterClause(t *testing.T) {
	for _, c := range []struct {
		f    PageFilter
		sql  string
		args []any
	}{
		{PageFilter{}, "", nil},
		{PageFilter{Types: []string{}, Senders: []string{}}, "", nil},
		{PageFilter{Types: []string{"m.*.message", "m.room._x"}},
			"(event.type LIKE $2 OR event.type = $3)", []any{"m.%.message", "m.room._x"}},
		{PageFilter{NotTypes: []string{"m.room.*", "m.reaction"}},
			"event.type NOT LIKE $2 AND event.type != $3", []any{"m.room.%", "m.reaction"}},
		{PageFilter{ContainsURL: true}, "event.contains_url = $2", []any{true}},
		{PageFilter{Labels: []string{"#a", "#b"}, RelatedByRelTypes: []string{"m.annotation"}},
			"(label = $2 OR label = $3) AND (relation_type = $4)", []any{"#a", "#b", "m.annotation"}},
		{PageFilter{NotRelTypes: []string{"m.thread", "m.replace"}},
			"((event_relation.relation_type != $2 AND event_relation.relation_type != $3) OR event_relation.relation_type IS NULL)",
			[]any{"m.thread", "m.replace"}},
	} {
		sql, args := filterClause(&c.f, 2)
		if sql != c.sql || !reflect.DeepEqual(args, c.args) {
			t.Errorf("%+v: got %q %v, want %q %v", c.f, sql, args, c.sql, c.args)
		}
	}
}

// _filter_results: historical bounds compare (topological, stream),
// live ones the stream alone; the lower bound is exclusive.
func TestInPage(t *testing.T) {
	h := streamtoken.Historical(10, 100)
	l := streamtoken.Live(100)
	for _, c := range []struct {
		lower, upper *streamtoken.RoomKey
		topo, stream int64
		want         bool
	}{
		{&h, nil, 10, 100, false},
		{&h, nil, 10, 101, true},
		{&h, nil, 11, -5, true},
		{nil, &h, 10, 100, true},
		{nil, &h, 10, 101, false},
		{&l, nil, 1, 100, false},
		{&l, nil, 1, 101, true},
		{nil, &l, 99, 100, true},
		{nil, &l, 1, 101, false},
	} {
		if got := inPage(c.lower, c.upper, c.topo, c.stream); got != c.want {
			t.Errorf("inPage(%v, %v, %d, %d) = %v", c.lower, c.upper, c.topo, c.stream, got)
		}
	}
}
