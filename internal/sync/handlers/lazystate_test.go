package handlers

import (
	"encoding/json"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/filter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/lazyload"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// A full-state block sends every lazy-loaded member, even one the cache
// says was sent (#20433); an incremental one leaves those out unless their
// state changed.
func TestDedupeLazyMembersFullState(t *testing.T) {
	f, err := filter.New(json.RawMessage(`{"room":{"state":{"lazy_load_members":true}}}`))
	if err != nil {
		t.Fatal(err)
	}
	d := Deps{LazyLoad: lazyload.NewMembers(0, 0)}
	b := store.StateKey{Type: memberType, StateKey: "@b:x"}
	c := store.StateKey{Type: memberType, StateKey: "@c:x"}
	block := func() map[store.StateKey]string { return map[store.StateKey]string{b: "$b", c: "$c"} }

	startLazyMembers(d, f, "@a:x", "D", nil)
	first := block()
	dedupeLazyMembers(d, f, "@a:x", "D", first, nil, nil, true, streamtoken.Live(10))
	if len(first) != 2 {
		t.Fatalf("initial: %v", first)
	}

	full := block()
	dedupeLazyMembers(d, f, "@a:x", "D", full, nil, nil, true, streamtoken.Live(11))
	if len(full) != 2 {
		t.Errorf("full state filtered: %v", full)
	}

	since := streamtoken.Live(11)
	startLazyMembers(d, f, "@a:x", "D", &since)
	inc := block()
	dedupeLazyMembers(d, f, "@a:x", "D", inc, map[store.StateKey]bool{c: true}, nil, false, streamtoken.Live(12))
	if _, ok := inc[b]; ok || inc[c] != "$c" {
		t.Errorf("incremental: want b left out, c sent as changed: %v", inc)
	}

	// The client lost the response at 12 and retries from 10: what was
	// recorded at 11 and 12 is forgotten.
	back := streamtoken.Live(10)
	startLazyMembers(d, f, "@a:x", "D", &back)
	retry := block()
	dedupeLazyMembers(d, f, "@a:x", "D", retry, nil, nil, false, streamtoken.Live(13))
	if len(retry) != 2 {
		t.Errorf("retry from 10: %v", retry)
	}
}
