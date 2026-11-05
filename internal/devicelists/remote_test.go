package devicelists

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

type extStore struct {
	RemoteStore
	ext *string
}

func (e extStore) Extremity(context.Context, string) (*string, error) { return e.ext, nil }

func TestNeedResync(t *testing.T) {
	ext := "10"
	a := &App{Options: Options{RStore: extStore{ext: &ext}, Now: time.Now}}
	a.r.seen = map[string]*seenEntry{}
	ctx := context.Background()
	up := func(stream string, prev ...string) pendingUpdate {
		return pendingUpdate{streamID: stream, prevIDs: prev}
	}
	for _, c := range []struct {
		name    string
		updates []pendingUpdate
		want    bool
	}{
		{"chains on the extremity", []pendingUpdate{up("11", "10")}, false},
		{"chains within the batch", []pendingUpdate{up("11", "10"), up("12", "11")}, false},
		{"no prev_id", []pendingUpdate{up("11")}, true},
		{"a gap", []pendingUpdate{up("12", "11")}, true},
		{"a later prev in the batch does not count", []pendingUpdate{up("12", "11"), up("11", "10")}, true},
	} {
		got, err := a.needResync(ctx, "@u:x", c.updates)
		if err != nil || got != c.want {
			t.Errorf("%s: %v %v", c.name, got, err)
		}
	}
	// Seen from an EDU (str): chains. Seen from a resync as an int: does
	// not, as in Synapse, where the int 13 is not the str "13".
	a.seenAdd("@u:x", []string{"s:12"}, false)
	if got, _ := a.needResync(ctx, "@u:x", []pendingUpdate{up("13", "12")}); got {
		t.Error("a seen str stream ID did not chain")
	}
	a.seenAdd("@v:x", []string{pyKey(gjson.Parse("13"))}, true)
	if got, _ := a.needResync(ctx, "@v:x", []pendingUpdate{up("14", "13")}); !got {
		t.Error("a seen int stream ID chained")
	}
}

func TestPoppedAndKeys(t *testing.T) {
	enc, _, err := popped(gjson.Parse(`{"user_id":"@u:x","keys":{"b":1,"a":"é"},"device_id":"D","stream_id":5,"deleted":false,"prev_id":[4]}`),
		"user_id", "device_id", "stream_id", "prev_id")
	if err != nil || enc != `{"keys":{"b":1,"a":"`+"\\"+`u00e9"},"deleted":false}` {
		t.Errorf("%s %v", enc, err)
	}
	if v, err := verifyKeyVersion(gjson.Parse(`{"keys":{"ed25519:ABC":"ABC"}}`)); err != nil || v != "ABC" {
		t.Errorf("%s %v", v, err)
	}
	for _, k := range []string{`{"keys":{}}`, `{"keys":{"a:1":"x","ed25519:2":"y"}}`, `{"keys":{"curve25519:1":"x"}}`} {
		if _, err := verifyKeyVersion(gjson.Parse(k)); err == nil {
			t.Errorf("%s accepted", k)
		}
	}
	for raw, want := range map[string]bool{`true`: true, `false`: false, `0`: false, `""`: false, `{}`: false, `[1]`: true, `"x"`: true, `null`: false} {
		if got := clientTruthy(gjson.Parse(raw)); got != want {
			t.Errorf("%s: %v", raw, got)
		}
	}
}

// A remote user's signing key EDU, end to end over the fakes: stored once,
// unchanged keys skipped.
type csStore struct {
	RemoteStore
	keys map[string]json.RawMessage
	set  []string
}

func (c *csStore) CrossSigningKeys(context.Context, string) (map[string]json.RawMessage, error) {
	return c.keys, nil
}
func (c *csStore) SetCrossSigningKey(_ context.Context, _ string, _ string, typ, key string, _ int64) ([]cachestream.Row, error) {
	c.set = append(c.set, typ)
	c.keys[typ] = json.RawMessage(key)
	return nil, nil
}

type noCaches struct{}

func (noCaches) Do(w func() ([]cachestream.Row, error)) error { _, err := w(); return err }

func TestCrossSigningUpdate(t *testing.T) {
	cs := &csStore{keys: map[string]json.RawMessage{"master": json.RawMessage(`{"keys":{"ed25519:M":"M"},"usage":["master"]}`)}}
	hs := &synapsecfg.Config{ServerName: "hs.test"}
	a := &App{Options: Options{RStore: cs, Caches: noCaches{}, Synapse: func() *synapsecfg.Config { return hs }, Log: zerolog.Nop(), Now: time.Now}}
	// The same master key in another order is unchanged; a new self-signing key is stored.
	ids, err := a.crossSigningUpdate(context.Background(), "@u:x",
		gjson.Parse(`{"usage":["master"],"keys":{"ed25519:M":"M"}}`), gjson.Parse(`{"keys":{"ed25519:S":"S"}}`))
	if err != nil || len(ids) != 1 || ids[0] != "S" || len(cs.set) != 1 || cs.set[0] != "self_signing" {
		t.Errorf("%v %v %v", ids, cs.set, err)
	}
}
