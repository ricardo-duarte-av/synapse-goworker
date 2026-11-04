package native

import (
	"context"
	"errors"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/matrixstate"
)

// missingOnly answers /get_missing_events and counts the calls.
type missingOnly struct {
	Resolver
	calls int
}

func (m *missingOnly) GetMissingEvents(context.Context, string, string, string, []string, []string, int) (*matrixstate.MissingEventsResponse, error) {
	m.calls++
	return &matrixstate.MissingEventsResponse{}, nil
}

// MSC4242's state DAG walk is Synapse's, for any value Python finds true.
func TestGetMissingEventsStateDag(t *testing.T) {
	for body, synapses := range map[string]bool{
		`{"org.matrix.msc4242.state_dag":true}`:  true,
		`{"org.matrix.msc4242.state_dag":1}`:     true,
		`{"org.matrix.msc4242.state_dag":"yes"}`: true,
		`{"org.matrix.msc4242.state_dag":false}`: false,
		`{"org.matrix.msc4242.state_dag":0}`:     false,
		`{"org.matrix.msc4242.state_dag":null}`:  false,
		`{"org.matrix.msc4242.state_dag":{}}`:    false,
		`{"limit":5}`:                            false,
	} {
		r := &missingOnly{}
		_, _, _, err := Answer(context.Background(), r, "example.org",
			Request{Endpoint: "get_missing_events", RoomID: "!r:x", Body: []byte(body)})
		var left NotReproduced
		if got := errors.As(err, &left); got != synapses || (synapses && left.Reason != "state_dag") {
			t.Errorf("%s: err %v", body, err)
		}
		if !synapses && (err != nil || r.calls != 1) {
			t.Errorf("%s: answered %d times, err %v", body, r.calls, err)
		}
	}
}
