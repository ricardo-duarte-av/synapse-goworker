package native

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/matrixstate"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/e2ekeys"
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

// keysOnly answers /user/keys/query and records what it was asked.
type keysOnly struct {
	Resolver
	opts    matrixstate.KeyQueryOptions
	users   []string
	devices map[string][]string
}

func (k *keysOnly) UserKeys(_ context.Context, users []string, devices map[string][]string) ([]byte, error) {
	k.users, k.devices = users, devices
	return []byte(`{"device_keys":{}}`), nil
}
func (k *keysOnly) KeyQueryOptions() matrixstate.KeyQueryOptions { return k.opts }

func TestUserKeysQuery(t *testing.T) {
	ask := func(r *keysOnly, method, body string) ([]byte, int, error) {
		b, status, _, err := Answer(context.Background(), r, "example.org",
			Request{Endpoint: "user_keys_query", Method: method, Body: []byte(body)})
		return b, status, err
	}
	r := &keysOnly{}
	if _, status, err := ask(r, "POST", `{"device_keys":{"@a:example.org":[],"@b:example.org":null,"@c:example.org":["D1","D2"]}}`); err != nil || status != 200 {
		t.Fatalf("%d %v", status, err)
	}
	if fmt.Sprint(r.users) != "[@a:example.org @b:example.org @c:example.org]" || fmt.Sprint(r.devices["@c:example.org"]) != "[D1 D2]" ||
		r.devices["@a:example.org"] != nil || r.devices["@b:example.org"] != nil {
		t.Errorf("asked %v %v", r.users, r.devices)
	}
	if _, status, err := ask(&keysOnly{}, "POST", `{}`); err != nil || status != 200 {
		t.Errorf("no device_keys: %d %v", status, err)
	}
	body, status, err := ask(&keysOnly{}, "POST", `{"device_keys":{"@a:example.org":[],"@x:elsewhere.org":[]}}`)
	if err != nil || status != 400 || string(body) != `{"errcode":"M_UNKNOWN","error":"User is not hosted on this homeserver"}` {
		t.Errorf("remote user: %d %s %v", status, body, err)
	}
	for name, c := range map[string]struct{ method, body, reason string }{
		"GET":               {"GET", `{}`, "method"},
		"not json":          {"POST", `nope`, "body"},
		"not an object":     {"POST", `{"device_keys":[]}`, "body"},
		"device not string": {"POST", `{"device_keys":{"@a:example.org":[1]}}`, "body"},
		"devices a string":  {"POST", `{"device_keys":{"@a:example.org":"D"}}`, "body"},
		"bad user id":       {"POST", `{"device_keys":{"a:example.org":[]}}`, "user_id"},
	} {
		_, _, err := ask(&keysOnly{}, c.method, c.body)
		var left NotReproduced
		if !errors.As(err, &left) || left.Reason != c.reason {
			t.Errorf("%s: %v", name, err)
		}
	}
	// MSC3984: an appservice's user is Synapse's.
	as := &keysOnly{opts: matrixstate.KeyQueryOptions{MSC3984: true, ExclusiveUsers: regexp.MustCompile(`^@bridge_`)}}
	if _, _, err := ask(as, "POST", `{"device_keys":{"@bridge_1:example.org":[]}}`); !errors.As(err, new(NotReproduced)) {
		t.Errorf("appservice user: %v", err)
	}
	if _, status, err := ask(as, "POST", `{"device_keys":{"@a:example.org":[]}}`); err != nil || status != 200 {
		t.Errorf("not an appservice user: %d %v", status, err)
	}
}

// devicesOnly answers /user/devices and records whom it was asked about.
type devicesOnly struct {
	Resolver
	opts  matrixstate.KeyQueryOptions
	asked []string
	err   error
}

func (d *devicesOnly) UserDevices(_ context.Context, userID string) ([]byte, error) {
	d.asked = append(d.asked, userID)
	return []byte(`{"user_id":"x"}`), d.err
}
func (d *devicesOnly) KeyQueryOptions() matrixstate.KeyQueryOptions { return d.opts }

func TestUserDevices(t *testing.T) {
	ask := func(r *devicesOnly, method, param string) ([]byte, int, error) {
		b, status, _, err := Answer(context.Background(), r, "example.org",
			Request{Endpoint: "user_devices", Method: method, Param: param})
		return b, status, err
	}
	r := &devicesOnly{}
	if _, status, err := ask(r, "GET", "%40a%3Aexample.org"); err != nil || status != 200 || fmt.Sprint(r.asked) != "[@a:example.org]" {
		t.Fatalf("%d %v %v", status, err, r.asked)
	}
	// urllib.parse.unquote, then UserID.from_string and is_mine, in
	// Synapse's words.
	for param, want := range map[string]string{
		"a%3Aexample.org":    `{"errcode":"M_INVALID_PARAM","error":"Expected UserID string to start with '@'"}`,
		"%40a":               `{"errcode":"M_INVALID_PARAM","error":"Expected UserID of the form '@localname:domain'"}`,
		"@a:elsewhere.org":   `{"errcode":"M_UNKNOWN","error":"User is not hosted on this homeserver"}`,
		"@a:example.org:443": `{"errcode":"M_UNKNOWN","error":"User is not hosted on this homeserver"}`,
	} {
		r := &devicesOnly{}
		body, status, err := ask(r, "GET", param)
		if err != nil || status != 400 || string(body) != want || len(r.asked) != 0 {
			t.Errorf("%s: %d %s %v", param, status, body, err)
		}
	}
	for name, c := range map[string]struct {
		method, param string
		r             *devicesOnly
		reason        string
	}{
		"HEAD":          {"HEAD", "@a:example.org", &devicesOnly{}, "method"},
		"POST":          {"POST", "@a:example.org", &devicesOnly{}, "method"},
		"bad escape":    {"GET", "@a%zz:example.org", &devicesOnly{}, "user_id"},
		"not utf-8":     {"GET", "@a%ff:example.org", &devicesOnly{}, "user_id"},
		"no position":   {"GET", "@a:example.org", &devicesOnly{err: matrixstate.ErrNoDeviceStream}, "stream_position"},
		"bad key_json":  {"GET", "@a:example.org", &devicesOnly{err: fmt.Errorf("x: %w", e2ekeys.ErrUnsplicable)}, "key_json"},
		"appservice":    {"GET", "@bridge_1:example.org", &devicesOnly{opts: matrixstate.KeyQueryOptions{MSC3984: true, ExclusiveUsers: regexp.MustCompile(`^@bridge_`)}}, "appservice_keys"},
		"no namespaces": {"GET", "@a:example.org", &devicesOnly{opts: matrixstate.KeyQueryOptions{MSC3984: true, ExclusiveUnknown: true}}, "appservice_namespaces"},
	} {
		_, _, err := ask(c.r, c.method, c.param)
		var left NotReproduced
		if !errors.As(err, &left) || left.Reason != c.reason {
			t.Errorf("%s: %v", name, err)
		}
	}
	// MSC3984 leaves other users ours.
	as := &devicesOnly{opts: matrixstate.KeyQueryOptions{MSC3984: true, ExclusiveUsers: regexp.MustCompile(`^@bridge_`)}}
	if _, status, err := ask(as, "GET", "@a:example.org"); err != nil || status != 200 {
		t.Errorf("not an appservice user: %d %v", status, err)
	}
}
