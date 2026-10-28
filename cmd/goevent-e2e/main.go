// Command goevent-e2e drives goworker's event creator through a real
// hostname, as a client would, and checks what comes back: status codes and
// error codes, the events as Synapse serves them afterwards, and the
// worker's own counters (native verify mismatches must not move).
//
// It writes to one of the test rooms only (testRooms), as the test account:
//
//	GOEVENT_E2E_PASSWORD=... GOEXPERIMENT=jsonv2 go run ./cmd/goevent-e2e \
//	  -base https://goevent.aguiarvieira.pt -user test \
//	  -room '!nmZrvcKkeQiFGRlJtE:aguiarvieira.pt' \
//	  -metrics http://<container ip>:9200/metrics
//
// Refusal bodies are compared with refusals*.golden.json, Synapse's own;
// -record rewrites them, so record only against a hostname whose
// event-creation paths go to Synapse itself.
//
// Every event it sends says it is a test. It exits non-zero if any step
// fails.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// testRooms are the rooms the suite may run in: it bans, kicks and sends
// refusals on purpose, so never in a real room. The worker treats them as
// any other room.
var testRooms = []string{
	"!decSfHzLLhrNKJaZid:aguiarvieira.pt", // TEST6TEST, v11
	"!nmZrvcKkeQiFGRlJtE:aguiarvieira.pt", // TEST7TESTE, v11
	// The policy server room (MSC4284), v11. Its m.room.policy is {} since
	// 2026-10-20; the fake policy server stays in synapse-goevent-creator.
	"!FRudVQysKkIHAjrSzd:aguiarvieira.pt",
	// The v12 test room (MSC4291 room ID: the create event's hash).
	"!goBCqT-LJRiRgPetKo34PrUe4GqYZ_R74c6SBGLwTo4",
}

type client struct {
	base, token string
	http        *http.Client
}

type resp struct {
	status int
	body   gjson.Result
	raw    string
}

func (c *client) do(method, path string, body any, token bool) resp {
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		j, _ := json.Marshal(b)
		rd = bytes.NewReader(j)
	}
	req, err := http.NewRequest(method, c.base+path, rd)
	if err != nil {
		return resp{raw: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	// What nginx adds; needed when -base is the worker itself.
	req.Header.Set("X-Forwarded-For", "127.0.0.1")
	if token && c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	r, err := c.http.Do(req)
	if err != nil {
		return resp{raw: err.Error()}
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return resp{status: r.StatusCode, body: gjson.ParseBytes(b), raw: string(b)}
}

type step struct {
	name, result, detail string
}

type suite struct {
	c      *client
	me     string
	device string
	room   string
	run    string
	steps  []step
	failed bool
	// bodies are the refusals' bodies, normalised, by step.
	bodies map[string]string
}

func (s *suite) record(name string, ok bool, detail string, args ...any) {
	r := "PASS"
	if !ok {
		r, s.failed = "FAIL", true
	}
	s.steps = append(s.steps, step{name, r, fmt.Sprintf(detail, args...)})
	fmt.Printf("%-4s %-40s %s\n", r, name, fmt.Sprintf(detail, args...))
}

func (s *suite) skip(name, why string) {
	s.steps = append(s.steps, step{name, "SKIP", why})
	fmt.Printf("%-4s %-40s %s\n", "SKIP", name, why)
}

func esc(s string) string { return url.PathEscape(s) }

func (s *suite) roomPath(rest string) string {
	return "/_matrix/client/v3/rooms/" + esc(s.room) + rest
}

// txn makes a transaction ID unique to this run.
func (s *suite) txn(name string) string { return "goevent-e2e-" + s.run + "-" + name }

// send PUTs an event and checks it comes back as sent.
func (s *suite) send(name, typ string, content map[string]any, query string) string {
	txn := s.txn(name)
	r := s.c.do(http.MethodPut, s.roomPath("/send/"+esc(typ)+"/"+esc(txn))+query, content, true)
	id := r.body.Get("event_id").Str
	if r.status != 200 || id == "" {
		s.record(name, false, "status %d %s", r.status, r.raw)
		return ""
	}
	s.checkEvent(name, id, typ, content, txn)
	return id
}

// checkEvent reads the event back and compares what a client sees.
func (s *suite) checkEvent(name, id, typ string, content map[string]any, txn string) {
	ev := s.c.do(http.MethodGet, s.roomPath("/event/"+esc(id)), nil, true)
	if ev.status != 200 {
		s.record(name, false, "%s: read back %d %s", id, ev.status, ev.raw)
		return
	}
	var problems []string
	if ev.body.Get("type").Str != typ {
		problems = append(problems, "type "+ev.body.Get("type").Str)
	}
	want, _ := json.Marshal(content)
	if !jsonEqual(want, []byte(ev.body.Get("content").Raw)) {
		problems = append(problems, "content "+ev.body.Get("content").Raw)
	}
	// Echoed only to the device that sent it: the event carries our device
	// and transaction in its internal metadata.
	if txn != "" && ev.body.Get("unsigned.transaction_id").Str != txn {
		problems = append(problems, "transaction_id "+ev.body.Get("unsigned.transaction_id").Raw)
	}
	s.record(name, len(problems) == 0, "%s %s", id, strings.Join(problems, "; "))
}

// redacted reads an event until it comes back redacted, for up to 5s.
//
// Read-your-writes holds only on the process that created the redaction
// (replication/http/_base.py waits for the writer's streams there). Another
// process -- main, serving /event here -- catches up over replication, which
// can take a few milliseconds; that is Synapse's model too, whenever the
// creator and the reader are different workers.
func (s *suite) redacted(id string) resp {
	var ev resp
	for deadline := time.Now().Add(5 * time.Second); ; {
		ev = s.c.do(http.MethodGet, s.roomPath("/event/"+esc(id)), nil, true)
		if ev.body.Get("content").Raw == "{}" || time.Now().After(deadline) {
			return ev
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// memberEvent reads a user's member event until want accepts it, for up to
// 5s: main serves the read, and catches up over replication.
func (s *suite) memberEvent(user string, want func(gjson.Result) bool) gjson.Result {
	for deadline := time.Now().Add(5 * time.Second); ; {
		r := s.c.do(http.MethodGet, s.roomPath("/state/m.room.member/"+esc(user)+"?format=event"), nil, true)
		if (r.status == 200 && want(r.body)) || time.Now().After(deadline) {
			return r.body
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// membershipStep sends one membership change and checks the member event it
// leaves, as a client reads it.
func (s *suite) membershipStep(name, method, path string, body map[string]any, wantBody string, user string, content map[string]any) string {
	r := s.c.do(method, path, body, true)
	if r.status != 200 || !jsonEqual([]byte(r.raw), []byte(wantBody)) {
		s.record(name, false, "status %d %s (want %s)", r.status, r.raw, wantBody)
		return ""
	}
	want, _ := json.Marshal(content)
	ev := s.memberEvent(user, func(e gjson.Result) bool { return jsonEqual(want, []byte(e.Get("content").Raw)) })
	ok := jsonEqual(want, []byte(ev.Get("content").Raw)) && ev.Get("sender").Str == s.me
	s.record(name, ok, "%s %s", ev.Get("event_id").Str, ev.Get("content").Raw)
	return ev.Get("event_id").Str
}

// sameMember checks a request made no new member event.
func (s *suite) sameMember(name string, r resp, wantBody, user, eventID string) {
	time.Sleep(300 * time.Millisecond)
	ev := s.memberEvent(user, func(gjson.Result) bool { return true })
	ok := r.status == 200 && jsonEqual([]byte(r.raw), []byte(wantBody)) && eventID != "" && ev.Get("event_id").Str == eventID
	s.record(name, ok, "%d %s; member event %s (want %s)", r.status, r.raw, ev.Get("event_id").Str, eventID)
}

func (s *suite) membership(me, probe, tag string) {
	c := s.c
	path := func(action string) string { return s.roomPath("/" + action) }
	room := `{"room_id":"` + s.room + `"}`

	// Invite, its transaction retried, and the same invite again: both
	// answer without a new event.
	inv := map[string]any{"user_id": probe, "reason": tag}
	invited := s.membershipStep("invite probe", http.MethodPut, path("invite/"+esc(s.txn("invite"))), inv, "{}",
		probe, map[string]any{"membership": "invite", "reason": tag})
	s.sameMember("invite txn retry", c.do(http.MethodPut, path("invite/"+esc(s.txn("invite"))), inv, true), "{}", probe, invited)
	s.sameMember("invite again is deduplicated", c.do(http.MethodPost, path("invite"), inv, true), "{}", probe, invited)

	s.membershipStep("kick invited probe", http.MethodPost, path("kick"), map[string]any{"user_id": probe, "reason": tag}, "{}",
		probe, map[string]any{"membership": "leave", "reason": tag})
	s.expectError("refuse kick of absent user", c.do(http.MethodPost, path("kick"), map[string]any{"user_id": probe}, true), 403, "M_FORBIDDEN")

	banned := s.membershipStep("ban probe", http.MethodPost, path("ban"), map[string]any{"user_id": probe, "reason": tag}, "{}",
		probe, map[string]any{"membership": "ban", "reason": tag})
	s.sameMember("ban again is deduplicated", c.do(http.MethodPost, path("ban"), map[string]any{"user_id": probe, "reason": tag}, true),
		"{}", probe, banned)
	s.expectError("refuse invite of banned user", c.do(http.MethodPost, path("invite"), map[string]any{"user_id": probe}, true), 403, "M_BAD_STATE")
	s.membershipStep("unban probe", http.MethodPost, path("unban"), map[string]any{"user_id": probe}, "{}",
		probe, map[string]any{"membership": "leave"})
	s.expectError("refuse unban of unbanned user", c.do(http.MethodPost, path("unban"), map[string]any{"user_id": probe}, true), 403, "M_BAD_STATE")

	// A remote invitee: its profile is asked of its server, and the events
	// writer sends the invite there. A server that does not exist refuses
	// both, and nothing is created.
	s.expectError("refuse invite to a server that does not exist", c.do(http.MethodPost, path("invite"),
		map[string]any{"user_id": "@goevent-e2e-probe:nonexistent.invalid"}, true), 502, "M_UNKNOWN")
	// A knock by the joined account on the invite-only room: the auth
	// rules refuse it.
	s.expectError("refuse knock on an invite-only room", c.do(http.MethodPost, "/_matrix/client/v3/knock/"+esc(s.room),
		map[string]any{"reason": "e2e"}, true), 403, "M_FORBIDDEN")
	s.expectError("refuse knock without a body", c.do(http.MethodPost, "/_matrix/client/v3/knock/"+esc(s.room), "", true), 400, "M_NOT_JSON")
	s.expectError("refuse ban without user_id", c.do(http.MethodPost, path("ban"), map[string]any{}, true), 400, "M_MISSING_PARAM")
	s.expectError("refuse ban of a bad user_id", c.do(http.MethodPost, path("ban"), map[string]any{"user_id": "nobody"}, true), 400, "M_INVALID_PARAM")

	// Joins by the account, which is joined: no-ops while the profile is
	// unchanged, a new event with a reason, and back.
	mine := s.memberEvent(me, func(gjson.Result) bool { return true })
	profile := map[string]any{}
	_ = json.Unmarshal([]byte(mine.Get("content").Raw), &profile)
	if profile["membership"] != "join" {
		s.skip("joins", "the test account is not joined: "+mine.Raw)
		return
	}
	delete(profile, "reason")
	if mine.Get("content.reason").Exists() {
		// A previous run stopped half way: put the plain join back first.
		s.membershipStep("join without reason (repair)", http.MethodPost, "/_matrix/client/v3/join/"+esc(s.room), map[string]any{}, room, me, profile)
		mine = s.memberEvent(me, func(gjson.Result) bool { return true })
	}
	s.sameMember("join while joined", c.do(http.MethodPost, "/_matrix/client/v3/join/"+esc(s.room), map[string]any{}, true), room, me, mine.Get("event_id").Str)
	s.sameMember("rooms join while joined", c.do(http.MethodPut, path("join/"+esc(s.txn("join"))), map[string]any{}, true), room, me, mine.Get("event_id").Str)
	withReason := map[string]any{"reason": tag}
	for k, v := range profile {
		withReason[k] = v
	}
	s.membershipStep("join with reason", http.MethodPost, path("join"), map[string]any{"reason": tag}, room, me, withReason)
	s.membershipStep("join without reason", http.MethodPost, "/_matrix/client/v3/join/"+esc(s.room), map[string]any{}, room, me, profile)

	// A per-room name, as clients set it: the member event through the
	// state servlet, the content as sent. Then the plain join back, and the
	// same again, which update_membership finds already there.
	statePath := s.roomPath("/state/m.room.member/" + esc(me))
	nick := map[string]any{}
	for k, v := range profile {
		nick[k] = v
	}
	nick["displayname"] = "goevent-e2e " + s.run
	if r := c.do(http.MethodPut, statePath, nick, true); r.status != 200 || r.body.Get("event_id").Str == "" {
		s.record("state member per-room name", false, "%d %s", r.status, r.raw)
	} else {
		want, _ := json.Marshal(nick)
		ev := s.memberEvent(me, func(e gjson.Result) bool { return jsonEqual(want, []byte(e.Get("content").Raw)) })
		s.record("state member per-room name", ev.Get("event_id").Str == r.body.Get("event_id").Str, "%s %s", ev.Get("event_id").Str, ev.Get("content").Raw)
	}
	restored := c.do(http.MethodPut, statePath, profile, true)
	id := restored.body.Get("event_id").Str
	want, _ := json.Marshal(profile)
	ev := s.memberEvent(me, func(e gjson.Result) bool { return jsonEqual(want, []byte(e.Get("content").Raw)) })
	s.record("state member restore", restored.status == 200 && id != "" && ev.Get("event_id").Str == id, "%d %s; member event %s", restored.status, restored.raw, ev.Get("event_id").Str)
	again := c.do(http.MethodPut, statePath, profile, true)
	s.record("state member again is a no-op", again.status == 200 && again.body.Get("event_id").Str == id, "%d %s (want %s)", again.status, again.raw, id)
	s.expectError("refuse member state without membership", c.do(http.MethodPut, statePath, map[string]any{"displayname": "x"}, true), 400, "M_UNKNOWN")
	s.expectError("refuse member state with a bad membership", c.do(http.MethodPut, statePath, map[string]any{"membership": "dance"}, true), 400, "M_UNKNOWN")
}

// expectError checks a refusal: status and errcode.
// delayed adds a delayed state event and waits for main to send it, then
// checks Synapse's refusals of delays it will not take.
func (s *suite) delayed() {
	const typ = "pt.aguiarvieira.goevent.delayed"
	r := s.c.do(http.MethodPut, s.roomPath("/state/"+typ+"/")+"?org.matrix.msc4140.delay=1500", map[string]any{"run": s.run}, true)
	id := r.body.Get("delay_id").Str
	if r.status != 200 || !strings.HasPrefix(id, "syd_") {
		s.record("delayed state event", false, "%d %s", r.status, r.raw)
	} else {
		var got resp
		for i := 0; i < 40; i++ {
			time.Sleep(250 * time.Millisecond)
			got = s.c.do(http.MethodGet, s.roomPath("/state/"+typ+"/"), nil, true)
			if got.body.Get("run").Str == s.run {
				break
			}
		}
		s.record("delayed state event", got.body.Get("run").Str == s.run, "%s, sent: %s", id, got.raw)
	}
	s.expectError("refuse a delay over the maximum", s.c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("delay max")))+
		"?org.matrix.msc4140.delay=86400001", map[string]any{"msgtype": "m.text", "body": "x"}, true), 400, "ORG.MATRIX.MSC4140_DELAY_TOO_LARGE")
	s.expectError("refuse a negative delay", s.c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("delay neg")))+
		"?org.matrix.msc4140.delay=-1", map[string]any{"msgtype": "m.text", "body": "x"}, true), 400, "M_INVALID_PARAM")
}

// fork makes the room's DAG fork and sends on it: a state event the worker
// creates and one main creates (a delayed event, due at once: the worker
// adds it, main sends it), at the same moment, so both build on the same extremity; then a message, whose state
// before must be resolved across both. The worker counts a stored resolved
// group as the verify check state_before. Concurrency is not guaranteed:
// it is tried a few times.
func (s *suite) fork(metricsURL, tag string) {
	const typ = "pt.aguiarvieira.goevent.fork"
	if metricsURL == "" {
		s.skip("send on a fork", "needs -metrics")
		return
	}
	for attempt := 1; attempt <= 10; attempt++ {
		before, err := scrape(metricsURL)
		if err != nil {
			s.record("send on a fork", false, "%v", err)
			return
		}
		run := fmt.Sprintf("%s-%d", s.run, attempt)
		// The delayed event first: main sends it a moment after answering,
		// while the worker is building its own on the same extremity.
		main := s.c.do(http.MethodPut, s.roomPath("/state/"+typ+"/main")+"?org.matrix.msc4140.delay="+strconv.Itoa(attempt%5*25),
			map[string]any{"run": run}, true)
		native := s.c.do(http.MethodPut, s.roomPath("/state/"+typ+"/native"), map[string]any{"run": run}, true)
		time.Sleep(time.Second)
		if native.status != 200 || main.status != 200 || main.body.Get("delay_id").Str == "" {
			s.record("send on a fork", false, "state %d %s / %d %s", native.status, native.raw, main.status, main.raw)
			return
		}
		r := s.c.do(http.MethodPost, s.roomPath("/send/m.room.message"), map[string]any{"msgtype": "m.notice", "body": tag + ": on a fork"}, true)
		if r.status != 200 {
			s.record("send on a fork", false, "%d %s", r.status, r.raw)
			return
		}
		time.Sleep(2 * time.Second)
		after, err := scrape(metricsURL)
		if err != nil {
			s.record("send on a fork", false, "%v", err)
			return
		}
		resolved := after.sum("goevent_native_verify_total", `check="state_before"`) - before.sum("goevent_native_verify_total", `check="state_before"`)
		bad := after.sum("goevent_native_verify_total", `check="state_before"`, `result="mismatch"`) -
			before.sum("goevent_native_verify_total", `check="state_before"`, `result="mismatch"`)
		if resolved > 0 {
			s.record("send on a fork", bad == 0, "attempt %d: %s, resolved state stored and verified (%v mismatches)", attempt, r.body.Get("event_id").Str, bad)
			return
		}
	}
	s.skip("send on a fork", "no fork in 10 attempts (Synapse serialises, or the timing missed)")
}

func (s *suite) expectError(name string, r resp, status int, errcodes ...string) {
	ok := r.status == status && slices.Contains(errcodes, r.body.Get("errcode").Str)
	s.record(name, ok, "status %d errcode %s (want %d %v)", r.status, r.body.Get("errcode").Str, status, errcodes)
	s.bodies[name] = fmt.Sprintf("%d %s", r.status, s.normalise(r.raw))
}

var (
	eventIDs   = regexp.MustCompile(`\$[A-Za-z0-9+/_-]{43}`)
	retryAfter = regexp.MustCompile(`"retry_after_ms":\d+`)
)

// normalise makes a refusal's body comparable across runs: canonical, with
// the run, the account and event IDs replaced, and retry_after_ms zeroed.
func (s *suite) normalise(raw string) string {
	var v any
	if json.Unmarshal([]byte(raw), &v) == nil {
		b, _ := json.Marshal(v)
		raw = string(b)
	}
	raw = strings.ReplaceAll(raw, s.run, "RUN")
	raw = strings.ReplaceAll(raw, s.me, "@ME")
	raw = eventIDs.ReplaceAllString(raw, "$$EVENT")
	return retryAfter.ReplaceAllString(raw, `"retry_after_ms":0`)
}

// golden compares the refusals' bodies with a recording of Synapse's, or
// writes the recording.
func (s *suite) golden(path string, write bool) {
	if write {
		b, _ := json.MarshalIndent(s.bodies, "", "  ")
		if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
			s.record("refusal bodies", false, "%v", err)
			return
		}
		s.record("refusal bodies", true, "%d recorded in %s", len(s.bodies), path)
		return
	}
	b, err := os.ReadFile(path)
	want := map[string]string{}
	if err == nil {
		err = json.Unmarshal(b, &want)
	}
	if err != nil {
		s.record("refusal bodies", false, "%v", err)
		return
	}
	var diffs []string
	for _, name := range slices.Sorted(maps.Keys(s.bodies)) {
		w, ok := want[name]
		switch {
		case !ok:
			diffs = append(diffs, name+": not recorded")
		case w != s.bodies[name]:
			diffs = append(diffs, fmt.Sprintf("%s:\n       got  %s\n       want %s", name, s.bodies[name], w))
		}
	}
	s.record("refusal bodies", len(diffs) == 0, "%d compared with Synapse's%s", len(s.bodies),
		strings.Join(append([]string{""}, diffs...), "\n     "))
}

func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	ja, _ := json.Marshal(x)
	jb, _ := json.Marshal(y)
	return bytes.Equal(ja, jb)
}

// metrics is a scrape: series line -> value.
type metrics map[string]float64

func scrape(u string) (metrics, error) {
	r, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	m := metrics{}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "goevent_") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		var v float64
		fmt.Sscan(line[i+1:], &v)
		m[line[:i]] = v
	}
	return m, nil
}

// sum adds the series of a metric whose labels contain every match.
func (m metrics) sum(name string, match ...string) float64 {
	var t float64
	for k, v := range m {
		if !strings.HasPrefix(k, name+"{") && k != name {
			continue
		}
		ok := true
		for _, s := range match {
			if !strings.Contains(k, s) {
				ok = false
			}
		}
		if ok {
			t += v
		}
	}
	return t
}

func main() { os.Exit(run()) }

func run() int {
	base := flag.String("base", "https://goevent.aguiarvieira.pt", "the test hostname")
	user := flag.String("user", "test", "localpart of the test account")
	room := flag.String("room", "!nmZrvcKkeQiFGRlJtE:aguiarvieira.pt", "a test room (testRooms)")
	metricsURL := flag.String("metrics", "", "the worker's /metrics, to check its counters (optional)")
	goldenPath := flag.String("golden", "cmd/goevent-e2e/refusals.golden.json", "Synapse's refusal bodies, recorded")
	policyControl := flag.String("policy-control", "", "the fake policy server's control listener, for the policy room's steps")
	record := flag.Bool("record", false, "record the refusal bodies to -golden instead of comparing (run against Synapse)")
	flag.Parse()

	if !slices.Contains(testRooms, *room) {
		fmt.Fprintf(os.Stderr, "room %s is not a test room (%v): the suite bans, kicks and sends refusals, never in a real room\n", *room, testRooms)
		return 2
	}
	password := os.Getenv("GOEVENT_E2E_PASSWORD")
	if password == "" {
		fmt.Fprintln(os.Stderr, "GOEVENT_E2E_PASSWORD is not set")
		return 2
	}

	c := &client{base: strings.TrimRight(*base, "/"), http: &http.Client{Timeout: 60 * time.Second}}
	s := &suite{c: c, room: *room, run: fmt.Sprint(time.Now().Unix()), bodies: map[string]string{}}

	var before metrics
	if *metricsURL != "" {
		var err error
		if before, err = scrape(*metricsURL); err != nil {
			fmt.Fprintln(os.Stderr, "metrics:", err)
			return 2
		}
	}

	login := c.do(http.MethodPost, "/_matrix/client/v3/login", map[string]any{
		"type":                        "m.login.password",
		"identifier":                  map[string]any{"type": "m.id.user", "user": *user},
		"password":                    password,
		"initial_device_display_name": "goevent-e2e " + s.run,
	}, false)
	c.token = login.body.Get("access_token").Str
	if c.token == "" {
		fmt.Fprintf(os.Stderr, "login: %d %s\n", login.status, login.raw)
		return 2
	}
	me := login.body.Get("user_id").Str
	s.me, s.device = me, login.body.Get("device_id").Str
	defer func() {
		r := c.do(http.MethodPost, "/_matrix/client/v3/logout", map[string]any{}, true)
		if r.status != 200 {
			fmt.Fprintf(os.Stderr, "logout: %d %s\n", r.status, r.raw)
		}
	}()
	fmt.Printf("run %s as %s in %s\n\n", s.run, me, s.room)

	tag := "goevent-e2e " + s.run

	// /send
	text := s.send("send text", "m.room.message", map[string]any{"msgtype": "m.text", "body": tag + ": text"}, "")
	if text != "" {
		r := c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("send text"))), map[string]any{"msgtype": "m.text", "body": tag + ": text"}, true)
		s.record("send txn retry", r.status == 200 && r.body.Get("event_id").Str == text, "%d %s (want %s)", r.status, r.body.Get("event_id").Str, text)
	}
	post := c.do(http.MethodPost, s.roomPath("/send/m.room.message"), map[string]any{"msgtype": "m.notice", "body": tag + ": POST"}, true)
	if id := post.body.Get("event_id").Str; post.status == 200 && id != "" {
		s.checkEvent("send POST", id, "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": POST"}, "")
	} else {
		s.record("send POST", false, "status %d %s", post.status, post.raw)
	}
	s.send("send notice", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": notice"}, "")
	s.send("send mention", "m.room.message", map[string]any{"msgtype": "m.text", "body": tag + ": mention",
		"m.mentions": map[string]any{"user_ids": []string{me}}}, "")
	if id := s.send("send sticky", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": sticky"},
		"?org.matrix.msc4354.sticky_duration_ms=60000"); id != "" {
		ev := c.do(http.MethodGet, s.roomPath("/event/"+esc(id)), nil, true)
		s.record("sticky field", ev.body.Get("msc4354_sticky.duration_ms").Int() == 60000, "%s", ev.body.Get("msc4354_sticky").Raw)
	}
	s.send("send custom type", "pt.aguiarvieira.goevent.test", map[string]any{"run": s.run, "n": 1}, "")

	if text != "" {
		s.send("send reply", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": reply",
			"m.relates_to": map[string]any{"m.in_reply_to": map[string]any{"event_id": text}}}, "")
		s.send("send edit", "m.room.message", map[string]any{"msgtype": "m.text", "body": "* " + tag + ": edited",
			"m.new_content": map[string]any{"msgtype": "m.text", "body": tag + ": edited"},
			"m.relates_to":  map[string]any{"rel_type": "m.replace", "event_id": text}}, "")
		s.send("send reference", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": reference",
			"m.relates_to": map[string]any{"rel_type": "m.reference", "event_id": text}}, "")
		s.send("send thread", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": thread",
			"m.relates_to": map[string]any{"rel_type": "m.thread", "event_id": text, "is_falling_back": true,
				"m.in_reply_to": map[string]any{"event_id": text}}}, "")
		reaction := map[string]any{"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": text, "key": "🧪"}}
		s.send("send reaction", "m.reaction", reaction, "")
		r := c.do(http.MethodPut, s.roomPath("/send/m.reaction/"+esc(s.txn("reaction twice"))), reaction, true)
		s.expectError("send reaction twice", r, 400, "M_DUPLICATE_ANNOTATION")
	} else {
		s.skip("relations", "no text event to relate to")
	}

	// Refusals: Synapse's words, whoever serves the request.
	s.expectError("refuse content nested too deep", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("deep"))),
		`{"msgtype":"m.text","body":"x","d":`+strings.Repeat("[", 126)+strings.Repeat("]", 126)+`}`, true), 400, "M_BAD_JSON")
	s.expectError("refuse non-object", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("array"))), "[1]", true), 400, "M_BAD_JSON", "M_NOT_JSON")
	s.expectError("refuse invalid JSON", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("garbage"))), "{nope", true), 400, "M_NOT_JSON")
	s.expectError("refuse message without body", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("nobody"))),
		map[string]any{"msgtype": "m.text"}, true), 400, "M_UNKNOWN")
	s.expectError("refuse unknown relation", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("unknownrel"))),
		map[string]any{"msgtype": "m.text", "body": "x", "m.relates_to": map[string]any{"rel_type": "m.replace", "event_id": "$doesnotexist"}}, true), 400, "M_UNKNOWN")
	s.expectError("refuse float", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("float"))),
		`{"msgtype":"m.text","body":"x","n":1.5}`, true), 400, "M_BAD_JSON")
	saved := c.token
	c.token = ""
	s.expectError("refuse no token", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("notoken"))), map[string]any{"msgtype": "m.text", "body": "x"}, true), 401, "M_MISSING_TOKEN")
	c.token = "syt_bogus_token"
	s.expectError("refuse bad token", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("badtoken"))), map[string]any{"msgtype": "m.text", "body": "x"}, true), 401, "M_UNKNOWN_TOKEN")
	c.token = saved

	// /state
	topic := map[string]any{"topic": tag + ": topic"}
	r := c.do(http.MethodPut, s.roomPath("/state/m.room.topic/"), topic, true)
	if id := r.body.Get("event_id").Str; r.status == 200 && id != "" {
		s.checkEvent("state topic", id, "m.room.topic", topic, "")
		again := c.do(http.MethodPut, s.roomPath("/state/m.room.topic/"), topic, true)
		s.record("state repeat is deduplicated", again.status == 200 && again.body.Get("event_id").Str == id, "%d %s (want %s)", again.status, again.body.Get("event_id").Str, id)
	} else {
		s.record("state topic", false, "status %d %s", r.status, r.raw)
	}

	// /state, beyond the topic.
	custom := map[string]any{"run": s.run}
	r = c.do(http.MethodPut, s.roomPath("/state/pt.aguiarvieira.goevent.test/e2e"), custom, true)
	if id := r.body.Get("event_id").Str; r.status == 200 && id != "" {
		s.checkEvent("state custom", id, "pt.aguiarvieira.goevent.test", custom, "")
		next := map[string]any{"run": s.run, "second": true}
		r2 := c.do(http.MethodPut, s.roomPath("/state/pt.aguiarvieira.goevent.test/e2e"), next, true)
		ev := c.do(http.MethodGet, s.roomPath("/event/"+esc(r2.body.Get("event_id").Str)), nil, true)
		s.record("state replaces_state", ev.body.Get("unsigned.replaces_state").Str == id,
			"replaces_state %s (want %s)", ev.body.Get("unsigned.replaces_state").Raw, id)
		got := c.do(http.MethodGet, s.roomPath("/state/pt.aguiarvieira.goevent.test/e2e"), nil, true)
		s.record("state reads back", got.status == 200 && got.body.Get("run").Str == s.run && got.body.Get("second").Bool(), "%d %s", got.status, got.raw)
	} else {
		s.record("state custom", false, "status %d %s", r.status, r.raw)
	}
	r = c.do(http.MethodPut, s.roomPath("/state/pt.aguiarvieira.goevent.test/"+esc(me)), map[string]any{"run": s.run}, true)
	s.record("state keyed by own user", r.status == 200 && r.body.Get("event_id").Str != "", "%d %s", r.status, r.raw)
	s.expectError("refuse state keyed by another user", c.do(http.MethodPut,
		s.roomPath("/state/pt.aguiarvieira.goevent.test/"+esc("@nobody:example.org")), map[string]any{"run": s.run}, true), 403, "M_FORBIDDEN")
	name := c.do(http.MethodGet, s.roomPath("/state/m.room.name/"), nil, true)
	if name.status == 200 {
		r = c.do(http.MethodPut, s.roomPath("/state/m.room.name/"), name.raw, true)
		s.record("state name, unchanged", r.status == 200 && r.body.Get("event_id").Str != "", "%d %s", r.status, r.raw)
	} else {
		s.skip("state name, unchanged", fmt.Sprintf("no name to keep: %d", name.status))
	}
	// Types whose content Synapse validates (validate_new, validate_builder):
	// each valid one harmless (unchanged, or a policy this server ignores),
	// each invalid one refused. A valid tombstone is never sent: it would mark
	// the room as replaced in every client.
	// The valid ones carry this run in a key the type gives no meaning to,
	// so each run makes a new event rather than a duplicate.
	marked := func(raw string) string {
		var m map[string]any
		_ = json.Unmarshal([]byte(raw), &m)
		m["pt.aguiarvieira.goevent.e2e"] = s.run
		b, _ := json.Marshal(m)
		return string(b)
	}
	pl := c.do(http.MethodGet, s.roomPath("/state/m.room.power_levels/"), nil, true)
	if pl.status == 200 {
		r = c.do(http.MethodPut, s.roomPath("/state/m.room.power_levels/"), marked(pl.raw), true)
		s.record("state power levels", r.status == 200 && r.body.Get("event_id").Str != "", "%d %s", r.status, r.raw)
	}
	s.expectError("refuse power level as a string", c.do(http.MethodPut, s.roomPath("/state/m.room.power_levels/"),
		map[string]any{"ban": "50"}, true), 400, "M_BAD_JSON")
	s.expectError("refuse power level null", c.do(http.MethodPut, s.roomPath("/state/m.room.power_levels/"),
		`{"users":{"@a:example.org":null}}`, true), 400, "M_BAD_JSON")
	acl := c.do(http.MethodGet, s.roomPath("/state/m.room.server_acl/"), nil, true)
	if acl.status == 200 {
		r = c.do(http.MethodPut, s.roomPath("/state/m.room.server_acl/"), marked(acl.raw), true)
		s.record("state server ACL, same lists", r.status == 200 && r.body.Get("event_id").Str != "", "%d %s", r.status, r.raw)
	} else {
		s.skip("state server ACL, same lists", fmt.Sprintf("no ACL: %d", acl.status))
	}
	server := strings.SplitN(me, ":", 2)[1]
	s.expectError("refuse ACL denying this server", c.do(http.MethodPut, s.roomPath("/state/m.room.server_acl/"),
		map[string]any{"allow": []string{"*"}, "deny": []string{server}}, true), 400, "M_UNKNOWN")
	r = c.do(http.MethodPut, s.roomPath("/state/m.room.aliases/goevent-e2e"), marked(`{"aliases":[]}`), true)
	s.record("state aliases", r.status == 200 && r.body.Get("event_id").Str != "", "%d %s", r.status, r.raw)
	s.expectError("refuse alias too long", c.do(http.MethodPut, s.roomPath("/state/m.room.aliases/goevent-e2e"),
		map[string]any{"aliases": []string{"#" + strings.Repeat("a", 300) + ":" + server}}, true), 400, "M_INVALID_PARAM")
	r = c.do(http.MethodPut, s.roomPath("/state/m.room.retention/"), marked(`{"max_lifetime":31536000000}`), true)
	s.record("state retention", r.status == 200 && r.body.Get("event_id").Str != "", "%d %s", r.status, r.raw)
	s.expectError("refuse retention min over max", c.do(http.MethodPut, s.roomPath("/state/m.room.retention/"),
		map[string]any{"min_lifetime": 10, "max_lifetime": 5}, true), 400, "M_BAD_JSON")
	s.expectError("refuse retention as a message", c.do(http.MethodPut, s.roomPath("/send/m.room.retention/"+esc(s.txn("retention"))),
		map[string]any{"max_lifetime": 5}, true), 400, "M_UNKNOWN")
	s.expectError("refuse tombstone to this room", c.do(http.MethodPut, s.roomPath("/state/m.room.tombstone/"),
		map[string]any{"replacement_room": s.room}, true), 400, "M_UNKNOWN")
	s.expectError("refuse tombstone without a room", c.do(http.MethodPut, s.roomPath("/state/m.room.tombstone/"),
		map[string]any{"body": "x"}, true), 400, "M_UNKNOWN")

	// /redact
	victim := s.send("send to redact", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": to be redacted"}, "")
	if victim != "" {
		r := c.do(http.MethodPut, s.roomPath("/redact/"+esc(victim)+"/"+esc(s.txn("redact"))), map[string]any{"reason": tag}, true)
		ev := s.redacted(victim)
		s.record("redact", r.status == 200 && r.body.Get("event_id").Str != "" && ev.body.Get("content").Raw == "{}",
			"%d %s; redacted content %s", r.status, r.body.Get("event_id").Str, ev.body.Get("content").Raw)
		if id := r.body.Get("event_id").Str; id != "" {
			red := c.do(http.MethodGet, s.roomPath("/event/"+esc(id)), nil, true)
			s.record("redaction event", red.body.Get("content.redacts").Str == victim && red.body.Get("content.reason").Str == tag &&
				red.body.Get("unsigned.transaction_id").Str == s.txn("redact"), "%s", red.body.Get("content").Raw)
		}
		again := c.do(http.MethodPut, s.roomPath("/redact/"+esc(victim)+"/"+esc(s.txn("redact"))), map[string]any{"reason": tag}, true)
		s.record("redact txn retry", again.status == 200 && again.body.Get("event_id").Str == r.body.Get("event_id").Str,
			"%d %s (want %s)", again.status, again.body.Get("event_id").Str, r.body.Get("event_id").Str)
	}
	if v := s.send("send to redact by POST", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": redacted by POST"}, ""); v != "" {
		r := c.do(http.MethodPost, s.roomPath("/redact/"+esc(v)), map[string]any{}, true)
		ev := s.redacted(v)
		s.record("redact by POST", r.status == 200 && ev.body.Get("content").Raw == "{}", "%d %s", r.status, r.raw)
		s.expectError("refuse redacts that differs", c.do(http.MethodPut, s.roomPath("/redact/"+esc(v)+"/"+esc(s.txn("mismatch"))),
			map[string]any{"redacts": "$someotherevent"}, true), 400, "M_INVALID_PARAM")
	}
	if text != "" {
		reaction := map[string]any{"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": text, "key": "🗑"}}
		if rid := s.send("send reaction to redact", "m.reaction", reaction, ""); rid != "" {
			r := c.do(http.MethodPut, s.roomPath("/redact/"+esc(rid)+"/"+esc(s.txn("redact reaction"))), map[string]any{}, true)
			s.record("redact reaction", r.status == 200, "%d %s", r.status, r.raw)
		}
	}
	create := c.do(http.MethodGet, s.roomPath("/state/m.room.create/?format=event"), nil, true)
	if cid := create.body.Get("event_id").Str; cid != "" {
		s.expectError("refuse redacting the create event", c.do(http.MethodPut, s.roomPath("/redact/"+esc(cid)+"/"+esc(s.txn("create"))),
			map[string]any{}, true), 403, "M_FORBIDDEN")
	} else {
		s.skip("refuse redacting the create event", fmt.Sprintf("create event not found: %d %s", create.status, create.raw))
	}

	s.delayed()
	s.policy(*policyControl, tag)
	s.fork(*metricsURL, tag)
	s.appservice(os.Getenv("GOEVENT_E2E_AS_TOKEN"), tag)

	// Membership: a user that does not exist, so nobody is disturbed, and
	// joins by the test account that is already joined. The account never
	// leaves: the room is invite-only.
	probe := "@goevent-e2e-probe:" + strings.SplitN(me, ":", 2)[1]
	s.membership(me, probe, tag)
	s.golden(*goldenPath, *record)

	if before != nil {
		// Checks run in the background; give them a moment.
		time.Sleep(3 * time.Second)
		after, err := scrape(*metricsURL)
		if err != nil {
			s.record("metrics", false, "%v", err)
		} else {
			d := func(name string, match ...string) float64 {
				return after.sum(name, match...) - before.sum(name, match...)
			}
			s.record("no native mismatches", d("goevent_native_verify_total", `result="mismatch"`) == 0,
				"mismatch +%v, match +%v", d("goevent_native_verify_total", `result="mismatch"`), d("goevent_native_verify_total", `result="match"`))
			fmt.Printf("\nnative: created +%v, fallbacks +%v\n", d("goevent_native_total", `result="created"`), d("goevent_native_fallback_total"))
			for k, v := range after {
				if strings.HasPrefix(k, "goevent_native_fallback_total{") && v-before[k] > 0 {
					fmt.Printf("  %s +%v\n", strings.TrimPrefix(k, "goevent_native_fallback_total"), v-before[k])
				}
			}
		}
	}

	pass, fail, skip := 0, 0, 0
	for _, st := range s.steps {
		switch st.result {
		case "PASS":
			pass++
		case "FAIL":
			fail++
		default:
			skip++
		}
	}
	fmt.Printf("\n%d passed, %d failed, %d skipped\n", pass, fail, skip)
	if s.failed {
		return 1
	}
	return 0
}
