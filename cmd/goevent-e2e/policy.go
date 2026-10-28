package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

// policy runs the policy server steps, in a room whose policy server is the
// fake (cmd/goevent-policy), driven through its control listener: a signed
// message, a message it calls spam, and its other answers (no signature, an
// error), each refused in Synapse's words. The fake is left signing.
func (s *suite) policy(control, tag string) {
	if control == "" {
		s.skip("policy server", "needs -policy-control")
		return
	}
	mode := func(m string) error {
		r, err := http.Post(strings.TrimRight(control, "/")+"/mode?m="+m, "text/plain", nil)
		if err != nil {
			return err
		}
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		if r.StatusCode != 200 {
			return fmt.Errorf("mode %s: %d %s", m, r.StatusCode, b)
		}
		return nil
	}
	if err := mode("sign"); err != nil {
		s.record("policy server", false, "%v", err)
		return
	}
	defer func() { _ = mode("sign") }()
	msg := func(name, body string) resp {
		return s.c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn(name))), map[string]any{"msgtype": "m.notice", "body": tag + ": " + body}, true)
	}
	r := msg("policy signed", "signed by the policy server")
	s.record("policy server signs", r.status == 200 && r.body.Get("event_id").Str != "", "%d %s", r.status, r.raw)
	s.expectError("refuse what the policy server calls spam", msg("policy spam", "goevent-spam"), 403, "M_FORBIDDEN")
	for _, tc := range []struct {
		mode, name string
		status     int
		errcode    string
	}{
		{"empty", "refuse without a policy signature", 403, "M_FORBIDDEN"},
		{"error", "refuse when the policy server fails", 500, "M_UNKNOWN"},
		{"refuse", "refuse when the policy server refuses all", 403, "M_FORBIDDEN"},
	} {
		if err := mode(tc.mode); err != nil {
			s.record(tc.name, false, "%v", err)
			continue
		}
		s.expectError(tc.name, msg("policy "+tc.mode, "not spam"), tc.status, tc.errcode)
	}
}
