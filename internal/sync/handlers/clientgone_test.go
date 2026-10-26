package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
)

// clientGone decides whether a failed request is logged as an error or shrugged
// off, so it must be narrow. A long poll exists to be abandoned -- 4% of real
// sliding sync traffic ends that way -- but a deadline that passed is OURS, and
// silencing those would hide the failures the log exists for.
func TestClientGone(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"the caller hung up", context.Canceled, true},
		{"wrapped by a store method", fmt.Errorf("store: visibility extras: %w", context.Canceled), true},
		{"wrapped twice", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", context.Canceled)), true},

		// OURS, not theirs: a statement timeout or a context we bounded. These
		// are real failures and must still be reported.
		{"a deadline we set", context.DeadlineExceeded, false},
		{"a wrapped deadline", fmt.Errorf("store: %w", context.DeadlineExceeded), false},
		{"an ordinary error", errors.New("connection refused"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clientGone(tc.err); got != tc.want {
				t.Errorf("clientGone(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// A refusal and a failure are different things, and the outcome label is the
// only place the dashboard can tell them apart.
func TestRefuseLabelsServerFaultsAsErrors(t *testing.T) {
	cases := []struct {
		status int
		want   string
	}{
		{http.StatusBadRequest, "refused"},
		{http.StatusNotFound, "refused"},
		{http.StatusForbidden, "refused"},
		{http.StatusInternalServerError, "error"},
		{http.StatusBadGateway, "error"},
	}
	for _, tc := range cases {
		ann := &server.Annotation{}
		refuse(httptest.NewRecorder(), ann, tc.status,
			matrixerr.Error{ErrCode: "M_TEST", Error: "test"})
		if ann.Outcome != tc.want {
			t.Errorf("status %d -> outcome %q, want %q", tc.status, ann.Outcome, tc.want)
		}
	}
}

// An outcome the handler already decided is more specific than anything refuse
// can infer, and must survive. client_gone is the case that matters: those
// requests carry a 500 nobody read, and relabelling them "error" would put 4%
// of ordinary traffic back on the failure count.
func TestRefuseKeepsAnOutcomeAlreadySet(t *testing.T) {
	ann := &server.Annotation{Outcome: "client_gone"}
	refuse(httptest.NewRecorder(), ann, http.StatusInternalServerError,
		matrixerr.Error{ErrCode: "M_UNKNOWN", Error: "test"})
	if ann.Outcome != "client_gone" {
		t.Errorf("outcome = %q, want it left as client_gone", ann.Outcome)
	}
}
