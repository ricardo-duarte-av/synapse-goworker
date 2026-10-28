// Package matrixerr renders Matrix client-API errors.
//
// Parity extends to failures: a client distinguishes M_UNKNOWN_TOKEN (log out)
// from M_MISSING_TOKEN (never authenticated) from M_FORBIDDEN (do not retry),
// and getting the code wrong sends clients down the wrong recovery path even
// though the status matches.
package matrixerr

import (
	"encoding/json"
	"net/http"
)

// Error is the standard Matrix error body.
type Error struct {
	ErrCode string `json:"errcode"`
	Error   string `json:"error,omitempty"`
	// SoftLogout appears on M_UNKNOWN_TOKEN when the token expired but the
	// device still exists. Clients treat it very differently from a hard
	// logout: they keep local state and prompt for re-login instead of wiping.
	SoftLogout bool `json:"soft_logout,omitempty"`
	// RetryAfterMS appears on M_LIMIT_EXCEEDED.
	RetryAfterMS int64 `json:"retry_after_ms,omitempty"`
}

const (
	CodeForbidden      = "M_FORBIDDEN"
	CodeUnknownToken   = "M_UNKNOWN_TOKEN"
	CodeMissingToken   = "M_MISSING_TOKEN"
	CodeNotFound       = "M_NOT_FOUND"
	CodeUnknown        = "M_UNKNOWN"
	CodeInvalidParam   = "M_INVALID_PARAM"
	CodeMissingParam   = "M_MISSING_PARAM"
	CodeUnrecognized   = "M_UNRECOGNIZED"
	CodeNotJSON        = "M_NOT_JSON"
	CodeGuestForbidden = "M_GUEST_ACCESS_FORBIDDEN"
)

// Write emits a Matrix error body with the given status.
func Write(w http.ResponseWriter, status int, e Error) {
	body, err := json.Marshal(e)
	if err != nil {
		// Marshalling a struct of strings cannot realistically fail, but a
		// half-written body would be worse than a plain one.
		body = []byte(`{"errcode":"M_UNKNOWN"}`)
		status = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// WriteCode is the common case: a status, an errcode and a message.
func WriteCode(w http.ResponseWriter, status int, code, msg string) {
	Write(w, status, Error{ErrCode: code, Error: msg})
}
