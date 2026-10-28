package pdu

import (
	"errors"
	"fmt"

	"github.com/tidwall/gjson"
)

// AuthError is Synapse's AuthError (and its UnstableSpecAuthError subclass):
// the event fails the auth rules. In the receive path this rejects the event;
// in state resolution it keeps the event out of the resolved state; for a
// local event it is the client's answer.
type AuthError struct {
	// Msg is Synapse's message, word for word unless Inexact.
	Msg string
	// Status is the HTTP status: 403, or 500 for an unknown membership.
	Status int
	// Unstable is UnstableSpecAuthError's errcode (MSC3848), "" for a plain
	// AuthError. The errcode itself is M_FORBIDDEN either way.
	Unstable string
	// Inexact is a message that is not Synapse's text: Synapse formats it
	// from Python objects (an event's repr, a tuple) not reproduced here.
	Inexact bool
}

func (e *AuthError) Error() string { return "auth: " + e.Msg }

// The MSC3848 errcodes (api/errors.py).
const (
	CodeAlreadyJoined     = "ORG.MATRIX.MSC3848.ALREADY_JOINED"
	CodeNotJoined         = "ORG.MATRIX.MSC3848.NOT_JOINED"
	CodeInsufficientPower = "ORG.MATRIX.MSC3848.INSUFFICIENT_POWER"
)

// deny is AuthError(403, msg).
func deny(format string, args ...any) error {
	return &AuthError{Msg: fmt.Sprintf(format, args...), Status: 403}
}

// denyUnstable is UnstableSpecAuthError(403, msg, errcode).
func denyUnstable(code, format string, args ...any) error {
	return &AuthError{Msg: fmt.Sprintf(format, args...), Status: 403, Unstable: code}
}

// denyInexact is an AuthError whose text is not Synapse's.
func denyInexact(format string, args ...any) error {
	return &AuthError{Msg: fmt.Sprintf(format, args...), Status: 403, Inexact: true}
}

// RaiseError is anything else Synapse would raise from inside the auth
// checks: a SynapseError (which is NOT an AuthError), a KeyError, a ValueError
// from int(). Synapse does not catch these where it catches AuthError, so the
// consequence differs by caller -- a receive fails instead of rejecting, a
// state resolution fails outright. Callers must not treat it as a rejection.
type RaiseError struct{ Msg string }

func (e *RaiseError) Error() string { return "raise: " + e.Msg }

func raise(msg string) error { return &RaiseError{Msg: msg} }

// IsAuthError reports whether err is a plain auth failure.
func IsAuthError(err error) bool {
	var ae *AuthError
	return errors.As(err, &ae)
}

// intOf is int(v) inside an auth check, where its exception is a RaiseError.
func intOf(v gjson.Result) (int64, error) {
	n, err := pyInt(v)
	if err != nil {
		return 0, raise(err.Error())
	}
	return n, nil
}
