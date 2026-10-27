package clientauth

import (
	"context"
	"crypto/sha256"
	"strings"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/bus"
)

// Run follows the caches stream until ctx ends, dropping what Synapse
// invalidates: a token deleted (logout, a deleted device, deactivation, a
// password change), a user locked or unlocked, a user changed. Without a
// subscription it returns at once, and the TTL alone expires lookups.
func (a *Authenticator) Run(ctx context.Context) {
	if a.repl == nil {
		return
	}
	for {
		ev, ok := a.repl.Next(ctx)
		if !ok {
			return
		}
		switch ev.Kind {
		case bus.Connected, bus.Dropped:
			// Invalidations may have been missed while disconnected, or
			// while this consumer was too far behind to be queued.
			a.purge()
		case bus.Message:
			a.handle(ev.Line)
		}
	}
}

func (a *Authenticator) purge() {
	a.tokens.Purge()
	a.users.Purge()
	a.devices.Purge()
	a.whoami.purge()
	invalidations.WithLabelValues("purge").Inc()
}

// handle reads RDATA caches <instance> <token> [cache_func, keys, ts]. Keys
// null means the whole cache (_send_invalidation_to_replication with
// keys=None).
func (a *Authenticator) handle(line string) {
	rest, ok := strings.CutPrefix(line, "RDATA caches ")
	if !ok {
		return
	}
	parts := strings.SplitN(rest, " ", 3)
	if len(parts) != 3 {
		return
	}
	row := gjson.Parse(parts[2])
	if !row.IsArray() {
		return
	}
	fn, keys := row.Get("0").String(), row.Get("1")
	switch fn {
	case "get_user_by_access_token":
		invalidations.WithLabelValues(fn).Inc()
		if keys.Type == gjson.Null {
			a.tokens.Purge()
			// The whoami cache cannot be searched by token.
			a.whoami.purge()
			return
		}
		token := keys.Get("0").String()
		a.tokens.Remove(sha256.Sum256([]byte(token)))
		// A guest's macaroon is never in access_tokens, but a token that
		// /whoami answered for may be: drop its answers too.
		a.whoami.purge()
	case "get_user_by_id", "get_user_locked_status":
		invalidations.WithLabelValues(fn).Inc()
		if keys.Type == gjson.Null {
			a.users.Purge()
			return
		}
		a.users.Remove(keys.Get("0").String())
	}
}
