package todevice

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
)

// parse is SendToDeviceRestServlet's client_patterns: r0, v3, unstable;
// /sendToDevice/{type}/{txnId}, each [^/]*, anchored.
func parse(p string) (msgType, txnID string, ok bool) {
	rest, found := strings.CutPrefix(p, "/_matrix/client/")
	if !found {
		return "", "", false
	}
	version, rest, _ := strings.Cut(rest, "/")
	if version != "r0" && version != "v3" && version != "unstable" {
		return "", "", false
	}
	seg := strings.Split(rest, "/")
	if len(seg) != 3 || seg[0] != "sendToDevice" {
		return "", "", false
	}
	return seg[1], seg[2], true
}

// Match is PUT /sendToDevice/{type}/{txnId}; anything else there goes to
// main, which answers as Synapse does.
func Match(method, escapedPath string) bool {
	_, _, ok := parse(escapedPath)
	return ok && method == http.MethodPut
}

// Handler serves what Match accepts.
func (a *App) Handler() http.Handler { return http.HandlerFunc(a.serve) }

func (a *App) serve(w http.ResponseWriter, r *http.Request) {
	if !a.writing {
		requests.WithLabelValues("not_writer").Inc()
		a.notWriter.Do(func() {
			a.Log.Warn().Msg("a sendToDevice request reached this process, which did not start as the " +
				"to-device writer; it goes to main. nginx and homeserver.yaml disagree, or goworker was not restarted")
		})
		a.Fallback.ServeHTTP(w, r)
		return
	}
	ctx := r.Context()
	escMsgType, _, _ := parse(r.URL.EscapedPath())
	v, cerr, err := clientreq.User(ctx, a.Auth, r, true)
	if err != nil {
		a.Log.Error().Err(err).Msg("to-device: cannot validate a token")
		requests.WithLabelValues("auth_unavailable").Inc()
		clientreq.Write(w, cerr, nil)
		return
	}
	if cerr != nil {
		requests.WithLabelValues("auth_rejected").Inc()
		clientreq.Write(w, cerr, nil)
		return
	}
	cerr, cached := a.txns.do(txnKey(r, v), func() *clientreq.Error {
		return a.put(ctx, r, v, clientreq.Unquote(escMsgType))
	})
	switch {
	case cached:
		requests.WithLabelValues("txn_repeat").Inc()
	case cerr != nil:
		requests.WithLabelValues("refused").Inc()
	default:
		requests.WithLabelValues("ok").Inc()
	}
	clientreq.Write(w, cerr, nil)
}

// put is SendToDeviceRestServlet._put.
func (a *App) put(ctx context.Context, r *http.Request, v clientauth.Verdict, msgType string) *clientreq.Error {
	raw, _, cerr := clientreq.ReadObjectRaw(r.Body)
	if cerr != nil {
		return cerr
	}
	raw, err := pyjson.Dedupe(raw)
	if err != nil {
		return clientreq.Internal
	}
	msgs := gjson.GetBytes(raw, "messages")
	if !msgs.Exists() {
		return clientreq.New(400, "M_MISSING_PARAM", "Missing params: ['messages']")
	}
	err = a.Send(ctx, v, msgType, msgs)
	var serr *clientreq.Error
	switch {
	case err == nil:
		return nil
	case errors.As(err, &serr):
		return serr
	}
	a.Log.Error().Err(err).Str("user", v.UserID).Msg("to-device: sendToDevice failed")
	return clientreq.Internal
}

// txnKey is HttpTransactionCache._get_transaction_key.
func txnKey(r *http.Request, v clientauth.Verdict) string {
	p := r.URL.EscapedPath()
	switch {
	case v.IsGuest:
		return p + "\x00guest\x00" + v.UserID
	case v.AppServiceID != "":
		return p + "\x00appservice\x00" + v.AppServiceID
	case v.DeviceID != "":
		return p + "\x00user\x00" + v.UserID + "\x00" + v.DeviceID
	}
	return p + "\x00user_admin\x00" + itoa(v.TokenID)
}

func itoa(n int64) string {
	var b [20]byte
	i := len(b)
	neg := n < 0
	if neg {
		n = -n
	}
	for {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
		if n == 0 {
			break
		}
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// txnCache is HttpTransactionCache: a request whose key is held answers
// as the first did, waiting for it if it is still running. A failed one
// is forgotten (its errback), so a retry runs again; requests that joined
// it get its error. Entries go at the 30-minute cleanup after they are 30
// minutes old, so they live 30 to 60 minutes, as Synapse's do.
type txnCache struct {
	now func() time.Time

	mu        sync.Mutex
	entries   map[string]*txnEntry
	lastClean time.Time
	bytes     int
}

type txnEntry struct {
	done chan struct{}
	err  *clientreq.Error
	at   time.Time
}

const (
	txnCleanup = 30 * time.Minute
	// txnMaxBytes bounds the keys held; past it the cleanup runs early.
	txnMaxBytes = 32 << 20
)

func newTxnCache(now func() time.Time) *txnCache {
	return &txnCache{now: now, entries: map[string]*txnEntry{}, lastClean: now()}
}

func (c *txnCache) do(key string, fn func() *clientreq.Error) (*clientreq.Error, bool) {
	c.mu.Lock()
	now := c.now()
	if now.Sub(c.lastClean) >= txnCleanup || c.bytes > txnMaxBytes {
		c.cleanLocked(now)
	}
	if e, ok := c.entries[key]; ok {
		c.mu.Unlock()
		<-e.done
		return e.err, true
	}
	e := &txnEntry{done: make(chan struct{}), at: now}
	c.entries[key] = e
	c.bytes += len(key)
	c.mu.Unlock()

	e.err = fn()
	close(e.done)
	if e.err != nil {
		c.mu.Lock()
		if c.entries[key] == e {
			delete(c.entries, key)
			c.bytes -= len(key)
		}
		c.mu.Unlock()
	}
	return e.err, false
}

func (c *txnCache) cleanLocked(now time.Time) {
	c.lastClean = now
	for k, e := range c.entries {
		if now.Sub(e.at) > txnCleanup {
			delete(c.entries, k)
			c.bytes -= len(k)
		}
	}
	if c.bytes > txnMaxBytes {
		// Still too big: forget the oldest half.
		cut := now.Add(-txnCleanup / 2)
		for k, e := range c.entries {
			if e.at.Before(cut) {
				delete(c.entries, k)
				c.bytes -= len(k)
			}
		}
	}
}
