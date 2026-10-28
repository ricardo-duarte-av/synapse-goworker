package creator

import (
	"context"
	"crypto/rand"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// Shadow-banned requesters (ShadowBanError): Synapse pretends. A non-member
// event, or an invite, is answered after a random pause of one to ten
// seconds, with a fake event ID for an event, and nothing is created.

// shadowBanned answers a shadow-banned event request: a fake event ID,
// remembered for the transaction as HttpTransactionCache remembers it.
func (c *Creator) shadowBanned(ctx context.Context, w http.ResponseWriter, txnKey string) Outcome {
	c.shadowPause(ctx)
	id := fakeEventID()
	c.answers.add(txnKey, eventBody(id), c.o.Now())
	writeEventID(w, id)
	return Outcome{Result: "shadow_banned"}
}

// shadowPause is clock.sleep(random.randint(1, 10)) seconds.
func (c *Creator) shadowPause(ctx context.Context) {
	n, _ := rand.Int(rand.Reader, big.NewInt(10))
	d := time.Duration(n.Int64()+1) * time.Second
	if c.o.ShadowPause != nil {
		d = c.o.ShadowPause(d)
	}
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

// fakeEventID is generate_fake_event_id: "$" and 43 random ASCII letters.
func fakeEventID() string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 44)
	b[0] = '$'
	for i := 1; i < len(b); i++ {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		b[i] = letters[n.Int64()]
	}
	return string(b)
}

// answerCache is HttpTransactionCache for the event endpoints: the body a
// transaction was answered with, for 30 minutes, on this process.
type answerCache struct {
	mu   sync.Mutex
	byID map[string]answerEntry
}

type answerEntry struct {
	id string
	at time.Time
}

func (f *answerCache) get(key string, now time.Time) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.byID[key]
	if !ok || now.Sub(e.at) >= txnCacheTTL {
		return "", false
	}
	return e.id, true
}

func (f *answerCache) add(key, id string, now time.Time) {
	if key == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.byID == nil {
		f.byID = map[string]answerEntry{}
	}
	for k, e := range f.byID {
		if now.Sub(e.at) >= txnCacheTTL {
			delete(f.byID, k)
		}
	}
	f.byID[key] = answerEntry{id, now}
}

// userSuspended is the refusals of a suspended account (MSC3823).
func userSuspended(msg string) *refusal {
	return &refusal{reason: "suspended", status: http.StatusForbidden, errcode: "M_USER_SUSPENDED", msg: msg}
}

// suspendedRedaction is the redact servlet's and create_event's check: a
// suspended account may redact only its own events, or one not held.
func (c *Creator) suspendedRedaction(ctx context.Context, rq *requester, target string) *stop {
	evs, _, err := c.o.Store.GetStoredEvents(ctx, []string{target})
	if err != nil {
		return fb("db_error")
	}
	// get_event(allow_none=True): not a rejected event.
	if ev := evs[target]; ev != nil && ev.Reason == "" && ev.Sender != rq.UserID {
		return userSuspended("You can only redact your own events while account is suspended.").stop()
	}
	return nil
}
