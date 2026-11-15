// Package keyring finds the signing keys a PDU's signatures need, as
// Synapse's Keyring does (crypto/keyring.py): what Synapse holds, then the
// trusted key servers (all at once, results merged), then the server itself,
// stopping once every key is found valid for long enough.
//
// Keys fetched here are kept in memory, not stored: Synapse stores them in
// server_signature_keys and server_keys_json and invalidates its caches over
// the replication stream, which is not ours to write. Synapse fetches and
// stores a key itself whenever it needs one, as it does today.
package keyring

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.mau.fi/util/exhttp"
	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix/federation"
	"maunium.net/go/mautrix/federation/signutil"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/backoff"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/fedclient"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

// DBKeys reads the keys Synapse holds (StoreKeyFetcher).
type DBKeys func(ctx context.Context, server string, keyIDs []string) (map[string]pdu.VerifyKey, error)

// Keyring looks keys up.
type Keyring struct {
	db       DBKeys
	fed      *federation.Client
	notaries []string
	server   string
	now      func() time.Time
	// Backoff is the destination retry limiter; nil for none.
	Backoff *backoff.Limiter

	mu  sync.Mutex
	mem map[string]map[string]pdu.VerifyKey
}

// Keys not found are looked for again every time, as Synapse's Keyring does:
// it asks the notaries and the server on each request, the server even with
// ignore_backoff=True (keyring.py:981-993). So a key not found is always a
// full lookup, and is what Synapse's "Failed to find any key" would be.

// New builds a keyring for serverName, asking notaries (trusted_key_servers,
// in order) before the servers themselves.
func New(serverName string, db DBKeys, notaries []string) *Keyring {
	// Each attempt is timed by fedclient.Attempt; this only bounds one.
	settings := exhttp.SensibleClientSettings
	settings.GlobalTimeout = 2 * time.Minute
	fed := fedclient.NewMautrix(serverName, nil, settings)
	return &Keyring{db: db, fed: fed, notaries: notaries, server: serverName, now: time.Now,
		mem: map[string]map[string]pdu.VerifyKey{}}
}

// Fetch is pdu.KeyFetch: the keys found for server among keyIDs, each the
// longest-valid one found, looking further while any is missing or not
// valid until minValidMS.
func (k *Keyring) Fetch(ctx context.Context) pdu.KeyFetch {
	return func(server string, keyIDs []string, minValidMS int64) (map[string]pdu.VerifyKey, error) {
		return k.fetch(ctx, server, keyIDs, minValidMS)
	}
}

func (k *Keyring) fetch(ctx context.Context, server string, keyIDs []string, minValid int64) (map[string]pdu.VerifyKey, error) {
	found := map[string]pdu.VerifyKey{}
	missing := map[string]bool{}
	for _, id := range keyIDs {
		missing[id] = true
	}
	merge := func(keys map[string]pdu.VerifyKey) {
		for id, key := range keys {
			if !missing[id] && found[id].ValidUntilMS >= minValid {
				continue
			}
			if cur, ok := found[id]; ok && cur.ValidUntilMS > key.ValidUntilMS {
				continue
			}
			found[id] = key
			if key.ValidUntilMS >= minValid {
				delete(missing, id)
			}
		}
	}

	held, err := k.db(ctx, server, keyIDs)
	if err != nil {
		return nil, err
	}
	merge(held)
	k.mu.Lock()
	merge(k.mem[server])
	k.mu.Unlock()
	if len(missing) == 0 || server == k.server {
		return found, nil
	}

	merge(k.fromNotaries(ctx, server, setKeys(missing), minValid))
	if len(missing) > 0 {
		merge(k.direct(ctx, server))
	}
	return found, nil
}

// fromNotaries is PerspectivesKeyFetcher for one server's keys.
func (k *Keyring) fromNotaries(ctx context.Context, server string, keyIDs []string, minValid int64) map[string]pdu.VerifyKey {
	want := map[string]map[string]int64{server: {}}
	for _, kid := range keyIDs {
		want[server][kid] = minValid
	}
	return k.queryNotaries(ctx, want)[server]
}

// queryNotaries is PerspectivesKeyFetcher._fetch_keys: one /key/v2/query to
// every notary at once, naming every server and key wanted with the time
// each must be valid until, results merged (the longest-valid key of each
// ID). A notary's own signature is not checked: trusted_key_servers here
// names no verify_keys, so Synapse does not check it either. A response
// naming a server_name that is not a string fails the notary's whole
// answer, as get_server_verify_key_v2_indirect raises on it; any other bad
// entry is skipped.
func (k *Keyring) queryNotaries(ctx context.Context, want map[string]map[string]int64) map[string]map[string]pdu.VerifyKey {
	req := &federation.ReqQueryKeys{ServerKeys: map[string]map[id.KeyID]federation.QueryKeysCriteria{}}
	for server, kids := range want {
		crit := map[id.KeyID]federation.QueryKeysCriteria{}
		for kid, minValid := range kids {
			crit[id.KeyID(kid)] = federation.QueryKeysCriteria{MinimumValidUntilTS: msTime(minValid)}
		}
		req.ServerKeys[server] = crit
	}
	var mu sync.Mutex
	out := map[string]map[string]pdu.VerifyKey{}
	var wg sync.WaitGroup
	for _, notary := range k.notaries {
		if notary == k.server {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var resp struct {
				ServerKeys []json.RawMessage `json:"server_keys"`
			}
			// post_json without a timeout: retried, and backoff honoured.
			err := k.Backoff.Do(ctx, notary, backoff.Options{}, func() (*http.Response, error) {
				return fedclient.Retrying(ctx, func(ctx context.Context) (*http.Response, error) {
					_, r, err := k.fed.MakeFullRequest(ctx, federation.RequestParams{
						ServerName: notary, Method: http.MethodPost,
						Path:        federation.KeyURLPath{"v2", "query"},
						RequestJSON: req, ResponseJSON: &resp,
					})
					return r, err
				})
			})
			if err != nil {
				return
			}
			got := map[string]map[string]pdu.VerifyKey{}
			for _, raw := range resp.ServerKeys {
				var head struct {
					ServerName any `json:"server_name"`
				}
				if err := json.Unmarshal(raw, &head); err != nil {
					continue
				}
				name, ok := head.ServerName.(string)
				if !ok {
					return // KeyLookupError: this notary's answer is dropped
				}
				keys, err := k.processV2(raw, name)
				if err != nil {
					continue
				}
				if got[name] == nil {
					got[name] = map[string]pdu.VerifyKey{}
				}
				for kid, key := range keys {
					got[name][kid] = key
				}
			}
			mu.Lock()
			for name, keys := range got {
				if out[name] == nil {
					out[name] = map[string]pdu.VerifyKey{}
				}
				for kid, key := range keys {
					if cur, ok := out[name][kid]; !ok || key.ValidUntilMS >= cur.ValidUntilMS {
						out[name][kid] = key
					}
				}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	for server, keys := range out {
		k.remember(server, keys)
	}
	return out
}

// KeyRequest is a _FetchKeyRequest.
type KeyRequest = pdu.KeyRequest

// Batch looks up, as one batch of Synapse's Keyring does, every key the
// requests need: deduplicated to each key's latest required validity
// (_inner_fetch_key_requests), then what Synapse holds, then one query to
// each notary for everything still missing, then each server still
// missing a key, its servers at once. It returns a lookup that answers
// from what the batch found and asks nobody again, as every signature
// check of a batch waits on the one fetch in Synapse: a key nobody had is
// not looked for once per event.
func (k *Keyring) Batch(ctx context.Context, reqs []KeyRequest) pdu.KeyFetch {
	want := map[string]map[string]int64{}
	for _, r := range reqs {
		if want[r.Server] == nil {
			want[r.Server] = map[string]int64{}
		}
		for _, kid := range r.KeyIDs {
			if cur, ok := want[r.Server][kid]; !ok || r.MinValidMS > cur {
				want[r.Server][kid] = r.MinValidMS
			}
		}
	}
	found := map[string]map[string]pdu.VerifyKey{}
	var lookupErr error
	merge := func(server string, keys map[string]pdu.VerifyKey) {
		if found[server] == nil {
			found[server] = map[string]pdu.VerifyKey{}
		}
		for kid, key := range keys {
			if _, asked := want[server][kid]; !asked {
				continue
			}
			if cur, ok := found[server][kid]; !ok || key.ValidUntilMS > cur.ValidUntilMS {
				found[server][kid] = key
			}
		}
	}
	missing := func(server string) map[string]int64 {
		out := map[string]int64{}
		for kid, minValid := range want[server] {
			if key, ok := found[server][kid]; !ok || key.ValidUntilMS < minValid {
				out[kid] = minValid
			}
		}
		return out
	}

	for server, kids := range want {
		ids := make([]string, 0, len(kids))
		for kid := range kids {
			ids = append(ids, kid)
		}
		held, err := k.db(ctx, server, ids)
		if err != nil {
			lookupErr = err
			break
		}
		merge(server, held)
		k.mu.Lock()
		merge(server, k.mem[server])
		k.mu.Unlock()
	}
	if lookupErr == nil {
		ask := map[string]map[string]int64{}
		for server := range want {
			if server == k.server {
				continue
			}
			if m := missing(server); len(m) > 0 {
				ask[server] = m
			}
		}
		if len(ask) > 0 {
			for server, keys := range k.queryNotaries(ctx, ask) {
				if _, asked := want[server]; asked {
					merge(server, keys)
				}
			}
		}
		var direct []string
		for server := range ask {
			if len(missing(server)) > 0 {
				direct = append(direct, server)
			}
		}
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, server := range direct {
			wg.Add(1)
			go func() {
				defer wg.Done()
				keys := k.direct(ctx, server)
				mu.Lock()
				merge(server, keys)
				mu.Unlock()
			}()
		}
		wg.Wait()
	}

	return func(server string, keyIDs []string, minValid int64) (map[string]pdu.VerifyKey, error) {
		if lookupErr != nil {
			return nil, lookupErr
		}
		out := map[string]pdu.VerifyKey{}
		for _, kid := range keyIDs {
			if key, ok := found[server][kid]; ok {
				out[kid] = key
			}
		}
		if _, asked := want[server]; !asked {
			// Not in the batch: looked up on its own.
			return k.fetch(ctx, server, keyIDs, minValid)
		}
		return out, nil
	}
}

// direct is ServerKeyFetcher: GET /_matrix/key/v2/server with
// ignore_backoff and a 10s timeout, so one attempt.
func (k *Keyring) direct(ctx context.Context, server string) map[string]pdu.VerifyKey {
	var body []byte
	err := k.Backoff.Do(ctx, server, backoff.Options{IgnoreBackoff: true}, func() (*http.Response, error) {
		return fedclient.Attempt(ctx, 10*time.Second, func(ctx context.Context) (*http.Response, error) {
			b, r, err := k.fed.MakeFullRequest(ctx, federation.RequestParams{
				ServerName: server, Method: http.MethodGet, Path: federation.KeyURLPath{"v2", "server"},
			})
			body = b
			return r, err
		})
	})
	if err != nil {
		return nil
	}
	keys, err := k.processV2(body, server)
	if err != nil {
		return nil
	}
	k.remember(server, keys)
	return keys
}

func (k *Keyring) remember(server string, keys map[string]pdu.VerifyKey) {
	if len(keys) == 0 {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	m := k.mem[server]
	if m == nil {
		m = map[string]pdu.VerifyKey{}
		k.mem[server] = m
	}
	for kid, key := range keys {
		if cur, ok := m[kid]; !ok || key.ValidUntilMS >= cur.ValidUntilMS {
			m[kid] = key
		}
	}
}

// processV2 is BaseV2KeyFetcher.process_v2_response for a response about
// server: its verify_keys (valid until valid_until_ts), self-signed by one
// of them, and old_verify_keys (valid until expired_ts).
func (k *Keyring) processV2(raw []byte, server string) (map[string]pdu.VerifyKey, error) {
	var resp struct {
		ServerName   string `json:"server_name"`
		ValidUntilTS int64  `json:"valid_until_ts"`
		VerifyKeys   map[string]struct {
			Key string `json:"key"`
		} `json:"verify_keys"`
		OldVerifyKeys map[string]struct {
			Key       string `json:"key"`
			ExpiredTS int64  `json:"expired_ts"`
		} `json:"old_verify_keys"`
		Signatures map[string]map[string]string `json:"signatures"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, err
	}
	if resp.ServerName != server {
		return nil, errors.New("keyring: response for another server")
	}
	out := map[string]pdu.VerifyKey{}
	for kid, v := range resp.VerifyKeys {
		if b, ok := decodeKey(kid, v.Key); ok {
			out[kid] = pdu.VerifyKey{Key: b, ValidUntilMS: resp.ValidUntilTS}
		}
	}
	verified := false
	for kid := range resp.Signatures[server] {
		key, ok := out[kid]
		if !ok {
			continue
		}
		if err := signutil.VerifyJSON(server, id.KeyID(kid), id.SigningKey(base64.RawStdEncoding.EncodeToString(key.Key)), json.RawMessage(raw)); err != nil {
			return nil, err
		}
		verified = true
		break
	}
	if !verified {
		return nil, errors.New("keyring: key response not signed by the origin server")
	}
	for kid, v := range resp.OldVerifyKeys {
		if b, ok := decodeKey(kid, v.Key); ok {
			out[kid] = pdu.VerifyKey{Key: b, ValidUntilMS: v.ExpiredTS}
		}
	}
	return out, nil
}

// decodeKey is is_signing_algorithm_supported plus decoding: ed25519 keys
// only.
func decodeKey(kid, b64 string) (ed25519.PublicKey, bool) {
	if !strings.HasPrefix(kid, "ed25519:") {
		return nil, false
	}
	b, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(b64, "="))
	if err != nil || len(b) != ed25519.PublicKeySize {
		return nil, false
	}
	return b, true
}

func setKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func msTime(ms int64) jsontime.UnixMilli { return jsontime.UM(time.UnixMilli(ms)) }
