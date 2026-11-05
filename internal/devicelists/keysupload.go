package devicelists

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
)

// Synapse's limits and names (handlers/e2e_keys.py).
const (
	maxOneTimeKeysPerAlgorithm = 500
	oneTimeKeyUploadLock       = "one_time_key_upload_lock"
	signedCurve25519           = "signed_curve25519"
)

// Locker is Synapse's WorkerLocksHandler.acquire_lock (inbound's lock
// store, the process's one): release when done.
type Locker interface {
	Acquire(ctx context.Context, name, key string) (release func(), err error)
}

// KeysStore is what /keys/upload reads and writes (DB).
type KeysStore interface {
	SetDeviceKeys(ctx context.Context, userID, deviceID string, nowMS int64, canonical string) (bool, error)
	OneTimeKeys(ctx context.Context, userID, deviceID string, keyIDs []string) (map[[2]string]string, error)
	CountOneTimeKeys(ctx context.Context, userID, deviceID string) ([][2]any, error)
	AddOneTimeKeys(ctx context.Context, instance, userID, deviceID string, nowMS int64, keys [][3]string) ([]cachestream.Row, error)
	SetFallbackKeys(ctx context.Context, userID, deviceID string, keys []member3) error
}

type member3 struct{ algorithm, keyID, canonical, encoded string }

// pydErrors collects a pydantic ValidationError as Synapse renders it
// (_format_pydantic_error): each message once, in order, joined by "; ";
// the count is the raw errors', which picks the errcode.
type pydErrors struct {
	msgs  []string
	count int
	kind  string // the first error's type
}

func (p *pydErrors) add(kind, msg string) {
	if p.count == 0 {
		p.kind = kind
	}
	p.count++
	for _, m := range p.msgs {
		if m == msg {
			return
		}
	}
	p.msgs = append(p.msgs, msg)
}

func (p *pydErrors) typ(path, what string) {
	p.add("type", "'"+path+"': Input should be a valid "+what)
}
func (p *pydErrors) missing(path string) { p.add("missing", "Missing required field '"+path+"'") }

func (p *pydErrors) err() *clientreq.Error {
	if p.count == 0 {
		return nil
	}
	code := "M_BAD_JSON"
	if p.count == 1 {
		switch p.kind {
		case "missing":
			code = "M_MISSING_PARAM"
		case "value_error":
			code = "M_INVALID_PARAM"
		}
	}
	return clientreq.New(400, code, strings.Join(p.msgs, "; "))
}

func strMap(p *pydErrors, path string, v gjson.Result) {
	if !v.IsObject() {
		p.typ(path, "dictionary")
		return
	}
	v.ForEach(func(k, x gjson.Result) bool {
		if x.Type != gjson.String {
			p.typ(path+"."+k.Str, "string")
		}
		return true
	})
}

func sigMap(p *pydErrors, path string, v gjson.Result) {
	if !v.IsObject() {
		p.typ(path, "dictionary")
		return
	}
	v.ForEach(func(k, x gjson.Result) bool {
		strMap(p, path+"."+k.Str, x)
		return true
	})
}

// keyObjectOrString is StrictStr | KeyObject: a string passes; anything
// else reports both alternatives, the object's under ".KeyObject" (the
// formatter stops following the path there).
func keyObjectOrString(p *pydErrors, path string, v gjson.Result) {
	if v.Type == gjson.String {
		return
	}
	if !v.IsObject() {
		p.typ(path, "string")
		p.add("model_type", "'"+path+"' must be an object")
		return
	}
	// The string alternative is reported only when the object fails too.
	var q pydErrors
	ko := path + ".KeyObject"
	if k := v.Get("key"); !k.Exists() {
		q.missing(ko)
	} else if k.Type != gjson.String {
		q.typ(ko, "string")
	}
	if f := v.Get("fallback"); f.Exists() && f.Type != gjson.True && f.Type != gjson.False && f.Type != gjson.Null {
		q.typ(ko, "boolean")
	}
	if s := v.Get("signatures"); !s.Exists() {
		q.missing(ko)
	} else if !s.IsObject() {
		q.typ(ko, "dictionary")
	} else {
		s.ForEach(func(_, inner gjson.Result) bool {
			if !inner.IsObject() {
				q.typ(ko, "dictionary")
				return true
			}
			inner.ForEach(func(_, x gjson.Result) bool {
				if x.Type != gjson.String {
					q.typ(ko, "string")
				}
				return true
			})
			return true
		})
	}
	if q.count == 0 {
		return
	}
	p.typ(path+".str", "string")
	for _, m := range q.msgs {
		p.add("type", m)
	}
	p.count += q.count - len(q.msgs)
}

// errTypeError is a TypeError a before-validator raises: not a
// ValidationError, so Synapse answers 500.
var errTypeError = errors.New("devicelists: TypeError in a key upload validator")

// validateKeyUpload is validate_json_object(body, KeyUploadRequestBody):
// the fields in their order (device_keys, fallback_keys, one_time_keys);
// a before-validator's SynapseError or TypeError ends it at once.
func validateKeyUpload(body gjson.Result) (*clientreq.Error, error) {
	var p pydErrors
	if dk := body.Get("device_keys"); dk.Exists() {
		switch {
		case dk.Type == gjson.Null:
			p.add("value_error", "device_keys must not be null")
		case !dk.IsObject():
			p.add("model_type", "'.device_keys' must be an object")
		default:
			if a := dk.Get("algorithms"); !a.Exists() {
				p.missing(".device_keys.algorithms")
			} else if !a.IsArray() {
				p.typ(".device_keys.algorithms", "list")
			} else {
				for i, x := range a.Array() {
					if x.Type != gjson.String {
						p.typ(fmt.Sprintf(".device_keys.algorithms[%d]", i), "string")
					}
				}
			}
			for _, f := range []string{"device_id"} {
				if x := dk.Get(f); !x.Exists() {
					p.missing(".device_keys." + f)
				} else if x.Type != gjson.String {
					p.typ(".device_keys."+f, "string")
				}
			}
			if k := dk.Get("keys"); !k.Exists() {
				p.missing(".device_keys.keys")
			} else {
				strMap(&p, ".device_keys.keys", k)
			}
			if s := dk.Get("signatures"); !s.Exists() {
				p.missing(".device_keys.signatures")
			} else {
				sigMap(&p, ".device_keys.signatures", s)
			}
			if x := dk.Get("user_id"); !x.Exists() {
				p.missing(".device_keys.user_id")
			} else if x.Type != gjson.String {
				p.typ(".device_keys.user_id", "string")
			}
		}
	}
	for _, field := range []string{"fallback_keys", "one_time_keys"} {
		v := body.Get(field)
		if !v.Exists() || v.Type == gjson.Null {
			continue
		}
		if !v.IsObject() {
			return nil, errTypeError
		}
		var bad *clientreq.Error
		v.ForEach(func(k, _ gjson.Result) bool {
			if len(strings.Split(k.Str, ":")) != 2 {
				bad = clientreq.New(400, "M_BAD_JSON", "Invalid "+field+" key "+pyjson.ReprString(k.Str)+`. Expected "<algorithm>:<device_id>".`)
				return false
			}
			return true
		})
		if bad != nil {
			return bad, nil
		}
		v.ForEach(func(k, x gjson.Result) bool {
			keyObjectOrString(&p, "."+field+"."+k.Str, x)
			return true
		})
	}
	return p.err(), nil
}

// keysUploadPath is client_patterns("/keys/upload$").
func keysUploadPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/_matrix/client/")
	if !ok {
		return false
	}
	version, rest, _ := strings.Cut(rest, "/")
	return (version == "r0" || version == "v3" || version == "unstable") && rest == "keys/upload"
}

// serveKeysUpload is KeyUploadServlet.on_POST.
func (a *App) serveKeysUpload(ctx context.Context, r *http.Request, v clientauth.Verdict) ([]byte, *clientreq.Error) {
	raw, _, cerr := clientreq.ReadObjectRaw(r.Body)
	if cerr != nil {
		return nil, cerr
	}
	raw, err := pyjson.Dedupe(raw)
	if err != nil {
		return nil, clientreq.Internal
	}
	body := gjson.ParseBytes(raw)
	cerr, err = validateKeyUpload(body)
	if err != nil {
		return nil, a.internal(err)
	}
	if cerr != nil {
		return nil, cerr
	}
	if v.DeviceID == "" {
		return nil, clientreq.New(400, "M_UNKNOWN", "To upload keys, you must pass device_id when authenticating")
	}
	if dk := body.Get("device_keys"); dk.Exists() {
		if dk.Get("user_id").Str != v.UserID {
			return nil, clientreq.New(400, "M_BAD_JSON", "Provided `user_id` in `device_keys` does not match that of the authenticated user")
		}
		if dk.Get("device_id").Str != v.DeviceID {
			return nil, clientreq.New(400, "M_BAD_JSON", "Provided `device_id` in `device_keys` does not match that of the authenticated user device")
		}
	}
	res, err := a.uploadKeys(ctx, v.UserID, v.DeviceID, body)
	if err != nil {
		var se *clientreq.Error
		if errors.As(err, &se) {
			return nil, se
		}
		return nil, a.internal(err)
	}
	return res, nil
}

// uploadKeys is upload_keys_for_user.
func (a *App) uploadKeys(ctx context.Context, userID, deviceID string, body gjson.Result) ([]byte, error) {
	now := a.Now().UnixMilli()
	if dk := body.Get("device_keys"); clientTruthy(dk) {
		canon, err := pyjson.Canonical([]byte(dk.Raw))
		if err != nil {
			return nil, err
		}
		changed, err := a.KStore.SetDeviceKeys(ctx, userID, deviceID, now, canon)
		if err != nil {
			return nil, err
		}
		if changed {
			if err := a.NotifyDeviceUpdate(ctx, userID, []string{deviceID}); err != nil {
				return nil, err
			}
		}
		if err := a.checkDeviceRegistered(ctx, userID, deviceID); err != nil {
			return nil, err
		}
	}
	if otk := body.Get("one_time_keys"); clientTruthy(otk) {
		if err := a.uploadOneTimeKeys(ctx, userID, deviceID, now, otk); err != nil {
			return nil, err
		}
	}
	if fb := body.Get("fallback_keys"); clientTruthy(fb) {
		var keys []member3
		var ferr error
		fb.ForEach(func(k, x gjson.Result) bool {
			alg, id, _ := strings.Cut(k.Str, ":")
			canon, err := pyjson.Canonical([]byte(x.Raw))
			if err != nil {
				ferr = err
				return false
			}
			enc, err := pyjson.Encode([]byte(x.Raw))
			if err != nil {
				ferr = err
				return false
			}
			keys = append(keys, member3{alg, id, canon, enc})
			return true
		})
		if ferr != nil {
			return nil, ferr
		}
		if err := a.KStore.SetFallbackKeys(ctx, userID, deviceID, keys); err != nil {
			return nil, err
		}
		a.invalidate("get_e2e_unused_fallback_key_types", userID, deviceID)
	}
	counts, err := a.KStore.CountOneTimeKeys(ctx, userID, deviceID)
	if err != nil {
		return nil, err
	}
	return countsJSON(counts), nil
}

// countsJSON is {"one_time_key_counts": {...}}: signed_curve25519 at 0
// first, then the table's counts (an algorithm already there keeps its
// place).
func countsJSON(counts [][2]any) []byte {
	order := []string{signedCurve25519}
	vals := map[string]int64{signedCurve25519: 0}
	for _, c := range counts {
		alg := c[0].(string)
		if _, ok := vals[alg]; !ok {
			order = append(order, alg)
		}
		vals[alg] = c[1].(int64)
	}
	var b strings.Builder
	b.WriteString(`{"one_time_key_counts":{`)
	for i, alg := range order {
		if i > 0 {
			b.WriteByte(',')
		}
		k, _ := json.Marshal(alg)
		b.Write(k)
		fmt.Fprintf(&b, ":%d", vals[alg])
	}
	b.WriteString(`}}`)
	enc, _ := pyjson.Encode([]byte(b.String()))
	return []byte(enc)
}

// uploadOneTimeKeys is _upload_one_time_keys_for_user, under the worker
// lock of (user, device).
func (a *App) uploadOneTimeKeys(ctx context.Context, userID, deviceID string, now int64, otk gjson.Result) error {
	if a.Locks == nil {
		return errors.New("devicelists: no lock store for one-time keys")
	}
	lctx, cancel := context.WithTimeout(ctx, time.Minute)
	release, err := a.Locks.Acquire(lctx, oneTimeKeyUploadLock, userID+"_"+deviceID)
	cancel()
	if err != nil {
		return err
	}
	defer release()
	type item struct {
		alg, id string
		key     gjson.Result
	}
	var items []item
	var ids []string
	otk.ForEach(func(k, x gjson.Result) bool {
		alg, id, _ := strings.Cut(k.Str, ":")
		items = append(items, item{alg, id, x})
		ids = append(ids, id)
		return true
	})
	existing, err := a.KStore.OneTimeKeys(ctx, userID, deviceID, ids)
	if err != nil {
		return err
	}
	var add [][3]string
	perAlg := map[string]int{}
	var algOrder []string
	for _, it := range items {
		if ex, ok := existing[[2]string{it.alg, it.id}]; ok && ex != "" {
			if !oneTimeKeysMatch(ex, it.key) {
				r, err := pyjson.Repr([]byte(it.key.Raw))
				if err != nil {
					return err
				}
				return clientreq.New(400, "M_UNKNOWN", fmt.Sprintf("One time key %s:%s already exists. Old key: %s; new key: %s", it.alg, it.id, ex, r))
			}
			continue
		}
		canon, err := pyjson.Canonical([]byte(it.key.Raw))
		if err != nil {
			return err
		}
		// encode_canonical_json(key).decode("ascii"): canonical JSON is
		// UTF-8, so a key with anything past ASCII is a UnicodeDecodeError,
		// a 500, and nothing is inserted.
		if !isASCII(canon) {
			return errNonASCIIOneTimeKey
		}
		add = append(add, [3]string{it.alg, it.id, canon})
		if perAlg[it.alg] == 0 {
			algOrder = append(algOrder, it.alg)
		}
		perAlg[it.alg]++
	}
	counts, err := a.KStore.CountOneTimeKeys(ctx, userID, deviceID)
	if err != nil {
		return err
	}
	have := map[string]int64{}
	for _, c := range counts {
		have[c[0].(string)] = c[1].(int64)
	}
	for _, alg := range algOrder {
		total := have[alg] + int64(perAlg[alg])
		if total > maxOneTimeKeysPerAlgorithm {
			return clientreq.New(400, "M_TOO_LARGE", fmt.Sprintf("Uploading %d more %s one-time keys would leave the device holding %d, over the limit of %d",
				perAlg[alg], alg, total, maxOneTimeKeysPerAlgorithm))
		}
	}
	return a.Caches.Do(func() ([]cachestream.Row, error) {
		return a.KStore.AddOneTimeKeys(ctx, a.Instance, userID, deviceID, now, add)
	})
}

var errNonASCIIOneTimeKey = errors.New("devicelists: a one-time key past ASCII (Synapse's UnicodeDecodeError)")

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// oneTimeKeysMatch is _one_time_keys_match: two objects compared without
// their signatures; anything else exactly.
func oneTimeKeysMatch(old string, newKey gjson.Result) bool {
	var o, n any
	if json.Unmarshal([]byte(old), &o) != nil || json.Unmarshal([]byte(newKey.Raw), &n) != nil {
		return false
	}
	om, ok1 := o.(map[string]any)
	nm, ok2 := n.(map[string]any)
	if !ok1 || !ok2 {
		return reflect.DeepEqual(o, n)
	}
	delete(om, "signatures")
	delete(nm, "signatures")
	return reflect.DeepEqual(om, nm)
}

// checkDeviceRegistered is check_device_registered with a device ID: the
// device created if it does not exist, and then notified (unless the user
// is in dont_notify_new_devices_for).
func (a *App) checkDeviceRegistered(ctx context.Context, userID, deviceID string) error {
	created, err := a.CStore.StoreDevice(ctx, userID, deviceID, nil)
	if err != nil {
		return clientreq.New(500, "M_UNKNOWN", "Problem storing device.")
	}
	a.invalidateDevice(userID, deviceID)
	if !created {
		hidden, err := a.CStore.DeviceHidden(ctx, userID, deviceID)
		if err != nil {
			return err
		}
		if hidden {
			return clientreq.New(400, "M_FORBIDDEN", "The device ID is in use")
		}
		return nil
	}
	for _, u := range a.Synapse().DontNotifyNewDevicesFor {
		if u == userID {
			return nil
		}
	}
	return a.NotifyDeviceUpdate(ctx, userID, []string{deviceID})
}

// invalidate is invalidate_cache_and_stream(cacheFunc, (user, device)).
func (a *App) invalidate(cacheFunc, userID, deviceID string) {
	if a.Caches == nil || a.CachesInsert == nil {
		return
	}
	if err := a.Caches.Do(func() ([]cachestream.Row, error) {
		return a.CachesInsert(context.Background(), cacheFunc, [][]string{{userID, deviceID}})
	}); err != nil {
		a.Log.Warn().Err(err).Str("cache", cacheFunc).Msg("devices: invalidation")
	}
}

// Store.

// SetDeviceKeys is _set_e2e_device_keys_txn: stored (and true) unless the
// canonical JSON is already there.
func (d *DB) SetDeviceKeys(ctx context.Context, userID, deviceID string, nowMS int64, canonical string) (bool, error) {
	var changed bool
	err := d.runTxn(ctx, func(tx pgx.Tx) error {
		changed = false
		var old *string
		err := tx.QueryRow(ctx, `SELECT key_json FROM e2e_device_keys_json WHERE user_id = $1 AND device_id = $2`, userID, deviceID).Scan(&old)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if old != nil && *old == canonical {
			return nil
		}
		changed = true
		_, err = tx.Exec(ctx, `INSERT INTO e2e_device_keys_json (user_id, device_id, ts_added_ms, key_json) VALUES ($1, $2, $3, $4)
			ON CONFLICT (user_id, device_id) DO UPDATE SET ts_added_ms = EXCLUDED.ts_added_ms, key_json = EXCLUDED.key_json`,
			userID, deviceID, nowMS, canonical)
		return err
	})
	return changed, err
}

// OneTimeKeys is get_e2e_one_time_keys: (algorithm, key_id) to key_json,
// for the given key IDs.
func (d *DB) OneTimeKeys(ctx context.Context, userID, deviceID string, keyIDs []string) (map[[2]string]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT algorithm, key_id, key_json FROM e2e_one_time_keys_json
		WHERE user_id = $1 AND device_id = $2 AND key_id = ANY($3)`, userID, deviceID, keyIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[[2]string]string{}
	for rows.Next() {
		var alg, id, js string
		if err := rows.Scan(&alg, &id, &js); err != nil {
			return nil, err
		}
		out[[2]string{alg, id}] = js
	}
	return out, rows.Err()
}

// CountOneTimeKeys is count_e2e_one_time_keys' query: (algorithm, count).
func (d *DB) CountOneTimeKeys(ctx context.Context, userID, deviceID string) ([][2]any, error) {
	rows, err := d.pool.Query(ctx, `SELECT algorithm, COUNT(key_id) FROM e2e_one_time_keys_json
		WHERE user_id = $1 AND device_id = $2 GROUP BY algorithm`, userID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]any
	for rows.Next() {
		var alg string
		var n int64
		if err := rows.Scan(&alg, &n); err != nil {
			return nil, err
		}
		out = append(out, [2]any{alg, n})
	}
	return out, rows.Err()
}

// AddOneTimeKeys is _add_e2e_one_time_keys_txn: the keys, and the
// count_e2e_one_time_keys invalidation in the same transaction.
func (d *DB) AddOneTimeKeys(ctx context.Context, instance, userID, deviceID string, nowMS int64, keys [][3]string) ([]cachestream.Row, error) {
	var rows []cachestream.Row
	err := d.runTxn(ctx, func(tx pgx.Tx) error {
		for _, k := range keys {
			if _, err := tx.Exec(ctx, `INSERT INTO e2e_one_time_keys_json (user_id, device_id, algorithm, key_id, ts_added_ms, key_json)
				VALUES ($1, $2, $3, $4, $5, $6)`, userID, deviceID, k[0], k[1], nowMS, k[2]); err != nil {
				return err
			}
		}
		var err error
		rows, err = cachestream.Insert(ctx, tx, instance, "count_e2e_one_time_keys", [][]string{{userID, deviceID}}, nowMS)
		return err
	})
	return rows, err
}

// SetFallbackKeys is _set_e2e_fallback_keys_txn: a key whose canonical
// JSON differs from the stored one is upserted, unused.
func (d *DB) SetFallbackKeys(ctx context.Context, userID, deviceID string, keys []member3) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		for _, k := range keys {
			var old *string
			err := tx.QueryRow(ctx, `SELECT key_json FROM e2e_fallback_keys_json WHERE user_id = $1 AND device_id = $2 AND algorithm = $3`,
				userID, deviceID, k.algorithm).Scan(&old)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			if old != nil && *old == k.canonical {
				continue
			}
			if _, err := tx.Exec(ctx, `INSERT INTO e2e_fallback_keys_json (user_id, device_id, algorithm, key_id, key_json, used)
				VALUES ($1, $2, $3, $4, $5, false)
				ON CONFLICT (user_id, device_id, algorithm) DO UPDATE SET key_id = EXCLUDED.key_id, key_json = EXCLUDED.key_json, used = EXCLUDED.used`,
				userID, deviceID, k.algorithm, k.keyID, k.encoded); err != nil {
				return err
			}
		}
		return nil
	})
}
