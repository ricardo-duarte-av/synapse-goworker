package devicelists

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/e2ekeys"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/userid"
)

// SignatureItem is SignatureListItem: a signature to store.
type SignatureItem struct {
	SigningKeyID, TargetUserID, TargetDeviceID string
	// Signature is the value as the client sent it, as Postgres would
	// store it (see sigText).
	Signature string
}

// SignaturesStore is what /keys/signatures/upload writes (DB).
type SignaturesStore interface {
	StoreSignatures(ctx context.Context, instance, userID string, items []SignatureItem, nowMS int64) ([]cachestream.Row, error)
}

// signaturesUploadPath is client_patterns("/keys/signatures/upload$").
func signaturesUploadPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/_matrix/client/")
	if !ok {
		return false
	}
	version, rest, _ := strings.Cut(rest, "/")
	return (version == "r0" || version == "v3" || version == "unstable") && rest == "keys/signatures/upload"
}

// sigError is a SynapseError raised inside the processing: a failure in
// the answer, or (abort) the whole request's error.
type sigError struct {
	status  int
	errcode string
	msg     string
}

func (e *sigError) Error() string { return fmt.Sprintf("%d: %s", e.status, e.msg) }

func notFound(msg string) *sigError { return &sigError{404, "M_NOT_FOUND", msg} }

var (
	errInvalidParam     = &sigError{400, "M_INVALID_PARAM", "Invalid parameter"}
	errInvalidSignature = &sigError{400, "M_INVALID_SIGNATURE", "Invalid signature"}
	errKeyMismatch      = &sigError{400, "M_UNKNOWN", "Key does not match"}
)

// failure is _exception_to_failure of a SynapseError.
func (e *sigError) failure() string {
	return `{"status":` + strconv.Itoa(e.status) + `,"errcode":` + jsonStr(e.errcode) + `,"message":` + jsonStr(e.Error()) + `}`
}

func jsonStr(s string) string {
	b, _ := sjson.Set(`{}`, "s", s)
	return gjson.Get(b, "s").Raw
}

// omap is a Python dict of str: insertion order, assignment keeping a
// key's place.
type omap[V any] struct {
	keys []string
	vals map[string]V
}

func (m *omap[V]) set(k string, v V) {
	if m.vals == nil {
		m.vals = map[string]V{}
	}
	if _, ok := m.vals[k]; !ok {
		m.keys = append(m.keys, k)
	}
	m.vals[k] = v
}

func (m *omap[V]) get(k string) (V, bool) {
	v, ok := m.vals[k]
	return v, ok
}

// failures is {user: {device: failure}}.
type failures struct{ omap[*omap[string]] }

// fromKeys is failures[user] = dict.fromkeys(keys, failure).
func (f *failures) fromKeys(user string, keys []string, e *sigError) {
	m := &omap[string]{}
	for _, k := range keys {
		m.set(k, e.failure())
	}
	f.set(user, m)
}

// setOne is failures.setdefault(user, {})[device] = failure.
func (f *failures) setOne(user, device string, e *sigError) {
	m, ok := f.get(user)
	if !ok {
		m = &omap[string]{}
		f.set(user, m)
	}
	m.set(device, e.failure())
}

func (f *failures) update(o *failures) {
	for _, k := range o.keys {
		f.set(k, o.vals[k])
	}
}

func (f *failures) json() string {
	var b strings.Builder
	b.WriteString(`{"failures":{`)
	for i, u := range f.keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(jsonStr(u))
		b.WriteString(":{")
		m := f.vals[u]
		for j, d := range m.keys {
			if j > 0 {
				b.WriteByte(',')
			}
			b.WriteString(jsonStr(d))
			b.WriteByte(':')
			b.WriteString(m.vals[d])
		}
		b.WriteByte('}')
	}
	b.WriteString("}}")
	return b.String()
}

func keysOf(v gjson.Result) []string {
	var ks []string
	v.ForEach(func(k, _ gjson.Result) bool { ks = append(ks, k.Str); return true })
	return ks
}

// verifyKey is a signedjson VerifyKey: "ed25519:" + version.
type verifyKey struct {
	keyID, version string
	pub            ed25519.PublicKey
}

// errValue is a ValueError from get_verify_key_from_cross_signing_key
// (what the remote path skips); errPython the rest.
var errValue = errors.New("devicelists: invalid cross-signing key (ValueError)")

// crossSigningVerifyKey is get_verify_key_from_cross_signing_key.
func crossSigningVerifyKey(key gjson.Result) (verifyKey, error) {
	keys, ok, err := pyIndex(key, "keys")
	if err != nil {
		return verifyKey{}, err
	}
	if !ok {
		return verifyKey{}, errValue
	}
	if !keys.IsObject() {
		return verifyKey{}, errPython
	}
	var id string
	var data gjson.Result
	n := 0
	keys.ForEach(func(k, v gjson.Result) bool {
		if n == 0 {
			id, data = k.Str, v
		}
		n++
		return true
	})
	if n != 1 {
		return verifyKey{}, errValue
	}
	return decodeVerifyKey(id, data)
}

// decodeVerifyKey is decode_verify_key_bytes(key_id, decode_base64(data)).
func decodeVerifyKey(id string, data gjson.Result) (verifyKey, error) {
	if data.Type != gjson.String {
		return verifyKey{}, errPython
	}
	raw, err := pyB64Decode(data.Str)
	if err != nil {
		return verifyKey{}, errValue
	}
	version, ok := strings.CutPrefix(id, "ed25519:")
	if !ok || len(raw) != ed25519.PublicKeySize {
		return verifyKey{}, errValue
	}
	return verifyKey{keyID: "ed25519:" + version, version: version, pub: ed25519.PublicKey(raw)}, nil
}

// verifySigned is signedjson's verify_signed_json: false is its
// SignatureVerifyException; an error is a Python error before it.
// Go's ed25519 and libsodium differ only on adversarial points (small
// order keys), which only the signer's own keys could carry.
func verifySigned(obj gjson.Result, name string, vk verifyKey) (bool, error) {
	sigs, ok, err := pyIndex(obj, "signatures")
	if err != nil || !ok {
		return false, err
	}
	byName, ok, err := pyIndex(sigs, name)
	if err != nil || !ok {
		return false, err
	}
	sig, ok, err := pyIndex(byName, vk.keyID)
	if err != nil || !ok {
		return false, err
	}
	if sig.Type != gjson.String {
		return false, nil
	}
	raw, err := pyB64Decode(sig.Str)
	if err != nil {
		return false, nil
	}
	msg, err := sjson.Delete(obj.Raw, "signatures")
	if err != nil {
		return false, err
	}
	if msg, err = sjson.Delete(msg, "unsigned"); err != nil {
		return false, err
	}
	canon, err := pyjson.Canonical([]byte(msg))
	if err != nil {
		return false, err
	}
	return len(raw) == ed25519.SignatureSize && ed25519.Verify(vk.pub, []byte(canon), raw), nil
}

// checkDeviceSignature is _check_device_signature.
func checkDeviceSignature(userID string, vk verifyKey, signed, stored gjson.Result) error {
	if !pyDictEqual(signed, stored, []string{"signatures", "unsigned"}) {
		return errKeyMismatch
	}
	ok, err := verifySigned(signed, userID, vk)
	if err != nil {
		return err
	}
	if !ok {
		return errInvalidSignature
	}
	return nil
}

// sigText is the signature as psycopg2 adapts it and Postgres assigns it
// to a text column: a string as is, an int's digits, a bool as "true" or
// "false". Synapse stores a float, list or empty dict too (each as
// Postgres renders it) and fails on null and a non-empty dict; only a
// master key signed under an algorithm other than ed25519 reaches here
// unverified, and those are refused as a 500.
func sigText(v gjson.Result) (string, error) {
	switch v.Type {
	case gjson.String:
		return v.Str, nil
	case gjson.True:
		return "true", nil
	case gjson.False:
		return "false", nil
	case gjson.Number:
		if !strings.ContainsAny(v.Raw, ".eE") {
			return v.Raw, nil
		}
	}
	return "", errPython
}

// serveSignaturesUpload is SignaturesUploadServlet.on_POST.
func (a *App) serveSignaturesUpload(ctx context.Context, r *http.Request, v clientauth.Verdict) ([]byte, *clientreq.Error) {
	raw, _, cerr := clientreq.ReadObjectRaw(r.Body)
	if cerr != nil {
		return nil, cerr
	}
	raw, err := pyjson.Dedupe(raw)
	if err != nil {
		return nil, clientreq.Internal
	}
	out, err := a.uploadSignatures(ctx, v.UserID, gjson.ParseBytes(raw))
	if err != nil {
		var se *sigError
		if errors.As(err, &se) {
			return nil, clientreq.New(se.status, se.errcode, se.msg)
		}
		return nil, a.internal(err)
	}
	enc, err := pyjson.Encode([]byte(out))
	if err != nil {
		return nil, a.internal(err)
	}
	return []byte(enc), nil
}

// uploadSignatures is upload_signatures_for_device_keys.
func (a *App) uploadSignatures(ctx context.Context, userID string, body gjson.Result) (string, error) {
	var all failures
	selfSigs := pyField(body, userID)
	selfList, selfFailures, err := a.processSelfSignatures(ctx, userID, selfSigs)
	if err != nil {
		return "", err
	}
	all.update(selfFailures)
	var others []gjson.Result
	var otherUsers []string
	body.ForEach(func(k, v gjson.Result) bool {
		if k.Str != userID {
			otherUsers = append(otherUsers, k.Str)
			others = append(others, v)
		}
		return true
	})
	otherList, otherFailures, err := a.processOtherSignatures(ctx, userID, otherUsers, others)
	if err != nil {
		return "", err
	}
	all.update(otherFailures)

	items := append(selfList, otherList...)
	if err := a.Caches.Do(func() ([]cachestream.Row, error) {
		return a.SStore.StoreSignatures(ctx, a.Instance, userID, items, a.Now().UnixMilli())
	}); err != nil {
		var perr *cachestream.PublishError
		if !errors.As(err, &perr) {
			return "", err
		}
		a.Log.Warn().Err(err).Msg("signatures: stored, caches rows not published")
	}
	var selfDevices []string
	for _, it := range selfList {
		selfDevices = append(selfDevices, it.TargetDeviceID)
	}
	if len(selfDevices) > 0 {
		if err := a.NotifyDeviceUpdate(ctx, userID, selfDevices); err != nil {
			return "", err
		}
	}
	if len(otherList) > 0 {
		var signed []string
		for _, it := range otherList {
			signed = append(signed, it.TargetUserID)
		}
		list, _ := sjson.Set(`{}`, "l", signed)
		enc, err := pyjson.Encode([]byte(gjson.Get(list, "l").Raw))
		if err != nil {
			return "", err
		}
		if err := a.NotifyUserSignatureUpdate(ctx, userID, enc); err != nil {
			return "", err
		}
	}
	return all.json(), nil
}

// ownCrossSigningKey is _get_e2e_cross_signing_verify_key for one of our
// own user's keys, without signatures.
func (a *App) ownCrossSigningKey(ctx context.Context, userID, keyType string) (gjson.Result, verifyKey, error) {
	keys, err := e2ekeys.CrossSigningKeys(ctx, a.E2E, []string{userID})
	if err != nil {
		return gjson.Result{}, verifyKey{}, err
	}
	k, ok := keys[userID][keyType]
	if !ok {
		return gjson.Result{}, verifyKey{}, notFound("No " + keyType + " key found for " + userID)
	}
	key := gjson.ParseBytes(k)
	vk, err := crossSigningVerifyKey(key)
	if err != nil {
		return gjson.Result{}, verifyKey{}, errPython
	}
	return key, vk, nil
}

// processSelfSignatures is _process_self_signatures.
func (a *App) processSelfSignatures(ctx context.Context, userID string, sigs gjson.Result) ([]SignatureItem, *failures, error) {
	f := &failures{}
	if !clientTruthy(sigs) {
		return nil, f, nil
	}
	if !sigs.IsObject() {
		return nil, nil, errInvalidParam
	}
	_, ssKey, err := a.ownCrossSigningKey(ctx, userID, "self_signing")
	var master gjson.Result
	var masterKey verifyKey
	if err == nil {
		master, masterKey, err = a.ownCrossSigningKey(ctx, userID, "master")
	}
	var devices map[string]gjson.Result
	if err == nil {
		var local map[string]map[string][]byte
		local, err = localDevices(ctx, a.E2E, userID)
		if err == nil {
			if len(local[userID]) == 0 {
				err = notFound("No device keys found")
			}
			devices = map[string]gjson.Result{}
			for d, k := range local[userID] {
				devices[d] = gjson.ParseBytes(k)
			}
		}
	}
	var se *sigError
	if errors.As(err, &se) {
		f.fromKeys(userID, keysOf(sigs), se)
		return nil, f, nil
	}
	if err != nil {
		return nil, nil, err
	}

	var list []SignatureItem
	var perr error
	sigs.ForEach(func(k, device gjson.Result) bool {
		deviceID := k.Str
		if !device.IsObject() {
			perr = errInvalidParam
			return false
		}
		items, err := a.selfSignature(userID, deviceID, device, ssKey, master, masterKey, devices)
		if errors.As(err, &se) {
			f.setOne(userID, deviceID, se)
			return true
		}
		if err != nil {
			perr = err
			return false
		}
		list = append(list, items...)
		return true
	})
	if perr != nil {
		return nil, nil, perr
	}
	return list, f, nil
}

func (a *App) selfSignature(userID, deviceID string, device gjson.Result, ssKey verifyKey, master gjson.Result, masterKey verifyKey, devices map[string]gjson.Result) ([]SignatureItem, error) {
	sigs := pyField(device, "signatures")
	if !sigs.Exists() {
		return nil, errInvalidSignature
	}
	in, err := pyIn(sigs, userID)
	if err != nil {
		return nil, err
	}
	if !in {
		return nil, errInvalidSignature
	}
	if deviceID == masterKey.version {
		return checkMasterKeySignature(userID, deviceID, device, master, devices)
	}
	mine, _, err := pyIndex(sigs, userID)
	if err != nil {
		return nil, err
	}
	if in, err = pyIn(mine, ssKey.keyID); err != nil {
		return nil, err
	} else if !in {
		return nil, errInvalidSignature
	}
	stored, ok := devices[deviceID]
	if !ok {
		return nil, notFound("Unknown device")
	}
	if pyField(pyField(pyField(stored, "signatures"), userID), ssKey.keyID).Exists() {
		return nil, nil
	}
	if err := checkDeviceSignature(userID, ssKey, device, stored); err != nil {
		return nil, err
	}
	return []SignatureItem{{ssKey.keyID, userID, deviceID, pyField(mine, ssKey.keyID).Str}}, nil
}

// checkMasterKeySignature is _check_master_key_signature.
func checkMasterKeySignature(userID, masterKeyID string, signed, stored gjson.Result, devices map[string]gjson.Result) ([]SignatureItem, error) {
	sigs, _, err := pyIndex(pyField(signed, "signatures"), userID)
	if err != nil {
		return nil, err
	}
	if !sigs.IsObject() {
		return nil, errPython // .items()
	}
	var list []SignatureItem
	var ferr error
	sigs.ForEach(func(k, sig gjson.Result) bool {
		alg, signingDevice, ok := strings.Cut(k.Str, ":")
		if !ok {
			ferr = errPython
			return false
		}
		if alg == "ed25519" {
			dev, known := devices[signingDevice]
			pub := pyField(pyField(dev, "keys"), k.Str)
			if !known || !pub.Exists() {
				ferr = errInvalidSignature
				return false
			}
			vk, err := decodeVerifyKey(k.Str, pub)
			if err != nil {
				ferr = errPython
				return false
			}
			if err := checkDeviceSignature(userID, vk, signed, stored); err != nil {
				ferr = err
				return false
			}
		}
		text, err := sigText(sig)
		if err != nil {
			ferr = err
			return false
		}
		list = append(list, SignatureItem{k.Str, userID, masterKeyID, text})
		return true
	})
	return list, ferr
}

// processOtherSignatures is _process_other_signatures.
func (a *App) processOtherSignatures(ctx context.Context, userID string, users []string, maps []gjson.Result) ([]SignatureItem, *failures, error) {
	f := &failures{}
	if len(users) == 0 {
		return nil, f, nil
	}
	_, usKey, err := a.ownCrossSigningKey(ctx, userID, "user_signing")
	var se *sigError
	if errors.As(err, &se) {
		for i, u := range users {
			if !maps[i].IsObject() {
				return nil, nil, errPython // .keys()
			}
			f.fromKeys(u, keysOf(maps[i]), se)
		}
		return nil, f, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var list []SignatureItem
	for i, target := range users {
		devicemap := maps[i]
		if !devicemap.IsObject() {
			return nil, nil, errInvalidParam
		}
		bad := false
		devicemap.ForEach(func(_, d gjson.Result) bool {
			bad = !d.IsObject()
			return !bad
		})
		if bad {
			return nil, nil, errInvalidParam
		}
		item, deviceID, err := a.otherSignature(ctx, userID, target, devicemap, usKey, f)
		if errors.As(err, &se) {
			if deviceID == "" {
				f.fromKeys(target, keysOf(devicemap), se)
			} else {
				f.setOne(target, deviceID, se)
			}
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		if item != nil {
			list = append(list, *item)
		}
	}
	return list, f, nil
}

// otherSignature is one target of _process_other_signatures; deviceID is
// the device the failure belongs to ("" for all of them).
func (a *App) otherSignature(ctx context.Context, userID, target string, devicemap gjson.Result, usKey verifyKey, f *failures) (*SignatureItem, string, error) {
	master, masterKey, err := a.targetMasterKey(ctx, target, userID)
	if err != nil {
		return nil, "", err
	}
	deviceID := masterKey.version
	key := pyField(devicemap, deviceID)
	if !key.Exists() {
		return nil, "", notFound("Unknown device")
	}
	var others []string
	for _, d := range keysOf(devicemap) {
		if d != deviceID {
			others = append(others, d)
		}
	}
	if len(others) > 0 {
		f.fromKeys(target, others, notFound("Unknown device"))
	}
	if pyField(pyField(pyField(master, "signatures"), userID), usKey.keyID).Exists() {
		return nil, deviceID, nil
	}
	if err := checkDeviceSignature(userID, usKey, key, master); err != nil {
		return nil, deviceID, err
	}
	sig := pyField(pyField(pyField(key, "signatures"), userID), usKey.keyID)
	return &SignatureItem{usKey.keyID, target, deviceID, sig.Str}, deviceID, nil
}

// targetMasterKey is _get_e2e_cross_signing_verify_key(target, "master",
// from): ours or stored, with from's signatures; a remote user's fetched
// over federation when we have none.
func (a *App) targetMasterKey(ctx context.Context, target, from string) (gjson.Result, verifyKey, error) {
	if !strings.HasPrefix(target, "@") {
		return gjson.Result{}, verifyKey{}, &sigError{400, "M_INVALID_PARAM", "Expected UserID string to start with '@'"}
	}
	if !strings.Contains(target[1:], ":") {
		return gjson.Result{}, verifyKey{}, &sigError{400, "M_INVALID_PARAM", "Expected UserID of the form '@localname:domain'"}
	}
	master, _, _, err := e2ekeys.CrossSigning(ctx, a.E2E, []string{target}, from)
	if err != nil {
		return gjson.Result{}, verifyKey{}, err
	}
	if k, ok := master[target]; ok {
		key := gjson.ParseBytes(k)
		vk, err := crossSigningVerifyKey(key)
		if err != nil {
			return gjson.Result{}, verifyKey{}, errPython
		}
		return key, vk, nil
	}
	missing := notFound("No master key found for " + target)
	if a.isMine(target) {
		return gjson.Result{}, verifyKey{}, missing
	}
	key, vk, ok, err := a.remoteCrossSigningKeys(ctx, target, "master")
	if err != nil {
		return gjson.Result{}, verifyKey{}, err
	}
	if !ok {
		return gjson.Result{}, verifyKey{}, missing
	}
	return key, vk, nil
}

// remoteCrossSigningKeys is _retrieve_cross_signing_keys_for_remote_user:
// the master and self-signing keys fetched, each valid one stored, and
// the user's devices notified.
func (a *App) remoteCrossSigningKeys(ctx context.Context, user, want string) (gjson.Result, verifyKey, bool, error) {
	if a.Query == nil {
		return gjson.Result{}, verifyKey{}, false, nil
	}
	ans, err := a.Query(ctx, userid.Domain(user), user)
	if err == nil {
		ans, err = pyjson.Dedupe(ans)
	}
	if err != nil {
		a.Log.Warn().Err(err).Str("user", user).Msg("Unable to query for cross-signing keys")
		return gjson.Result{}, verifyKey{}, false, nil
	}
	res := gjson.ParseBytes(ans)
	var (
		found   gjson.Result
		foundVK verifyKey
		ok      bool
		ids     []string
	)
	for _, typ := range []string{"master", "self_signing"} {
		content := pyField(res, typ+"_key")
		if !clientTruthy(content) {
			continue
		}
		if !content.IsObject() {
			a.Log.Warn().Str("user", user).Str("type", typ).Msg("Invalid cross-signing key retrieved")
			continue
		}
		owner := pyField(content, "user_id")
		if !owner.Exists() || owner.Type != gjson.String || owner.Str != user {
			a.Log.Warn().Str("user", user).Str("type", typ).Msg("Cross-signing key retrieved of another user, or of none")
			continue
		}
		vk, err := crossSigningVerifyKey(content)
		if errors.Is(err, errValue) {
			a.Log.Warn().Str("user", user).Str("type", typ).Msg("Invalid cross-signing key retrieved")
			continue
		}
		if err != nil {
			return gjson.Result{}, verifyKey{}, false, err
		}
		ids = append(ids, vk.version)
		if typ == want {
			found, foundVK, ok = content, vk, true
		}
		enc, err := pyjson.Encode([]byte(content.Raw))
		if err != nil {
			return gjson.Result{}, verifyKey{}, false, err
		}
		if err := a.Caches.Do(func() ([]cachestream.Row, error) {
			return a.RStore.SetCrossSigningKey(ctx, a.Instance, user, typ, enc, a.Now().UnixMilli())
		}); err != nil {
			var perr *cachestream.PublishError
			if !errors.As(err, &perr) {
				return gjson.Result{}, verifyKey{}, false, err
			}
		}
	}
	if len(ids) > 0 {
		if err := a.NotifyDeviceUpdate(ctx, user, ids); err != nil {
			return gjson.Result{}, verifyKey{}, false, err
		}
	}
	return found, foundVK, ok, nil
}

// localDevices is get_e2e_device_keys_for_cs_api([(user, None)]).
func localDevices(ctx context.Context, q e2ekeys.Querier, userID string) (map[string]map[string][]byte, error) {
	got, err := e2ekeys.Local(ctx, q, []string{userID}, nil, true)
	if err != nil {
		return nil, err
	}
	out := map[string]map[string][]byte{}
	for u, devs := range got {
		out[u] = map[string][]byte{}
		for d, k := range devs {
			out[u][d] = k
		}
	}
	return out, nil
}

// StoreSignatures is store_e2e_cross_signing_signatures: the signatures
// upserted, and a _get_e2e_cross_signing_signatures_for_device row for
// each, its one key json.dumps([user, target device]).
func (d *DB) StoreSignatures(ctx context.Context, instance, userID string, items []SignatureItem, nowMS int64) ([]cachestream.Row, error) {
	var rows []cachestream.Row
	err := d.runTxn(ctx, func(tx pgx.Tx) error {
		rows = nil
		if len(items) == 0 {
			return nil
		}
		// The table is in UNIQUE_INDEX_BACKGROUND_UPDATES: until its
		// unique index is built, Synapse emulates the upsert under a lock.
		var pending bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM background_updates WHERE update_name = 'e2e_cross_signing_signatures_add_key_id_to_index')`).Scan(&pending); err != nil {
			return err
		}
		if pending {
			if _, err := tx.Exec(ctx, `LOCK TABLE e2e_cross_signing_signatures in EXCLUSIVE MODE`); err != nil {
				return err
			}
		}
		var keys [][]string
		for _, it := range items {
			if pending {
				tag, err := tx.Exec(ctx, `UPDATE e2e_cross_signing_signatures SET signature = $5
					WHERE user_id = $1 AND key_id = $2 AND target_user_id = $3 AND target_device_id = $4`,
					userID, it.SigningKeyID, it.TargetUserID, it.TargetDeviceID, it.Signature)
				if err != nil {
					return err
				}
				if tag.RowsAffected() == 0 {
					if _, err := tx.Exec(ctx, `INSERT INTO e2e_cross_signing_signatures (user_id, key_id, target_user_id, target_device_id, signature)
						VALUES ($1, $2, $3, $4, $5)`, userID, it.SigningKeyID, it.TargetUserID, it.TargetDeviceID, it.Signature); err != nil {
						return err
					}
				}
			} else if _, err := tx.Exec(ctx, `INSERT INTO e2e_cross_signing_signatures (user_id, key_id, target_user_id, target_device_id, signature)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (user_id, key_id, target_user_id, target_device_id) DO UPDATE SET signature = EXCLUDED.signature`,
				userID, it.SigningKeyID, it.TargetUserID, it.TargetDeviceID, it.Signature); err != nil {
				return err
			}
			keys = append(keys, []string{"[" + pyDumpsStr(userID) + ", " + pyDumpsStr(it.TargetDeviceID) + "]"})
		}
		var err error
		rows, err = cachestream.Insert(ctx, tx, instance, "_get_e2e_cross_signing_signatures_for_device", keys, nowMS)
		return err
	})
	return rows, err
}

// pyDumpsStr is json.dumps of a str (ensure_ascii).
func pyDumpsStr(s string) string {
	enc, _ := pyjson.Encode([]byte(jsonStr(s)))
	return enc
}
