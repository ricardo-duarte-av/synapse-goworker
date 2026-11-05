package devicelists

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/e2ekeys"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
)

// SigningStore is what /keys/device_signing/upload reads and writes (DB).
type SigningStore interface {
	MasterKeyUpdatableBefore(ctx context.Context, userID string) (exists bool, ts *int64, err error)
	SetLocalCrossSigningKey(ctx context.Context, instance, userID, keyType, key, pubkey string, nowMS int64) ([]cachestream.Row, error)
}

// deviceSigningPath is client_patterns("/keys/device_signing/upload$",
// releases=("v3",)): v3 and unstable.
func deviceSigningPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/_matrix/client/")
	if !ok {
		return false
	}
	version, rest, _ := strings.Cut(rest, "/")
	return (version == "v3" || version == "unstable") && rest == "keys/device_signing/upload"
}

// validateAuthField is AuthenticationData's part of a body's validation.
func validateAuthField(p *pydErrors, body gjson.Result) {
	auth := pyField(body, "auth")
	if !auth.Exists() || auth.Type == gjson.Null {
		return
	}
	if !auth.IsObject() {
		p.add("model_type", "'.auth' must be an object")
		return
	}
	for _, f := range []string{"session", "type"} {
		if x := pyField(auth, f); x.Exists() && x.Type != gjson.Null && x.Type != gjson.String {
			p.typ(".auth."+f, "string")
		}
	}
}

// signingKeyFields is has_different_keys' map, in its order.
var signingKeyFields = [][2]string{{"master_key", "master"}, {"self_signing_key", "self_signing"}, {"user_signing_key", "user_signing"}}

// serveDeviceSigning is SigningKeyUploadServlet.on_POST.
func (a *App) serveDeviceSigning(ctx context.Context, w http.ResponseWriter, r *http.Request, v clientauth.Verdict, count func(string)) {
	fail := func(e *clientreq.Error) {
		count("refused")
		clientreq.Write(w, e, nil)
	}
	raw, _, cerr := clientreq.ReadObjectRaw(r.Body)
	if cerr != nil {
		fail(cerr)
		return
	}
	raw, err := pyjson.Dedupe(raw)
	if err != nil {
		fail(a.internal(err))
		return
	}
	body := gjson.ParseBytes(raw)
	var p pydErrors
	validateAuthField(&p, body)
	if cerr := p.err(); cerr != nil {
		fail(cerr)
		return
	}
	exists, _, err := a.GStore.MasterKeyUpdatableBefore(ctx, v.UserID)
	if err != nil {
		fail(a.internal(err))
		return
	}
	stored, err := e2ekeys.CrossSigningKeys(ctx, a.E2E, []string{v.UserID})
	if err != nil {
		fail(a.internal(err))
		return
	}
	different := false
	for _, f := range signingKeyFields {
		sent := pyField(body, f[0])
		if !sent.Exists() {
			continue
		}
		existing := gjson.Parse("null")
		if k, ok := stored[v.UserID][f[1]]; ok {
			existing = gjson.ParseBytes(k)
		}
		if !pyEqual(existing, sent) {
			different = true
			break
		}
	}
	if !different {
		count("ok")
		clientreq.Write(w, nil, nil)
		return
	}
	// MSC3967: the first upload needs no UIA; MSC4190: nor an appservice's.
	if exists && v.AppServiceID == "" {
		err := a.validateUserViaUIA(ctx, r, v, body, "reset the device signing key on your account", false)
		var ch *uiaChallenge
		var se *sigError
		switch {
		case errors.As(err, &ch):
			count("uia")
			writeChallenge(w, ch)
			return
		case errors.As(err, &se):
			fail(clientreq.New(se.status, se.errcode, se.msg))
			return
		case err != nil:
			fail(a.internal(err))
			return
		}
	}
	err = a.uploadSigningKeys(ctx, v.UserID, body, stored[v.UserID])
	var se *sigError
	switch {
	case errors.As(err, &se):
		fail(clientreq.New(se.status, se.errcode, se.msg))
	case err != nil:
		fail(a.internal(err))
	default:
		count("ok")
		clientreq.Write(w, nil, nil)
	}
}

// uploadSigningKeys is upload_signing_keys_for_user.
func (a *App) uploadSigningKeys(ctx context.Context, userID string, keys gjson.Result, stored map[string][]byte) error {
	master := pyField(keys, "master_key")
	fromRequest := master.Exists()
	if fromRequest {
		if err := checkCrossSigningKey(master, userID, "master", nil); err != nil {
			return err
		}
	} else if k, ok := stored["master"]; ok {
		master = gjson.ParseBytes(k)
	}
	if !clientTruthy(master) {
		return &sigError{400, "M_MISSING_PARAM", "No master key available"}
	}
	masterVK, err := crossSigningVerifyKey(master)
	if errors.Is(err, errValue) {
		if fromRequest {
			return &sigError{400, "M_INVALID_PARAM", "Invalid master key"}
		}
		a.Log.Error().Str("user", userID).Msg("Invalid master key found for user")
		return &sigError{500, "M_UNKNOWN", "Invalid master key"}
	}
	if err != nil {
		return err
	}
	ss, us := pyField(keys, "self_signing_key"), pyField(keys, "user_signing_key")
	if ss.Exists() {
		if err := checkCrossSigningKey(ss, userID, "self_signing", &masterVK); err != nil {
			return err
		}
	}
	if us.Exists() {
		if err := checkCrossSigningKey(us, userID, "user_signing", &masterVK); err != nil {
			return err
		}
	}
	var ids []string
	if fromRequest {
		if err := a.storeCrossSigningKey(ctx, userID, "master", master); err != nil {
			return err
		}
		ids = append(ids, masterVK.version)
	}
	if ss.Exists() {
		if err := a.storeCrossSigningKey(ctx, userID, "self_signing", ss); err != nil {
			return err
		}
		vk, err := crossSigningVerifyKey(ss)
		if errors.Is(err, errValue) {
			return &sigError{400, "M_INVALID_PARAM", "Invalid self-signing key"}
		}
		if err != nil {
			return err
		}
		ids = append(ids, vk.version)
	}
	if us.Exists() {
		if err := a.storeCrossSigningKey(ctx, userID, "user_signing", us); err != nil {
			return err
		}
		if err := a.NotifyUserSignatureUpdate(ctx, userID, "["+pyDumpsStr(userID)+"]"); err != nil {
			return err
		}
	}
	if len(ids) > 0 {
		return a.NotifyDeviceUpdate(ctx, userID, ids)
	}
	return nil
}

// storeCrossSigningKey is set_e2e_cross_signing_key for one of our users:
// the key's public key reserved as a hidden device (a second upload of the
// same public key fails there, as Synapse's IntegrityError does).
func (a *App) storeCrossSigningKey(ctx context.Context, userID, keyType string, key gjson.Result) error {
	var pub gjson.Result
	pyField(key, "keys").ForEach(func(_, v gjson.Result) bool { pub = v; return false })
	pubkey, err := sigText(pub)
	if err != nil {
		return err
	}
	enc, err := pyjson.Encode([]byte(key.Raw))
	if err != nil {
		return err
	}
	return a.Caches.Do(func() ([]cachestream.Row, error) {
		return a.GStore.SetLocalCrossSigningKey(ctx, a.Instance, userID, keyType, enc, pubkey, a.Now().UnixMilli())
	})
}

// checkCrossSigningKey is _check_cross_signing_key.
func checkCrossSigningKey(key gjson.Result, userID, keyType string, signing *verifyKey) error {
	if !key.IsObject() {
		return errPython // key.get
	}
	invalid := &sigError{400, "M_INVALID_PARAM", "Invalid " + keyType + " key"}
	uid := pyField(key, "user_id")
	if uid.Type != gjson.String || uid.Str != userID {
		return invalid
	}
	usage := pyField(key, "usage")
	if !usage.Exists() {
		usage = gjson.Parse(`[]`)
	}
	in, err := pyIn(usage, keyType)
	if err != nil {
		return err
	}
	if !in {
		return invalid
	}
	keys := pyField(key, "keys")
	if !keys.Exists() {
		keys = gjson.Parse(`{}`)
	}
	n, err := pyLen(keys)
	if err != nil {
		return err
	}
	if n != 1 {
		return invalid
	}
	if signing != nil {
		ok, err := verifySigned(key, userID, *signing)
		if err != nil {
			return err
		}
		if !ok {
			return &sigError{400, "M_INVALID_SIGNATURE", "Invalid signature on " + keyType + " key"}
		}
	}
	return nil
}

// pyLen is len(): a str's code points, a list's or dict's entries; a
// TypeError for the rest.
func pyLen(v gjson.Result) (int, error) {
	switch {
	case v.Type == gjson.String:
		return utf8.RuneCountInString(v.Str), nil
	case v.IsArray():
		return len(v.Array()), nil
	case v.IsObject():
		n := 0
		v.ForEach(func(_, _ gjson.Result) bool { n++; return true })
		return n, nil
	}
	return 0, errPython
}

// writeChallenge answers InteractiveAuthIncompleteError: 401 and its body.
func writeChallenge(w http.ResponseWriter, ch *uiaChallenge) {
	w.Header().Set("Content-Type", "application/json")
	clientreq.SetCORS(w)
	w.WriteHeader(http.StatusUnauthorized)
	enc, _ := pyjson.Encode([]byte(ch.body))
	_, _ = w.Write([]byte(enc))
}

// Store.

// MasterKeyUpdatableBefore is get_master_cross_signing_key_updatable_before.
func (d *DB) MasterKeyUpdatableBefore(ctx context.Context, userID string) (bool, *int64, error) {
	var ts *int64
	err := d.pool.QueryRow(ctx, `SELECT updatable_without_uia_before_ms FROM e2e_cross_signing_keys
		WHERE user_id = $1 AND keytype = 'master' ORDER BY stream_id DESC LIMIT 1`, userID).Scan(&ts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	return true, ts, nil
}

// SetLocalCrossSigningKey is _set_e2e_cross_signing_key_txn for a local
// user: the hidden device of the public key, the key, and the
// _get_bare_e2e_cross_signing_keys caches row; the ID from
// e2e_cross_signing_keys_sequence.
func (d *DB) SetLocalCrossSigningKey(ctx context.Context, instance, userID, keyType, key, pubkey string, nowMS int64) ([]cachestream.Row, error) {
	var rows []cachestream.Row
	err := d.runTxn(ctx, func(tx pgx.Tx) error {
		var id int64
		if err := tx.QueryRow(ctx, `SELECT nextval('e2e_cross_signing_keys_sequence')`).Scan(&id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO devices (user_id, device_id, display_name, hidden) VALUES ($1, $2, $3, true)`,
			userID, pubkey, keyType+" signing key"); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO e2e_cross_signing_keys (user_id, keytype, keydata, stream_id, instance_name)
			VALUES ($1, $2, $3, $4, $5)`, userID, keyType, key, id, instance); err != nil {
			return err
		}
		var err error
		rows, err = cachestream.Insert(ctx, tx, instance, "_get_bare_e2e_cross_signing_keys", [][]string{{userID}}, nowMS)
		return err
	})
	return rows, err
}
