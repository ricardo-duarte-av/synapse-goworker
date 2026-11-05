package devicelists

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
)

// Synapse's names (handlers/device.py, util/task_scheduler.py).
const (
	deleteDeviceMsgsTask = "delete_device_messages"
	msc3890Prefix        = "org.matrix.msc3890.local_notification_settings."
)

// DeleteStore is what delete_devices writes (DB).
type DeleteStore interface {
	DeleteDevices(ctx context.Context, instance, userID string, deviceIDs []string, nowMS int64) ([]cachestream.Row, error)
	DevicesWithMessages(ctx context.Context, userID string, deviceIDs []string) ([]string, error)
	InsertScheduledTask(ctx context.Context, id, action, status string, ts int64, resourceID, params string) error
	DeleteTokensForDevices(ctx context.Context, instance, userID string, deviceIDs []string, nowMS int64) ([]cachestream.Row, error)
	PushersForDevices(ctx context.Context, userID string) ([]Pusher, error)
}

// Pusher is a pushers row, as remove_pushers_by_devices reads it.
type Pusher struct {
	AppID, PushKey, UserName string
	DeviceID                 *string
}

// AccountDataRemover is remove_account_data_for_user: the account-data
// writer in this process, or one called over replication.
type AccountDataRemover interface {
	RemoveUserAccountData(ctx context.Context, userID, dataType string) error
}

// deleteDevicesPath is client_patterns("/delete_devices$").
func deleteDevicesPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/_matrix/client/")
	if !ok {
		return false
	}
	version, rest, _ := strings.Cut(rest, "/")
	return (version == "r0" || version == "v3" || version == "unstable") && rest == "delete_devices"
}

// deleteBody is parse_and_validate_json_object_from_request against
// DeleteBody (bulk false) or DeleteDevicesRestServlet.PostBody (bulk):
// the body as body.dict(exclude_unset=True) has it, AuthenticationData's
// declared fields first.
func deleteBody(r *http.Request, bulk bool) (gjson.Result, *clientreq.Error, error) {
	raw, _, cerr := clientreq.ReadObjectRaw(r.Body)
	if cerr != nil {
		if cerr.Body["errcode"] != "M_NOT_JSON" {
			return gjson.Result{}, cerr, nil
		}
		// Older clients sent no JSON: model_validate({}), which PostBody's
		// required devices turns into a pydantic ValidationError, a 500.
		if bulk {
			return gjson.Result{}, nil, errPython
		}
		raw = []byte(`{}`)
	}
	raw, err := pyjson.Dedupe(raw)
	if err != nil {
		return gjson.Result{}, nil, err
	}
	body := gjson.ParseBytes(raw)
	var p pydErrors
	auth := pyField(body, "auth")
	if auth.Exists() && auth.Type != gjson.Null {
		if !auth.IsObject() {
			p.add("model_type", "'.auth' must be an object")
		} else {
			for _, f := range []string{"session", "type"} {
				if x := pyField(auth, f); x.Exists() && x.Type != gjson.Null && x.Type != gjson.String {
					p.typ(".auth."+f, "string")
				}
			}
		}
	}
	devices := pyField(body, "devices")
	if bulk {
		switch {
		case !devices.Exists():
			p.missing(".devices")
		case !devices.IsArray():
			p.typ(".devices", "list")
		default:
			for i, d := range devices.Array() {
				if d.Type != gjson.String {
					p.typ(fmt.Sprintf(".devices[%d]", i), "string")
				}
			}
		}
	}
	if cerr := p.err(); cerr != nil {
		return gjson.Result{}, cerr, nil
	}
	var b strings.Builder
	b.WriteByte('{')
	if auth.Exists() {
		b.WriteString(`"auth":`)
		if auth.Type == gjson.Null {
			b.WriteString("null")
		} else {
			b.WriteByte('{')
			n := 0
			put := func(k string, v gjson.Result) {
				if n > 0 {
					b.WriteByte(',')
				}
				n++
				b.WriteString(jsonStr(k) + ":" + v.Raw)
			}
			for _, f := range []string{"session", "type"} {
				if x := pyField(auth, f); x.Exists() {
					put(f, x)
				}
			}
			auth.ForEach(func(k, v gjson.Result) bool {
				if k.Str != "session" && k.Str != "type" {
					put(k.Str, v)
				}
				return true
			})
			b.WriteByte('}')
		}
	}
	if bulk {
		if auth.Exists() {
			b.WriteByte(',')
		}
		b.WriteString(`"devices":` + devices.Raw)
	}
	b.WriteByte('}')
	return gjson.Parse(b.String()), nil, nil
}

// serveDeleteDevice is DeviceRestServlet.on_DELETE (bulk false) and
// DeleteDevicesRestServlet.on_POST (bulk true).
func (a *App) serveDeleteDevice(ctx context.Context, w http.ResponseWriter, r *http.Request, v clientauth.Verdict, deviceID string, bulk bool, count func(string)) {
	body, cerr, err := deleteBody(r, bulk)
	if err != nil {
		count("refused")
		clientreq.Write(w, a.internal(err), nil)
		return
	}
	if cerr != nil {
		count("refused")
		clientreq.Write(w, cerr, nil)
		return
	}
	ids := []string{deviceID}
	description := "remove a device from your account"
	if bulk {
		ids = nil
		for _, d := range pyField(body, "devices").Array() {
			ids = append(ids, d.Str)
		}
		description = "remove device(s) from your account"
	}
	if v.AppServiceID == "" {
		err := a.validateUserViaUIA(ctx, r, v, body, description)
		var ch *uiaChallenge
		var se *sigError
		switch {
		case errors.As(err, &ch):
			count("uia")
			w.Header().Set("Content-Type", "application/json")
			clientreq.SetCORS(w)
			w.WriteHeader(http.StatusUnauthorized)
			enc, _ := pyjson.Encode([]byte(ch.body))
			_, _ = w.Write([]byte(enc))
			return
		case errors.As(err, &se):
			count("refused")
			clientreq.Write(w, clientreq.New(se.status, se.errcode, se.msg), nil)
			return
		case err != nil:
			count("refused")
			clientreq.Write(w, a.internal(err), nil)
			return
		}
	}
	if err := a.deleteDevices(ctx, v.UserID, ids); err != nil {
		count("refused")
		clientreq.Write(w, a.internal(err), nil)
		return
	}
	count("ok")
	clientreq.Write(w, nil, nil)
}

// deleteDevices is DeviceWriterHandler.delete_devices.
func (a *App) deleteDevices(ctx context.Context, userID string, deviceIDs []string) error {
	upTo := int64(0)
	if a.ToDeviceToken != nil {
		upTo = a.ToDeviceToken()
	}
	now := a.Now().UnixMilli()
	for start := 0; start < len(deviceIDs); start += 1000 {
		batch := deviceIDs[start:min(start+1000, len(deviceIDs))]
		if err := a.Caches.Do(func() ([]cachestream.Row, error) {
			return a.DStore.DeleteDevices(ctx, a.Instance, userID, batch, now)
		}); err != nil {
			return err
		}
	}
	if a.Synapse().Flag("msc3890_enabled", false) {
		if a.AccountData == nil {
			return errors.New("devicelists: no account-data writer for MSC3890's settings")
		}
		for _, d := range deviceIDs {
			if err := a.AccountData.RemoveUserAccountData(ctx, userID, msc3890Prefix+d); err != nil {
				return err
			}
		}
	}
	var withMessages []string
	for start := 0; start < len(deviceIDs); start += 1000 {
		got, err := a.DStore.DevicesWithMessages(ctx, userID, deviceIDs[start:min(start+1000, len(deviceIDs))])
		if err != nil {
			return err
		}
		withMessages = append(withMessages, got...)
	}
	for _, d := range withMessages {
		id := randomString(16)
		params := `{"user_id":` + jsonStr(userID) + `,"device_id":` + jsonStr(d) + fmt.Sprintf(`,"up_to_stream_id":%d}`, upTo)
		enc, err := pyjson.Encode([]byte(params))
		if err != nil {
			return err
		}
		if err := a.DStore.InsertScheduledTask(ctx, id, deleteDeviceMsgsTask, "scheduled", a.Now().UnixMilli(), d, enc); err != nil {
			return err
		}
		// send_new_active_task: main runs background tasks.
		if a.PublishTask != nil {
			if err := a.PublishTask(ctx, id); err != nil {
				a.Log.Warn().Err(err).Str("task", id).Msg("devices: NEW_ACTIVE_TASK not published (main finds it within a minute)")
			}
		}
	}
	for start := 0; start < len(deviceIDs); start += 1000 {
		batch := deviceIDs[start:min(start+1000, len(deviceIDs))]
		if err := a.Caches.Do(func() ([]cachestream.Row, error) {
			return a.DStore.DeleteTokensForDevices(ctx, a.Instance, userID, batch, a.Now().UnixMilli())
		}); err != nil {
			return err
		}
	}
	pushers, err := a.DStore.PushersForDevices(ctx, userID)
	if err != nil {
		return err
	}
	set := map[string]bool{}
	for _, d := range deviceIDs {
		set[d] = true
	}
	for _, p := range pushers {
		if p.DeviceID == nil || !set[*p.DeviceID] {
			continue
		}
		a.Log.Info().Str("app_id", p.AppID).Str("user", p.UserName).Msg("Removing pusher")
		if a.RemovePusher == nil {
			return errors.New("devicelists: no way to remove pushers")
		}
		if err := a.RemovePusher(ctx, p.AppID, p.PushKey, p.UserName); err != nil {
			return err
		}
	}
	return a.NotifyDeviceUpdate(ctx, userID, deviceIDs)
}

// Store.

// DeleteDevices is _delete_devices_txn: the devices (not hidden ones),
// their keys, and the user's three caches rows.
func (d *DB) DeleteDevices(ctx context.Context, instance, userID string, deviceIDs []string, nowMS int64) ([]cachestream.Row, error) {
	var rows []cachestream.Row
	err := d.runTxn(ctx, func(tx pgx.Tx) error {
		rows = nil
		for _, q := range []string{
			`DELETE FROM devices WHERE user_id = $1 AND hidden = false AND device_id = ANY($2)`,
			`DELETE FROM device_auth_providers WHERE user_id = $1 AND device_id = ANY($2)`,
			`DELETE FROM e2e_device_keys_json WHERE user_id = $1 AND device_id = ANY($2)`,
			`DELETE FROM e2e_one_time_keys_json WHERE user_id = $1 AND device_id = ANY($2)`,
			`DELETE FROM dehydrated_devices WHERE user_id = $1 AND device_id = ANY($2)`,
			`DELETE FROM e2e_fallback_keys_json WHERE user_id = $1 AND device_id = ANY($2)`,
		} {
			if _, err := tx.Exec(ctx, q, userID, deviceIDs); err != nil {
				return err
			}
		}
		for _, fn := range []string{"get_device", "count_e2e_one_time_keys", "get_e2e_unused_fallback_key_types"} {
			r, err := cachestream.Insert(ctx, tx, instance, fn, [][]string{{userID}}, nowMS)
			if err != nil {
				return err
			}
			rows = append(rows, r...)
		}
		return nil
	})
	return rows, err
}

// DevicesWithMessages is get_devices_with_messages.
func (d *DB) DevicesWithMessages(ctx context.Context, userID string, deviceIDs []string) ([]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT DISTINCT device_id FROM device_inbox WHERE device_id = ANY($1) AND user_id = $2`, deviceIDs, userID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// InsertScheduledTask is insert_scheduled_task.
func (d *DB) InsertScheduledTask(ctx context.Context, id, action, status string, ts int64, resourceID, params string) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO scheduled_tasks (id, action, status, timestamp, resource_id, params, result, error)
		VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL)`, id, action, status, ts, resourceID, params)
	return err
}

// DeleteTokensForDevices is user_delete_access_tokens_for_devices_txn:
// the access tokens (a get_user_by_access_token row each) and refresh
// tokens of the devices.
func (d *DB) DeleteTokensForDevices(ctx context.Context, instance, userID string, deviceIDs []string, nowMS int64) ([]cachestream.Row, error) {
	var out []cachestream.Row
	err := d.runTxn(ctx, func(tx pgx.Tx) error {
		out = nil
		rows, err := tx.Query(ctx, `DELETE FROM access_tokens WHERE device_id = ANY($1) AND user_id = $2 RETURNING token`, deviceIDs, userID)
		if err != nil {
			return err
		}
		tokens, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		var keys [][]string
		for _, t := range tokens {
			keys = append(keys, []string{t})
		}
		if out, err = cachestream.Insert(ctx, tx, instance, "get_user_by_access_token", keys, nowMS); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `DELETE FROM refresh_tokens WHERE user_id = $1 AND device_id = ANY($2)`, userID, deviceIDs)
		return err
	})
	return out, err
}

// PushersForDevices is get_pushers_by_user_id's rows.
func (d *DB) PushersForDevices(ctx context.Context, userID string) ([]Pusher, error) {
	rows, err := d.pool.Query(ctx, `SELECT app_id, pushkey, user_name, device_id FROM pushers WHERE user_name = $1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Pusher
	for rows.Next() {
		var p Pusher
		if err := rows.Scan(&p.AppID, &p.PushKey, &p.UserName, &p.DeviceID); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ToDeviceMax is the greatest stream ID of the to_device stream's tables.
func (d *DB) ToDeviceMax(ctx context.Context) (int64, error) {
	var n int64
	err := d.pool.QueryRow(ctx, `SELECT GREATEST(
		(SELECT COALESCE(MAX(stream_id), 0) FROM device_inbox),
		(SELECT COALESCE(MAX(stream_id), 0) FROM device_federation_outbox))`).Scan(&n)
	return n, err
}
