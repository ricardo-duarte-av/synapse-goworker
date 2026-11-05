package devicelists

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
)

var clientRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goworker_device_lists_client_requests_total",
	Help: "Client requests to the device and key endpoints served here, by endpoint, method and outcome. to_main went to main (not reproduced here yet).",
}, []string{"endpoint", "method", "outcome"})

// maxDeviceNameLen is MAX_DEVICE_DISPLAY_NAME_LEN, in code points.
const maxDeviceNameLen = 100

// Device is one row of `devices` as the endpoints show it.
type Device struct {
	UserID, DeviceID string
	DisplayName      *string
}

// LastSeen is a device's last connection (devices.ip, user_agent,
// last_seen).
type LastSeen struct {
	IP, UserAgent *string
	TS            *int64
}

// ClientStore is what the client endpoints read and write (DB).
type ClientStore interface {
	Devices(ctx context.Context, userID string) ([]Device, error)
	Device(ctx context.Context, userID, deviceID string) (*Device, error)
	LastSeen(ctx context.Context, userID string, deviceID *string) (map[string]LastSeen, error)
	UpdateDisplayName(ctx context.Context, userID, deviceID, name string) (int64, error)
	StoreDevice(ctx context.Context, userID, deviceID string, name *string) (bool, error)
	DeviceHidden(ctx context.Context, userID, deviceID string) (bool, error)
}

// devicePath is client_patterns("/devices$") and
// ("/devices/(?P<device_id>[^/]*)$"): r0, v3, unstable.
func devicePath(p string) (deviceID string, one, ok bool) {
	rest, found := strings.CutPrefix(p, "/_matrix/client/")
	if !found {
		return "", false, false
	}
	version, rest, _ := strings.Cut(rest, "/")
	if version != "r0" && version != "v3" && version != "unstable" {
		return "", false, false
	}
	if rest == "devices" {
		return "", false, true
	}
	if id, found := strings.CutPrefix(rest, "devices/"); found && !strings.Contains(id, "/") {
		return clientreq.Unquote(id), true, true
	}
	return "", false, false
}

// MatchDevices is the device endpoints served here: GET /devices, GET and
// PUT /devices/{deviceId}. DELETE and anything else go to main.
func MatchDevices(method, escapedPath string) bool {
	_, one, ok := devicePath(escapedPath)
	if !ok {
		return false
	}
	if !one {
		return method == http.MethodGet
	}
	return method == http.MethodGet || method == http.MethodPut
}

// DevicesHandler serves what MatchDevices accepts.
func (a *App) DevicesHandler() http.Handler { return http.HandlerFunc(a.serveDevices) }

func (a *App) serveDevices(w http.ResponseWriter, r *http.Request) {
	id, one, _ := devicePath(r.URL.EscapedPath())
	endpoint := "devices"
	if one {
		endpoint = "device"
	}
	count := func(o string) { clientRequests.WithLabelValues(endpoint, r.Method, o).Inc() }
	if !a.writing || a.CStore == nil {
		count("to_main")
		a.Fallback.ServeHTTP(w, r)
		return
	}
	ctx := r.Context()
	v, cerr, err := clientreq.User(ctx, a.Auth, r, true)
	if err != nil {
		a.Log.Error().Err(err).Msg("devices: cannot validate a token")
		count("auth_unavailable")
		clientreq.Write(w, cerr, nil)
		return
	}
	if cerr != nil {
		count("auth_rejected")
		clientreq.Write(w, cerr, nil)
		return
	}
	var (
		body   []byte
		status = http.StatusOK
	)
	switch {
	case !one:
		body, cerr = a.getDevices(ctx, v.UserID)
	case r.Method == http.MethodGet:
		body, cerr = a.getDevice(ctx, v.UserID, id)
	default:
		status, cerr = a.putDevice(ctx, r, v, id)
	}
	if cerr != nil {
		count("refused")
		clientreq.Write(w, cerr, nil)
		return
	}
	count("ok")
	if body == nil {
		if status == http.StatusCreated {
			writeStatus(w, status)
			return
		}
		clientreq.Write(w, nil, nil)
		return
	}
	clientreq.WriteRaw(w, body)
}

func writeStatus(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "application/json")
	clientreq.SetCORS(w)
	w.WriteHeader(status)
	_, _ = w.Write([]byte("{}"))
}

// deviceJSON is a device as the endpoints answer it: user_id, device_id,
// display_name, last_seen_ts, last_seen_ip (last_seen_user_agent is added
// by _update_device_from_client_ips, then deleted).
func deviceJSON(d Device, seen map[string]LastSeen) json.RawMessage {
	var b strings.Builder
	str := func(s *string) string {
		if s == nil {
			return "null"
		}
		x, _ := json.Marshal(*s)
		return string(x)
	}
	b.WriteString(`{"user_id":`)
	b.WriteString(str(&d.UserID))
	b.WriteString(`,"device_id":`)
	b.WriteString(str(&d.DeviceID))
	b.WriteString(`,"display_name":`)
	b.WriteString(str(d.DisplayName))
	ls, ok := seen[d.DeviceID]
	b.WriteString(`,"last_seen_ts":`)
	if ok && ls.TS != nil {
		x, _ := json.Marshal(*ls.TS)
		b.Write(x)
	} else {
		b.WriteString("null")
	}
	b.WriteString(`,"last_seen_ip":`)
	if ok {
		b.WriteString(str(ls.IP))
	} else {
		b.WriteString("null")
	}
	b.WriteByte('}')
	return json.RawMessage(b.String())
}

// getDevices is DevicesRestServlet.on_GET.
func (a *App) getDevices(ctx context.Context, userID string) ([]byte, *clientreq.Error) {
	devs, err := a.CStore.Devices(ctx, userID)
	if err != nil {
		return nil, a.internal(err)
	}
	seen, err := a.CStore.LastSeen(ctx, userID, nil)
	if err != nil {
		return nil, a.internal(err)
	}
	parts := make([]string, len(devs))
	for i, d := range devs {
		parts[i] = string(deviceJSON(d, seen))
	}
	enc, err := pyjson.Encode([]byte(`{"devices":[` + strings.Join(parts, ",") + `]}`))
	if err != nil {
		return nil, a.internal(err)
	}
	return []byte(enc), nil
}

// getDevice is DeviceRestServlet.on_GET.
func (a *App) getDevice(ctx context.Context, userID, deviceID string) ([]byte, *clientreq.Error) {
	d, err := a.CStore.Device(ctx, userID, deviceID)
	if err != nil {
		return nil, a.internal(err)
	}
	if d == nil {
		// get_device raises NotFoundError() before the servlet's "No device found".
		return nil, clientreq.New(404, "M_NOT_FOUND", "Not found")
	}
	seen, err := a.CStore.LastSeen(ctx, userID, &deviceID)
	if err != nil {
		return nil, a.internal(err)
	}
	enc, err := pyjson.Encode(deviceJSON(*d, seen))
	if err != nil {
		return nil, a.internal(err)
	}
	return []byte(enc), nil
}

func (a *App) internal(err error) *clientreq.Error {
	a.Log.Error().Err(err).Msg("devices: request failed")
	return clientreq.Internal
}

// putDevice is DeviceRestServlet.on_PUT: PutBody(display_name: StrictStr |
// None), then upsert_device for an appservice (MSC4190) or update_device.
func (a *App) putDevice(ctx context.Context, r *http.Request, v clientauth.Verdict, deviceID string) (int, *clientreq.Error) {
	raw, _, cerr := clientreq.ReadObjectRaw(r.Body)
	if cerr != nil {
		return 0, cerr
	}
	raw, err := pyjson.Dedupe(raw)
	if err != nil {
		return 0, clientreq.Internal
	}
	dn := gjson.GetBytes(raw, "display_name")
	var name *string
	switch {
	case !dn.Exists() || dn.Type == gjson.Null:
	case dn.Type == gjson.String:
		s := dn.Str
		name = &s
	default:
		return 0, clientreq.New(400, "M_BAD_JSON", "'.display_name': Input should be a valid string")
	}
	if name != nil && *name != "" && utf8.RuneCountInString(*name) > maxDeviceNameLen {
		return 0, clientreq.New(400, "M_TOO_LARGE", "Device display name is too long (max 100)")
	}
	if v.AppServiceID != "" {
		created, err := a.upsertDevice(ctx, v.UserID, deviceID, name)
		if err != nil {
			var se *storeError
			if errors.As(err, &se) {
				return 0, clientreq.New(se.code, se.errcode, se.msg)
			}
			return 0, a.internal(err)
		}
		if created {
			return http.StatusCreated, nil
		}
		return http.StatusOK, nil
	}
	if name != nil {
		n, err := a.CStore.UpdateDisplayName(ctx, v.UserID, deviceID, *name)
		if err != nil {
			return 0, a.internal(err)
		}
		if n == 0 {
			return 0, clientreq.New(404, "M_NOT_FOUND", "Not found")
		}
		a.invalidateDevice(v.UserID, deviceID)
	}
	if err := a.NotifyDeviceUpdate(ctx, v.UserID, []string{deviceID}); err != nil {
		return 0, a.internal(err)
	}
	return http.StatusOK, nil
}

// storeError is a StoreError the store raises, as the client sees it.
type storeError struct {
	code         int
	errcode, msg string
}

func (e *storeError) Error() string { return e.msg }

// upsertDevice is upsert_device: the device created if it does not exist
// (store_device), its name set otherwise, and the change notified.
func (a *App) upsertDevice(ctx context.Context, userID, deviceID string, name *string) (bool, error) {
	created, err := a.CStore.StoreDevice(ctx, userID, deviceID, name)
	if err != nil {
		return false, &storeError{500, "M_UNKNOWN", "Problem storing device."}
	}
	a.invalidateDevice(userID, deviceID)
	if !created {
		hidden, err := a.CStore.DeviceHidden(ctx, userID, deviceID)
		if err != nil {
			return false, err
		}
		if hidden {
			return false, &storeError{400, "M_FORBIDDEN", "The device ID is in use"}
		}
		if name != nil {
			n, err := a.CStore.UpdateDisplayName(ctx, userID, deviceID, *name)
			if err != nil {
				return false, err
			}
			if n == 0 {
				return false, &storeError{404, "M_UNKNOWN", "No row found"}
			}
			a.invalidateDevice(userID, deviceID)
		}
	}
	if err := a.NotifyDeviceUpdate(ctx, userID, []string{deviceID}); err != nil {
		return false, err
	}
	return created, nil
}

// invalidateDevice is invalidate_cache_and_stream("get_device", (user,
// device)): its own transaction, after the write.
func (a *App) invalidateDevice(userID, deviceID string) {
	if a.Caches == nil || a.CachesInsert == nil {
		return
	}
	err := a.Caches.Do(func() ([]cachestream.Row, error) {
		return a.CachesInsert(context.Background(), "get_device", [][]string{{userID, deviceID}})
	})
	if err != nil {
		a.Log.Warn().Err(err).Msg("devices: get_device invalidation")
	}
}

// Store queries for the client endpoints.

// Devices is get_devices_by_user (simple_select_list: the table's order).
func (d *DB) Devices(ctx context.Context, userID string) ([]Device, error) {
	rows, err := d.pool.Query(ctx, `SELECT user_id, device_id, display_name FROM devices WHERE user_id = $1 AND hidden = false`, userID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Device, error) {
		var x Device
		return x, r.Scan(&x.UserID, &x.DeviceID, &x.DisplayName)
	})
}

// Device is get_device.
func (d *DB) Device(ctx context.Context, userID, deviceID string) (*Device, error) {
	var x Device
	err := d.pool.QueryRow(ctx, `SELECT user_id, device_id, display_name FROM devices WHERE user_id = $1 AND device_id = $2 AND hidden = false`,
		userID, deviceID).Scan(&x.UserID, &x.DeviceID, &x.DisplayName)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &x, nil
}

// LastSeen is _get_last_client_ip_by_device_from_database: the devices
// table's last connection, for one device or all of the user's.
func (d *DB) LastSeen(ctx context.Context, userID string, deviceID *string) (map[string]LastSeen, error) {
	q := `SELECT device_id, ip, user_agent, last_seen FROM devices WHERE user_id = $1`
	args := []any{userID}
	if deviceID != nil {
		q += ` AND device_id = $2`
		args = append(args, *deviceID)
	}
	rows, err := d.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]LastSeen{}
	for rows.Next() {
		var id string
		var ls LastSeen
		if err := rows.Scan(&id, &ls.IP, &ls.UserAgent, &ls.TS); err != nil {
			return nil, err
		}
		out[id] = ls
	}
	return out, rows.Err()
}

// UpdateDisplayName is update_device's simple_update_one: rows changed.
func (d *DB) UpdateDisplayName(ctx context.Context, userID, deviceID, name string) (int64, error) {
	tag, err := d.pool.Exec(ctx, `UPDATE devices SET display_name = $1 WHERE user_id = $2 AND device_id = $3 AND hidden = false`,
		name, userID, deviceID)
	return tag.RowsAffected(), err
}

// StoreDevice is store_device's upsert: inserted or not.
func (d *DB) StoreDevice(ctx context.Context, userID, deviceID string, name *string) (bool, error) {
	tag, err := d.pool.Exec(ctx, `INSERT INTO devices (user_id, device_id, display_name, hidden) VALUES ($1, $2, $3, false)
		ON CONFLICT (user_id, device_id) DO NOTHING`, userID, deviceID, name)
	return tag.RowsAffected() == 1, err
}

// DeviceHidden is the device's hidden flag.
func (d *DB) DeviceHidden(ctx context.Context, userID, deviceID string) (bool, error) {
	var h *bool
	err := d.pool.QueryRow(ctx, `SELECT hidden FROM devices WHERE user_id = $1 AND device_id = $2`, userID, deviceID).Scan(&h)
	return h != nil && *h, err
}

// InsertCaches inserts caches-stream rows in a transaction of their own
// (invalidate_cache_and_stream outside a transaction).
func (d *DB) InsertCaches(instance string) func(ctx context.Context, cacheFunc string, keys [][]string) ([]cachestream.Row, error) {
	return func(ctx context.Context, cacheFunc string, keys [][]string) ([]cachestream.Row, error) {
		var rows []cachestream.Row
		err := d.runTxn(ctx, func(tx pgx.Tx) error {
			var err error
			rows, err = cachestream.Insert(ctx, tx, instance, cacheFunc, keys, nowMS())
			return err
		})
		return rows, err
	}
}
