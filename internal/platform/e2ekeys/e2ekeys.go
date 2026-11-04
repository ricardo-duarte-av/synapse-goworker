// Package e2ekeys reads local users' end-to-end keys as Synapse serves them
// (storage/databases/main/end_to_end_keys.py, handlers/e2e_keys.py,
// v1.164.2): devices' keys with their owners' cross-signatures, and
// cross-signing keys with a requester's signatures. Client POST /keys/query
// (sync) and federation POST /user/keys/query (the federation reads) both
// answer from it.
package e2ekeys

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Querier runs a query; name labels it for the caller's metrics.
type Querier func(ctx context.Context, name, sql string, args ...any) (pgx.Rows, error)

// PoolQuerier is a Querier over anything with pgx's Query, for a caller with
// no query metrics of its own.
func PoolQuerier(p interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}) Querier {
	return func(ctx context.Context, _ string, sql string, args ...any) (pgx.Rows, error) {
		return p.Query(ctx, sql, args...)
	}
}

// UserDevice is a (user ID, device ID) pair.
type UserDevice struct{ UserID, DeviceID string }

// DeviceKey is one device's row from _get_e2e_device_keys.
type DeviceKey struct {
	DisplayName *string
	// KeyJSON is key_json as stored; nil when empty.
	KeyJSON []byte
}

// DeviceKeys is _get_e2e_device_keys (include_all_devices=False): every
// device of each user in users, and each pair in pairs, that has a
// key_json row and is not hidden.
func DeviceKeys(ctx context.Context, q Querier, users []string, pairs []UserDevice) (map[string]map[string]DeviceKey, error) {
	out := map[string]map[string]DeviceKey{}
	if len(users) == 0 && len(pairs) == 0 {
		return out, nil
	}
	pu, pd := splitPairs(pairs)
	rows, err := q(ctx, "DeviceKeys", `
		SELECT user_id, device_id, d.display_name, k.key_json
		  FROM e2e_device_keys_json k
		 INNER JOIN devices d USING (user_id, device_id)
		 WHERE (user_id = ANY($1)
		        OR (user_id, device_id) IN (SELECT * FROM unnest($2::text[], $3::text[])))
		   AND NOT d.hidden`, users, pu, pd)
	if err != nil {
		return nil, fmt.Errorf("e2ekeys: device keys: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var u, d string
		var name, key *string
		if err := rows.Scan(&u, &d, &name, &key); err != nil {
			return nil, fmt.Errorf("e2ekeys: device keys: %w", err)
		}
		k := DeviceKey{DisplayName: name}
		if key != nil && *key != "" {
			k.KeyJSON = []byte(*key)
		}
		if out[u] == nil {
			out[u] = map[string]DeviceKey{}
		}
		out[u][d] = k
	}
	return out, rows.Err()
}

// Signature is one (key ID, signature).
type Signature struct{ KeyID, Signature string }

// DeviceSelfCrossSignatures is _get_e2e_cross_signing_signatures_for_devices:
// the signatures each device's own user made on it with a cross-signing key.
func DeviceSelfCrossSignatures(ctx context.Context, q Querier, pairs []UserDevice) (map[UserDevice][]Signature, error) {
	out := map[UserDevice][]Signature{}
	if len(pairs) == 0 {
		return out, nil
	}
	pu, pd := splitPairs(pairs)
	rows, err := q(ctx, "DeviceSelfCrossSignatures", `
		SELECT user_id, key_id, target_device_id, signature
		  FROM e2e_cross_signing_signatures
		 WHERE (target_user_id, target_device_id, user_id)
		       IN (SELECT u, d, u FROM unnest($1::text[], $2::text[]) AS t(u, d))`, pu, pd)
	if err != nil {
		return nil, fmt.Errorf("e2ekeys: device signatures: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var u, keyID, d, sig string
		if err := rows.Scan(&u, &keyID, &d, &sig); err != nil {
			return nil, fmt.Errorf("e2ekeys: device signatures: %w", err)
		}
		k := UserDevice{u, d}
		out[k] = append(out[k], Signature{keyID, sig})
	}
	return out, rows.Err()
}

// CrossSigningKeys is _get_bare_e2e_cross_signing_keys_bulk: each user's
// newest key of each type, as stored.
func CrossSigningKeys(ctx context.Context, q Querier, users []string) (map[string]map[string][]byte, error) {
	out := map[string]map[string][]byte{}
	if len(users) == 0 {
		return out, nil
	}
	rows, err := q(ctx, "CrossSigningKeys", `
		SELECT DISTINCT ON (user_id, keytype) user_id, keytype, keydata
		  FROM e2e_cross_signing_keys
		 WHERE user_id = ANY($1)
		 ORDER BY user_id, keytype, stream_id DESC`, users)
	if err != nil {
		return nil, fmt.Errorf("e2ekeys: cross-signing keys: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var u, typ, data string
		if err := rows.Scan(&u, &typ, &data); err != nil {
			return nil, fmt.Errorf("e2ekeys: cross-signing keys: %w", err)
		}
		if out[u] == nil {
			out[u] = map[string][]byte{}
		}
		out[u][typ] = []byte(data)
	}
	return out, rows.Err()
}

// CrossSignature is a signature from _get_e2e_cross_signing_signatures_txn.
type CrossSignature struct {
	Target UserDevice
	Signature
}

// CrossSignaturesBy is _get_e2e_cross_signing_signatures_txn's query: the
// signatures from made on each target, in the order the rows come.
func CrossSignaturesBy(ctx context.Context, q Querier, from string, targets []UserDevice) ([]CrossSignature, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	tu, td := splitPairs(targets)
	rows, err := q(ctx, "CrossSignaturesBy", `
		SELECT target_user_id, target_device_id, key_id, signature
		  FROM e2e_cross_signing_signatures
		 WHERE user_id = $1
		   AND (target_user_id, target_device_id) IN (SELECT * FROM unnest($2::text[], $3::text[]))`,
		from, tu, td)
	if err != nil {
		return nil, fmt.Errorf("e2ekeys: cross signatures: %w", err)
	}
	defer rows.Close()
	var out []CrossSignature
	for rows.Next() {
		var c CrossSignature
		if err := rows.Scan(&c.Target.UserID, &c.Target.DeviceID, &c.KeyID, &c.Signature.Signature); err != nil {
			return nil, fmt.Errorf("e2ekeys: cross signatures: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func splitPairs(pairs []UserDevice) ([]string, []string) {
	u := make([]string, len(pairs))
	d := make([]string, len(pairs))
	for i, p := range pairs {
		u[i], d[i] = p.UserID, p.DeviceID
	}
	return u, d
}

// Local is query_local_devices over get_e2e_device_keys_for_cs_api: an
// entry for every user asked about, with the keys of the devices asked for
// (all of them for an empty list) that have keys and are not hidden, each
// spliced with its owner's cross-signatures and given an unsigned holding
// the device's display name when displayNames is set.
func Local(ctx context.Context, q Querier, users []string, devices map[string][]string,
	displayNames bool) (map[string]map[string]json.RawMessage, error) {

	out := map[string]map[string]json.RawMessage{}
	if len(users) == 0 {
		return out, nil
	}
	var all []string
	var pairs []UserDevice
	for _, u := range users {
		out[u] = map[string]json.RawMessage{}
		if len(devices[u]) == 0 {
			all = append(all, u)
		}
		for _, dev := range devices[u] {
			pairs = append(pairs, UserDevice{UserID: u, DeviceID: dev})
		}
	}
	keys, err := DeviceKeys(ctx, q, all, pairs)
	if err != nil {
		return nil, err
	}
	var signed []UserDevice
	for u, devs := range keys {
		for dev, k := range devs {
			if k.KeyJSON != nil {
				signed = append(signed, UserDevice{UserID: u, DeviceID: dev})
			}
		}
	}
	sigs, err := DeviceSelfCrossSignatures(ctx, q, signed)
	if err != nil {
		return nil, err
	}
	for u, devs := range keys {
		if out[u] == nil {
			continue
		}
		for dev, k := range devs {
			if k.KeyJSON == nil {
				continue
			}
			body := k.KeyJSON
			for _, s := range sigs[UserDevice{UserID: u, DeviceID: dev}] {
				if body, err = sjson.SetBytes(body, "signatures."+gjson.Escape(u)+"."+gjson.Escape(s.KeyID), s.Signature); err != nil {
					return nil, err
				}
			}
			unsigned := `{}`
			if displayNames && k.DisplayName != nil {
				unsigned = `{"device_display_name":` + string(jsonString(*k.DisplayName)) + `}`
			}
			if body, err = sjson.SetRawBytes(body, "unsigned", []byte(unsigned)); err != nil {
				return nil, err
			}
			out[u][dev] = body
		}
	}
	return out, nil
}

// CrossSigning is get_cross_signing_keys_from_cache over
// get_e2e_cross_signing_keys_bulk(user_ids, from_user_id): everyone's
// master and self-signing keys, from's own user-signing key, each with the
// signatures from made on it. from is empty for a server asking (no
// signatures, and no user-signing key).
func CrossSigning(ctx context.Context, q Querier, users []string, from string) (
	master, selfSigning, userSigning map[string]json.RawMessage, err error) {

	master, selfSigning, userSigning = map[string]json.RawMessage{}, map[string]json.RawMessage{}, map[string]json.RawMessage{}
	keys, err := CrossSigningKeys(ctx, q, users)
	if err != nil {
		return nil, nil, nil, err
	}
	// _get_e2e_cross_signing_signatures_txn: each key is found by its user
	// and the last value of its "keys", its public key.
	type slot struct{ user, typ string }
	bySigned := map[UserDevice]slot{}
	var targets []UserDevice
	for u, byType := range keys {
		for typ, data := range byType {
			pub := ""
			gjson.GetBytes(data, "keys").ForEach(func(_, v gjson.Result) bool {
				pub = v.String()
				return true
			})
			t := UserDevice{UserID: u, DeviceID: pub}
			if _, dup := bySigned[t]; !dup {
				targets = append(targets, t)
			}
			bySigned[t] = slot{u, typ}
		}
	}
	if from != "" && len(targets) > 0 {
		sigs, err := CrossSignaturesBy(ctx, q, from, targets)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, s := range sigs {
			at := bySigned[s.Target]
			data := keys[at.user][at.typ]
			if data, err = sjson.SetBytes(data, "signatures."+gjson.Escape(from)+"."+gjson.Escape(s.KeyID), s.Signature.Signature); err != nil {
				return nil, nil, nil, err
			}
			keys[at.user][at.typ] = data
		}
	}
	for u, byType := range keys {
		if k, ok := byType["master"]; ok {
			master[u] = k
		}
		if k, ok := byType["self_signing"]; ok {
			selfSigning[u] = k
		}
	}
	if k, ok := keys[from]["user_signing"]; ok && from != "" {
		userSigning[from] = k
	}
	return master, selfSigning, userSigning, nil
}

// jsonString encodes s without escaping <, > and &, as Synapse's encoder.
func jsonString(s string) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}
