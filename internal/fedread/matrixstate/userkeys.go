package matrixstate

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/e2ekeys"
)

// KeyQueryOptions are what POST /user/keys/query reads from homeserver.yaml.
type KeyQueryOptions struct {
	// DisplayNames is allow_device_name_lookup_over_federation.
	DisplayNames bool
	// MSC3984 is msc3984_appservice_key_query: a user an appservice owns
	// exclusively is asked of the appservice too. ExclusiveUsers is those
	// namespaces (nil when none), ExclusiveUnknown that they could not be
	// read.
	MSC3984          bool
	ExclusiveUsers   *regexp.Regexp
	ExclusiveUnknown bool
}

// SetKeyQuery configures UserKeys.
func (r *Resolver) SetKeyQuery(o KeyQueryOptions) { r.keyQuery = o }

// KeyQueryOptions is the configured options.
func (r *Resolver) KeyQueryOptions() KeyQueryOptions { return r.keyQuery }

// UserKeys is on_federation_query_client_keys (handlers/e2e_keys.py,
// v1.164.2) for local users already checked: query_local_devices, with
// display names only when allowed over federation, and the master and
// self-signing keys with no requester's signatures (from_user_id None).
func (r *Resolver) UserKeys(ctx context.Context, users []string, devices map[string][]string) ([]byte, error) {
	q := e2ekeys.PoolQuerier(r.db.Pool())
	local, err := e2ekeys.Local(ctx, q, users, devices, r.keyQuery.DisplayNames)
	if err != nil {
		return nil, err
	}
	master, selfSigning, userSigning, err := e2ekeys.CrossSigning(ctx, q, users, "")
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(map[string]any{
		"device_keys": local, "master_keys": master,
		"self_signing_keys": selfSigning, "user_signing_keys": userSigning,
	}); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
