package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"maunium.net/go/mautrix/federation"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/accountdata"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/devicelists"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/matrixstate"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/backoff"
	inboundlocks "github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/inbound"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/fedclient"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdustore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/statecache"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/todevice"
)

// deviceQueryTimeout is query_user_devices' timeout (30 s, one attempt).
const deviceQueryTimeout = 30 * time.Second

// deviceQuery is federation_client.query_user_devices for the device-lists
// writer: GET /_matrix/federation/v1/user/devices/{userId}, signed, under
// the destination's retry limiter (inbound's, which stores the timings as
// Synapse's RetryDestinationLimiter does; nil honours nothing), refused for
// a server outside federation_domain_whitelist.
func deviceQuery(hs *synapsecfg.Config, limiter *backoff.Limiter) (devicelists.Query, error) {
	line, err := hs.SigningKeyLine()
	if err != nil {
		return nil, err
	}
	fc, err := fedclient.New(hs.ServerName, line, nil, deviceQueryTimeout)
	if err != nil {
		return nil, err
	}
	whitelist := hs.DomainWhitelist
	return func(ctx context.Context, dest, userID string) (json.RawMessage, error) {
		if whitelist != nil && !whitelist[dest] {
			return nil, devicelists.ErrFederationDenied
		}
		var out json.RawMessage
		err := limiter.Do(ctx, dest, backoff.Options{}, func() (*http.Response, error) {
			return fedclient.Attempt(ctx, deviceQueryTimeout, func(ctx context.Context) (*http.Response, error) {
				_, resp, err := fc.MakeFullRequest(ctx, federation.RequestParams{
					ServerName:   dest,
					Method:       http.MethodGet,
					Path:         federation.URLPath{"v1", "user", "devices", userID},
					Authenticate: true,
					ResponseJSON: &out,
				})
				return resp, err
			})
		})
		if err != nil {
			return nil, err
		}
		if len(out) == 0 {
			return nil, errors.New("devicelists: empty answer")
		}
		return out, nil
	}, nil
}

// stateAtEvent is get_state_ids_for_event(await_full_state=False) for the
// device-lists writer: the event's state group, through the shared state
// cache; the members only.
func stateAtEvent(ctx context.Context, dsn string, states *statecache.Cache) (devicelists.StateAtEvent, func(), error) {
	st, err := pdustore.Open(ctx, dsn, 2)
	if err != nil {
		return nil, nil, fmt.Errorf("device lists: state reader: %w", err)
	}
	st.SetStateCache(states)
	return func(ctx context.Context, eventID string) (map[string]string, error) {
		g, err := st.StateGroupForEvent(ctx, eventID)
		if err != nil {
			return nil, err
		}
		m, err := st.CachedStateForGroup(ctx, g)
		if err != nil {
			return nil, err
		}
		out := map[string]string{}
		for k, ev := range m {
			if k.Type == "m.room.member" {
				out[k.StateKey] = ev
			}
		}
		return out, nil
	}, st.Close, nil
}

// workerLocks is inbound's lock store as devicelists.Locker; nil without
// it (inbound not writing), which sends /keys/upload to main.
func workerLocks(l *inboundlocks.Locks) devicelists.Locker {
	if l == nil {
		return nil
	}
	return lockerFunc(func(ctx context.Context, name, key string) (func(), error) {
		lk, err := l.Acquire(ctx, name, key)
		if err != nil {
			return nil, err
		}
		return func() { lk.Release(context.Background()) }, nil
	})
}

type lockerFunc func(ctx context.Context, name, key string) (func(), error)

func (f lockerFunc) Acquire(ctx context.Context, name, key string) (func(), error) {
	return f(ctx, name, key)
}

// deviceAccountData is remove_account_data_for_user for the device
// deletes: the account-data writer in this process, else one over
// replication.
func deviceAccountData(local *accountdata.App, remote accountDataWriter) devicelists.AccountDataRemover {
	if local != nil && local.Writing() {
		return localAccountData{local}
	}
	return remote
}

type localAccountData struct{ a *accountdata.App }

func (l localAccountData) RemoveUserAccountData(ctx context.Context, userID, dataType string) error {
	_, err := l.a.RemoveUser(ctx, "devices", userID, dataType)
	return err
}

// toDeviceToken is get_current_token().to_device_key: the writer's in
// this process, else the greatest ID either to-device table holds.
func toDeviceToken(td *todevice.App, ddb *devicelists.DB) func() int64 {
	return func() int64 {
		if td != nil && td.Writing() {
			return td.CurrentToken()
		}
		n, _ := ddb.ToDeviceMax(context.Background())
		return n
	}
}

// deviceStream is get_device_stream_token().stream for GET /user/devices:
// our generator's position while we write device_lists; otherwise the
// lowest position the bus has shown us of the writers homeserver.yaml
// names now (a follower's persisted-upto token), known only once every
// one of them has been seen since start. Unknown sends the request to main.
func deviceStream(dl *devicelists.App, positions *replserver.Tracker, hs func() *synapsecfg.Config) matrixstate.DeviceStream {
	return func() (int64, bool) {
		if pos, ok := dl.Position(); ok {
			return pos, true
		}
		if positions == nil {
			return 0, false
		}
		var low int64
		for i, w := range hs().Writers.DeviceLists {
			pos, ok := positions.Seen(devicelists.StreamName, w)
			if !ok {
				return 0, false
			}
			if i == 0 || pos < low {
				low = pos
			}
		}
		return low, len(hs().Writers.DeviceLists) > 0
	}
}
