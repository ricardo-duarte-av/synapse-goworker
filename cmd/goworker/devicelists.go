package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"maunium.net/go/mautrix/federation"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/devicelists"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/backoff"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/fedclient"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdustore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/statecache"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
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
