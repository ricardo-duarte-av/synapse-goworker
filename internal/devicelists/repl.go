package devicelists

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
)

// Endpoints are replication/http/devices.py's servlets, which
// every other process calls on the device-lists writer: POST, CACHE =
// False, WAIT_FOR_STREAMS.
func (a *App) Endpoints() []replserver.Endpoint {
	return []replserver.Endpoint{
		{
			Name: "notify_device_update", Method: http.MethodPost, PathArgs: []string{"user_id"}, WaitForStreams: true,
			Handle: func(ctx context.Context, req replserver.Request) (int, map[string]any, error) {
				raw, ok := req.Content["device_ids"]
				if !ok {
					return 0, nil, errors.New(`devicelists: no "device_ids"`) // KeyError
				}
				var ids []string
				if err := json.Unmarshal(raw, &ids); err != nil {
					return 0, nil, err
				}
				if err := a.NotifyDeviceUpdate(ctx, req.Args["user_id"], ids); err != nil {
					return 0, nil, err
				}
				return 200, map[string]any{}, nil
			},
		},
		{
			Name: "notify_user_signature_update", Method: http.MethodPost, PathArgs: []string{"from_user_id"}, WaitForStreams: true,
			Handle: func(ctx context.Context, req replserver.Request) (int, map[string]any, error) {
				raw, ok := req.Content["user_ids"]
				if !ok {
					return 0, nil, errors.New(`devicelists: no "user_ids"`)
				}
				enc, err := pyjson.Encode(raw)
				if err != nil {
					return 0, nil, err
				}
				if err := a.NotifyUserSignatureUpdate(ctx, req.Args["from_user_id"], enc); err != nil {
					return 0, nil, err
				}
				return 200, map[string]any{}, nil
			},
		},
		{
			// CACHE = True, no path arguments.
			Name: "multi_user_device_resync", Method: http.MethodPost, Cache: true, WaitForStreams: true,
			Handle: func(ctx context.Context, req replserver.Request) (int, map[string]any, error) {
				raw, ok := req.Content["user_ids"]
				if !ok {
					return 0, nil, errors.New(`devicelists: no "user_ids"`)
				}
				var users []string
				if err := json.Unmarshal(raw, &users); err != nil {
					return 0, nil, err
				}
				res, err := a.MultiUserDeviceResync(ctx, users, true)
				if err != nil {
					return 0, nil, err
				}
				out := make(map[string]any, len(res))
				for u, r := range res {
					if r == nil {
						out[u] = nil
					} else {
						out[u] = r
					}
				}
				return 200, out, nil
			},
		},
		{
			Name: "device_handle_room_un_partial_stated", Method: http.MethodPost, PathArgs: []string{"room_id"},
			Cache: true, WaitForStreams: true,
			Handle: func(ctx context.Context, req replserver.Request) (int, map[string]any, error) {
				if err := a.HandleRoomUnPartialStated(ctx, req.Args["room_id"]); err != nil {
					return 0, nil, err
				}
				return 200, map[string]any{}, nil
			},
		},
		{
			Name: "handle_new_device_update", Method: http.MethodPost, WaitForStreams: true,
			Handle: func(ctx context.Context, req replserver.Request) (int, map[string]any, error) {
				a.HandleNewDeviceUpdate()
				return 200, map[string]any{}, nil
			},
		},
	}
}
