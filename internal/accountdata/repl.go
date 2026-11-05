package accountdata

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
)

// Endpoints are replication/http/account_data.py's servlets, which every
// other process calls on the account-data writer: POST, CACHE = False,
// WAIT_FOR_STREAMS (the default), each answering {"max_stream_id": n}. The
// two removes exist only with MSC3391, as Synapse registers them.
func (a *App) Endpoints() []replserver.Endpoint {
	content := func(req replserver.Request) (json.RawMessage, error) {
		c, ok := req.Content["content"]
		if !ok {
			return nil, errors.New(`accountdata: replication request has no "content"`) // KeyError
		}
		return c, nil
	}
	ep := func(name string, args []string, fn func(ctx context.Context, req replserver.Request) (int64, error)) replserver.Endpoint {
		return replserver.Endpoint{
			Name: name, Method: http.MethodPost, PathArgs: args, WaitForStreams: true,
			Handle: func(ctx context.Context, req replserver.Request) (int, map[string]any, error) {
				n, err := fn(ctx, req)
				if errors.Is(err, ErrIgnoreSelf) {
					return 0, nil, &replserver.Error{Code: 400, Errcode: "M_INVALID_PARAM", Msg: ErrIgnoreSelf.Error()}
				}
				if err != nil {
					return 0, nil, err
				}
				return 200, map[string]any{"max_stream_id": n}, nil
			},
		}
	}
	out := []replserver.Endpoint{
		ep("add_user_account_data", []string{"user_id", "account_data_type"}, func(ctx context.Context, req replserver.Request) (int64, error) {
			c, err := content(req)
			if err != nil {
				return 0, err
			}
			return a.AddUser(ctx, CallerReplication, req.Args["user_id"], req.Args["account_data_type"], c)
		}),
		ep("add_room_account_data", []string{"user_id", "room_id", "account_data_type"}, func(ctx context.Context, req replserver.Request) (int64, error) {
			c, err := content(req)
			if err != nil {
				return 0, err
			}
			return a.AddRoom(ctx, CallerReplication, req.Args["user_id"], req.Args["room_id"], req.Args["account_data_type"], c)
		}),
		ep("add_tag", []string{"user_id", "room_id", "tag"}, func(ctx context.Context, req replserver.Request) (int64, error) {
			c, err := content(req)
			if err != nil {
				return 0, err
			}
			return a.AddTag(ctx, CallerReplication, req.Args["user_id"], req.Args["room_id"], req.Args["tag"], c)
		}),
		ep("remove_tag", []string{"user_id", "room_id", "tag"}, func(ctx context.Context, req replserver.Request) (int64, error) {
			return a.RemoveTag(ctx, CallerReplication, req.Args["user_id"], req.Args["room_id"], req.Args["tag"])
		}),
	}
	if a.msc3391() {
		out = append(out,
			ep("remove_user_account_data", []string{"user_id", "account_data_type"}, func(ctx context.Context, req replserver.Request) (int64, error) {
				return a.RemoveUser(ctx, CallerReplication, req.Args["user_id"], req.Args["account_data_type"])
			}),
			ep("remove_room_account_data", []string{"user_id", "room_id", "account_data_type"}, func(ctx context.Context, req replserver.Request) (int64, error) {
				return a.RemoveRoom(ctx, CallerReplication, req.Args["user_id"], req.Args["room_id"], req.Args["account_data_type"])
			}),
		)
	}
	return out
}
