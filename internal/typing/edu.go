package typing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/edutype"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
)

// ErrNotWriter is a typing EDU for a process that does not write typing.
var ErrNotWriter = errors.New("typing: this process is not the typing writer")

// ReceiveEDU is _recv_edu (typing.py:397): a remote server says one of its
// users is or is not typing. It is dropped, as Synapse drops it, when we are
// not in the room, the user is not the origin's, the user ID is not
// compliant, or the origin is not in the room. An error is what Synapse
// would raise: a missing key, or a user ID that does not parse.
func (a *App) ReceiveEDU(ctx context.Context, origin string, content json.RawMessage) error {
	state := a.state.Load()
	if state == nil {
		edus.WithLabelValues("not_writer").Inc()
		return ErrNotWriter
	}
	var c map[string]json.RawMessage
	if err := json.Unmarshal(content, &c); err != nil {
		edus.WithLabelValues("error").Inc()
		return fmt.Errorf("typing: EDU content: %w", err)
	}
	roomID, err := stringKey(c, "room_id")
	if err != nil {
		edus.WithLabelValues("error").Inc()
		return err
	}
	userID, err := stringKey(c, "user_id")
	if err != nil {
		edus.WithLabelValues("error").Inc()
		return err
	}

	in, err := a.store.HostJoined(ctx, roomID, a.serverName)
	if err != nil {
		edus.WithLabelValues("error").Inc()
		return fmt.Errorf("typing: is_host_in_room: %w", err)
	}
	if !in {
		edus.WithLabelValues("not_in_room").Inc()
		a.log.Info().Str("room", roomID).Str("origin", origin).
			Msg("Ignoring typing update: we are not in the room")
		return nil
	}
	if cerr := checkUserID(userID); cerr != nil {
		edus.WithLabelValues("error").Inc()
		return &replserver.Error{Code: cerr.status, Errcode: cerr.body["errcode"].(string), Msg: cerr.body["error"].(string)}
	}
	domain := domainOf(userID)
	if domain != origin {
		edus.WithLabelValues("wrong_origin").Inc()
		a.log.Info().Str("origin", origin).Str("user", userID).Msg("Got typing update with bad 'user_id'")
		return nil
	}
	if !compliantUserID(userID) {
		edus.WithLabelValues("non_compliant").Inc()
		a.log.Info().Str("origin", origin).Str("user", userID).Msg("Dropping typing update for non-compliant user ID")
		return nil
	}
	hosts, err := a.store.HostsApprox(ctx, roomID)
	if err != nil {
		edus.WithLabelValues("error").Inc()
		return fmt.Errorf("typing: current hosts: %w", err)
	}
	if _, ok := hosts[domain]; !ok {
		edus.WithLabelValues("origin_not_in_room").Inc()
		return nil
	}
	rawTyping, ok := c["typing"]
	if !ok {
		edus.WithLabelValues("error").Inc()
		return errors.New("typing: EDU has no 'typing'") // KeyError
	}
	state.Remote(roomID, userID, truthy(rawTyping))
	edus.WithLabelValues("applied").Inc()
	return nil
}

// stringKey is content[key], which must be there (KeyError) and, to be of
// any use, a string.
func stringKey(c map[string]json.RawMessage, key string) (string, error) {
	raw, ok := c[key]
	if !ok {
		return "", fmt.Errorf("typing: EDU has no %q", key)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("typing: EDU %q is not a string", key)
	}
	return s, nil
}

// compliantUserID is is_compliant_user_id: a localpart that is not empty and
// all U+0021 to U+007E.
func compliantUserID(userID string) bool {
	if !strings.HasPrefix(userID, "@") {
		return false
	}
	local, _, _ := strings.Cut(userID[1:], ":")
	if local == "" {
		return false
	}
	for _, r := range local {
		if r < 0x21 || r > 0x7e {
			return false
		}
	}
	return true
}

// fedSendEDU is fed_send_edu (replication/http/federation.py), which
// another process calls with a typing EDU it received over federation. The
// EDU is handled as Synapse's registry handles it (on_edu): an error in the
// handler is the request's error.
//
// Only m.typing is handled here. Synapse's on_edu would forward any other
// type to its writer; nothing sends this process another type, since
// stream_writers names it only for typing, and one that arrives is a 500.
func (a *App) fedSendEDU() replserver.Endpoint {
	return replserver.Endpoint{
		Name:           "fed_send_edu",
		Method:         http.MethodPost,
		PathArgs:       []string{"edu_type"},
		Cache:          true,
		WaitForStreams: true,
		Handle: func(ctx context.Context, req replserver.Request) (int, map[string]any, error) {
			if req.Args["edu_type"] != edutype.Typing {
				return 0, nil, fmt.Errorf("typing: fed_send_edu for %q", req.Args["edu_type"])
			}
			var origin string
			if err := json.Unmarshal(req.Content["origin"], &origin); err != nil {
				return 0, nil, fmt.Errorf("typing: fed_send_edu origin: %w", err)
			}
			content, ok := req.Content["content"]
			if !ok {
				return 0, nil, errors.New("typing: fed_send_edu has no content")
			}
			if err := a.ReceiveEDU(ctx, origin, content); err != nil {
				a.log.Warn().Err(err).Str("origin", origin).Msg("typing: EDU from another process failed")
				return 0, nil, err
			}
			return 200, map[string]any{}, nil
		},
	}
}
