package typing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/userid"
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
		return &replserver.Error{Code: cerr.Status, Errcode: cerr.Body["errcode"].(string), Msg: cerr.Body["error"].(string)}
	}
	domain := domainOf(userID)
	if domain != origin {
		edus.WithLabelValues("wrong_origin").Inc()
		a.log.Info().Str("origin", origin).Str("user", userID).Msg("Got typing update with bad 'user_id'")
		return nil
	}
	if !userid.Compliant(userID) {
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
	state.Remote(roomID, userID, clientreq.Truthy(rawTyping))
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
