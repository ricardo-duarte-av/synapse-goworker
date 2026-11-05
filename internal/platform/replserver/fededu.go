package replserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// EDUHandler handles one EDU type in this process, as a handler
// registered with Synapse's FederationHandlerRegistry does.
type EDUHandler func(ctx context.Context, origin string, content json.RawMessage) error

// FedSendEDU is fed_send_edu (replication/http/federation.py), which other
// processes call with an EDU they received over federation for a stream
// this process writes. The handler for its type runs as on_edu runs it: its
// error is the request's.
//
// handlers is looked up on every request, so a part that stops writing its
// stream stops being found. Synapse's on_edu would forward a type it has no
// handler for to its writer; nothing sends this process such a type, since
// stream_writers names it only for its own, and one that arrives is a 500.
func FedSendEDU(handlers func(eduType string) EDUHandler) Endpoint {
	return Endpoint{
		Name:           "fed_send_edu",
		Method:         http.MethodPost,
		PathArgs:       []string{"edu_type"},
		Cache:          true,
		WaitForStreams: true,
		Handle: func(ctx context.Context, req Request) (int, map[string]any, error) {
			eduType := req.Args["edu_type"]
			h := handlers(eduType)
			if h == nil {
				return 0, nil, fmt.Errorf("fed_send_edu: no handler here for %q", eduType)
			}
			var origin string
			if err := json.Unmarshal(req.Content["origin"], &origin); err != nil {
				return 0, nil, fmt.Errorf("fed_send_edu: origin: %w", err)
			}
			content, ok := req.Content["content"]
			if !ok {
				return 0, nil, errors.New("fed_send_edu: no content")
			}
			if err := h(ctx, origin, content); err != nil {
				return 0, nil, err
			}
			return 200, map[string]any{}, nil
		},
	}
}
