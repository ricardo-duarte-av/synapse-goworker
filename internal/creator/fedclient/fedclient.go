// Package fedclient is what the creator asks other servers: a remote user's
// profile, which update_membership_locked adds to an invite
// (handlers/profile.py, get_displayname and get_avatar_url), and a room's
// policy server's signature. The client is the worker's
// (platform/fedclient: mautrix's discovery and X-Matrix signing); the IP
// ranges are Synapse's federation_ip_range_blocklist.
package fedclient

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/federation"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/pyjson"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/fedclient"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// queryTimeout is make_query's timeout=10000.
const queryTimeout = 10 * time.Second

// Client queries remote servers.
type Client struct {
	fed *federation.Client
}

// New builds a client signing as serverName with a Synapse-format key line
// ("ed25519 <version> <seed>"). allow, when not nil, is
// federation_domain_whitelist: Synapse refuses any other destination.
func New(serverName, keyLine string, allow func(string) bool, ranges synapsecfg.IPRanges) (*Client, error) {
	// The longest request's timeout: each request bounds itself with its
	// context.
	c, err := fedclient.New(serverName, keyLine, allow, policyTimeout)
	if err != nil {
		return nil, err
	}
	c.AllowIP = func(ip net.IP) bool {
		a, ok := netip.AddrFromSlice(ip)
		return ok && !ranges.Blocked(a)
	}
	c.UserAgent = "goevent-creator"
	return &Client{fed: c}, nil
}

// ProfileField is make_query(query_type="profile", field=...) then
// result.get(field): the value, null when absent. ok is false for anything
// Synapse raises on (an unreachable or refusing server, an answer that is
// no object), which update_membership_locked swallows.
func (c *Client) ProfileField(ctx context.Context, userID, field string) (v gjson.Result, ok bool) {
	i := strings.IndexByte(userID, ':')
	if i < 0 {
		return gjson.Result{}, false
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	var raw json.RawMessage
	err := c.fed.Query(ctx, userID[i+1:], "profile", url.Values{"user_id": {userID}, "field": {field}}, &raw)
	if err != nil {
		return gjson.Result{}, false
	}
	// As json.loads holds it.
	if d, err := pyjson.Dedupe(raw); err == nil {
		raw = d
	}
	r := gjson.ParseBytes(raw)
	if !r.IsObject() {
		return gjson.Result{}, false
	}
	return r.Get(gjson.Escape(field)), true
}

// policyTimeout is ask_policy_server_to_sign_event's timeout=30000.
const policyTimeout = 30 * time.Second

// PolicySign is FederationClient.ask_policy_server_to_sign_event: the event
// (as get_pdu_json gives it) POSTed to the policy server's sign endpoint,
// and to the unstable one when the stable one is unknown. It returns the
// answer's status and body; err is a request that got no answer at all.
func (c *Client) PolicySign(ctx context.Context, server string, event []byte) (status int, body []byte, err error) {
	ctx, cancel := context.WithTimeout(ctx, policyTimeout)
	defer cancel()
	status, body, err = c.post(ctx, server, mautrix.BaseURLPath{"_matrix", "policy", "v1", "sign"}, event)
	if err == nil && unknownEndpoint(status, body) {
		status, body, err = c.post(ctx, server, mautrix.BaseURLPath{"_matrix", "policy", "unstable", "org.matrix.msc4284", "sign"}, event)
	}
	return status, body, err
}

func (c *Client) post(ctx context.Context, server string, path mautrix.BaseURLPath, event []byte) (int, []byte, error) {
	body, resp, err := c.fed.MakeFullRequest(ctx, federation.RequestParams{
		ServerName:   server,
		Method:       http.MethodPost,
		Path:         path,
		Authenticate: true,
		RequestJSON:  json.RawMessage(event),
	})
	switch {
	case resp != nil && resp.StatusCode >= 300:
		// mautrix reports a non-2xx answer as an error; it is an answer.
		return resp.StatusCode, body, nil
	case err != nil:
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

// unknownEndpoint is is_unknown_endpoint (http/client.py).
func unknownEndpoint(status int, body []byte) bool {
	errcode := gjson.GetBytes(body, "errcode").Str
	switch status {
	case 404, 405:
		return len(body) == 0 || body[0] != '{' || errcode == "M_UNRECOGNIZED"
	case 400:
		return errcode == "M_UNRECOGNIZED"
	}
	return false
}
