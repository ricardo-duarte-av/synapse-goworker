package todevice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/userid"
)

// Sizes (api/constants.py).
const (
	softMaxEDUSize         = 65536
	maxToDeviceContentSize = softMaxEDUSize
	// emptyEDUSize is _EMPTY_EDU_SIZE: len(encode_canonical_json(
	// _create_new_to_device_edu("", "", {}))), with its 16-letter
	// message_id.
	emptyEDUSize = 113
)

// resyncTimeout bounds the background device resync.
const resyncTimeout = time.Minute

// RoomKeyRequest is ToDeviceEventTypes.RoomKeyRequest.
const RoomKeyRequest = "m.room_key_request"

// ErrNotWriter is a message for a process that does not write to-device.
var ErrNotWriter = errors.New("todevice: this process is not the to-device writer")

// randomLetters is random_string_insecure_fast(16).
func randomLetters() string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 16)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}

// member is a key of a JSON object and its value, in the order a Python
// dict holds them (pyjson.Dedupe first).
type member struct {
	key   string
	value gjson.Result
}

func members(v gjson.Result) []member {
	var out []member
	v.ForEach(func(k, x gjson.Result) bool {
		out = append(out, member{k.Str, x})
		return true
	})
	return out
}

// rawObject renders members as a JSON object in their order.
func rawObject(ms []member) []byte {
	var b strings.Builder
	b.WriteByte('{')
	for i, m := range ms {
		if i > 0 {
			b.WriteByte(',')
		}
		k, _ := json.Marshal(m.key)
		b.Write(k)
		b.WriteByte(':')
		b.WriteString(m.value.Raw)
	}
	b.WriteByte('}')
	return []byte(b.String())
}

// message is {"content", "type", "sender"} as json_encoder writes it.
func message(content gjson.Result, msgType, sender string) (Message, error) {
	t, _ := json.Marshal(msgType)
	s, _ := json.Marshal(sender)
	enc, err := pyjson.Encode([]byte(`{"content":` + content.Raw + `,"type":` + string(t) + `,"sender":` + string(s) + `}`))
	if err != nil {
		return Message{}, err
	}
	return Message{JSON: enc, NotObject: !content.IsObject()}, nil
}

// errInternal is an exception Synapse does not catch: a 500.
type errInternal struct{ msg string }

func (e *errInternal) Error() string { return "todevice: " + e.msg }

// allowed is Ratelimiter.can_do_action for a requester: an appservice
// that is not rate limited, and a user whose ratelimit_override has no
// rate, always may.
func (a *App) allowed(ctx context.Context, v *clientauth.Verdict, key string) (bool, error) {
	if v != nil {
		if v.AppServiceID != "" && v.AppServiceUnlimited {
			return true, nil
		}
		entity := v.Entity
		if entity == "" {
			entity = v.UserID
		}
		exempt, err := a.Store.RatelimitExempt(ctx, entity)
		if err != nil {
			return false, err
		}
		if exempt {
			return true, nil
		}
	}
	ok, _ := a.keyRequests.CanDo(key, true)
	return ok, nil
}

// Send is send_device_message: a client's messages, of one type, to local
// devices (one stream ID) and to other servers (an EDU per destination,
// split to fit, each its own stream ID). An error with a status is what
// the client gets; any other is a 500.
func (a *App) Send(ctx context.Context, v clientauth.Verdict, msgType string, messagesRaw gjson.Result) error {
	if !a.writing {
		return ErrNotWriter
	}
	sender := v.UserID
	if !messagesRaw.IsObject() {
		// content["messages"].items(): AttributeError.
		return &errInternal{"messages is not an object"}
	}
	var (
		local       []UserMessages
		remoteOrder []string
		remote      = map[string][]member{} // destination -> recipient -> by_device
	)
	for _, u := range members(messagesRaw) {
		if !userid.Valid(u.key) {
			messages.WithLabelValues("local", "dropped").Inc()
			a.Log.Warn().Str("user", u.key).Msg("Ignoring attempt to send device message to invalid user")
			continue
		}
		if !u.value.IsObject() {
			return &errInternal{"messages[" + u.key + "] is not an object"}
		}
		devices := members(u.value)
		for _, d := range devices {
			n, err := pyjson.CanonicalLen([]byte(d.value.Raw))
			if err != nil {
				return &errInternal{err.Error()}
			}
			if n > maxToDeviceContentSize {
				return clientreq.New(413, "M_TOO_LARGE", fmt.Sprintf(
					"To-device message for %s:%s is too large to send (%d > %d)", u.key, d.key, n, maxToDeviceContentSize))
			}
			if !d.value.IsObject() {
				// log_kv(... message_content.get(...)): AttributeError.
				return &errInternal{"a to-device message is not an object"}
			}
		}
		if msgType == RoomKeyRequest && u.key != sender {
			ok, err := a.allowed(ctx, &v, sender+"\x00"+v.DeviceID)
			if err != nil {
				return err
			}
			if !ok {
				messages.WithLabelValues("local", "dropped").Add(float64(len(devices)))
				a.Log.Info().Str("sender", sender).Str("user", u.key).Msg("Dropping room_key_request due to rate limit")
				continue
			}
		}
		if userid.Domain(u.key) == a.serverName {
			um := UserMessages{UserID: u.key, ByID: map[string]Message{}}
			for _, d := range devices {
				m, err := message(d.value, msgType, sender)
				if err != nil {
					return &errInternal{err.Error()}
				}
				if _, seen := um.ByID[d.key]; !seen {
					um.Devices = append(um.Devices, d.key)
				}
				um.ByID[d.key] = m
			}
			if len(um.Devices) > 0 {
				local = appendUser(local, um)
				messages.WithLabelValues("local", "local").Add(float64(len(devices)))
			}
			continue
		}
		dest := userid.Domain(u.key)
		if _, ok := remote[dest]; !ok {
			remoteOrder = append(remoteOrder, dest)
		}
		remote[dest] = setMember(remote[dest], member{u.key, u.value})
		messages.WithLabelValues("local", "remote").Add(float64(len(devices)))
	}

	// add_local_messages_from_client_to_device_inbox: an ID even for none.
	if err := a.stored(ctx, func(id int64) error {
		return a.Store.AddLocal(ctx, a.Instance, id, local)
	}); err != nil {
		return err
	}
	for _, dest := range remoteOrder {
		split, err := a.splitEDUs(sender, msgType, remote[dest])
		if err != nil {
			return &errInternal{err.Error()}
		}
		for _, edu := range split {
			if err := a.stored(ctx, func(id int64) error {
				return a.Store.AddRemote(ctx, a.Instance, id, dest, edu, a.Now().UnixMilli())
			}); err != nil {
				return err
			}
			edus.WithLabelValues("out", "queued").Inc()
		}
	}
	if a.OnDestinations != nil && len(remoteOrder) > 0 {
		a.OnDestinations(remoteOrder)
	}
	return nil
}

// appendUser adds um, or replaces an earlier entry for the same user (a
// dict assignment keeps the first position).
func appendUser(l []UserMessages, um UserMessages) []UserMessages {
	for i := range l {
		if l[i].UserID == um.UserID {
			l[i] = um
			return l
		}
	}
	return append(l, um)
}

func setMember(l []member, m member) []member {
	for i := range l {
		if l[i].key == m.key {
			l[i] = m
			return l
		}
	}
	return append(l, m)
}

// stored runs one write under a stream ID, finished whatever happens.
func (a *App) stored(ctx context.Context, fn func(id int64) error) error {
	id, err := a.ids.Next(ctx)
	if err != nil {
		return err
	}
	err = fn(id.Value)
	if ferr := id.Finish(ctx, err == nil); err == nil && ferr != nil {
		err = ferr
	}
	return err
}

// canonicalLenWith is _len_with_wrapping_object.
func canonicalLenWith(ms []member, wrapping int) (int, error) {
	n, err := pyjson.CanonicalLen(rawObject(ms))
	return n + wrapping - 2, err
}

type chunk struct {
	members []member
	size    int
}

// splitToFit is split_dict_to_fit_to_size.
func splitToFit(ms []member, softMax, wrapping int) ([]chunk, error) {
	if len(ms) == 0 {
		return nil, nil
	}
	full, err := canonicalLenWith(ms, wrapping)
	if err != nil {
		return nil, err
	}
	if full <= softMax {
		return []chunk{{ms, full}}, nil
	}
	var out []chunk
	cur := chunk{size: wrapping}
	for _, m := range ms {
		cur.members = append(cur.members, m)
		size, err := canonicalLenWith(cur.members, wrapping)
		if err != nil {
			return nil, err
		}
		if size > softMax && len(cur.members) > 1 {
			cur.members = cur.members[:len(cur.members)-1]
			out = append(out, cur)
			cur = chunk{members: []member{m}, size: wrapping}
			if size, err = canonicalLenWith(cur.members, wrapping); err != nil {
				return nil, err
			}
		}
		cur.size = size
	}
	if len(cur.members) > 0 {
		out = append(out, cur)
	}
	return out, nil
}

// splitEDUs is split_device_messages_into_edus, each EDU's content as
// json_encoder writes it, with the opentracing context the sender adds
// ("{}": tracing is off).
func (a *App) splitEDUs(sender, msgType string, byUser []member) ([]string, error) {
	// Python's len of str: code points.
	base := emptyEDUSize + utf8.RuneCountInString(sender) + utf8.RuneCountInString(msgType)
	var out []string
	add := func(ms []member) error {
		s, _ := json.Marshal(sender)
		t, _ := json.Marshal(msgType)
		id, _ := json.Marshal(a.MessageID())
		raw := `{"sender":` + string(s) + `,"type":` + string(t) + `,"message_id":` + string(id) +
			`,"messages":` + string(rawObject(ms)) + `,"org.matrix.opentracing_context":"{}"}`
		enc, err := pyjson.Encode([]byte(raw))
		if err != nil {
			return err
		}
		out = append(out, enc)
		return nil
	}
	chunks, err := splitToFit(byUser, softMaxEDUSize, base)
	if err != nil {
		return nil, err
	}
	for _, c := range chunks {
		if c.size <= softMaxEDUSize {
			if err := add(c.members); err != nil {
				return nil, err
			}
			continue
		}
		// One recipient too big for an EDU: split by device.
		for _, recipient := range c.members {
			s, _ := json.Marshal(sender)
			t, _ := json.Marshal(msgType)
			k, _ := json.Marshal(recipient.key)
			header := `{"edu_type":"m.direct_to_device","content":{"sender":` + string(s) + `,"type":` + string(t) +
				`,"message_id":"xxxxxxxxxxxxxxxx","messages":{` + string(k) + `:{}}}}`
			subBase, err := pyjson.CanonicalLen([]byte(header))
			if err != nil {
				return nil, err
			}
			parts, err := splitToFit(members(recipient.value), softMaxEDUSize, subBase)
			if err != nil {
				return nil, err
			}
			for _, p := range parts {
				if err := add([]member{{recipient.key, gjson.Parse(string(rawObject(p.members)))}}); err != nil {
					return nil, err
				}
			}
		}
	}
	return out, nil
}

// ReceiveEDU is on_direct_to_device_edu. An error is what Synapse raises,
// which on_edu does not catch: a SynapseError (a recipient not ours) is a
// *replserver.Error, which fed_send_edu answers with its status; anything
// else (a missing key) is a 500.
func (a *App) ReceiveEDU(ctx context.Context, origin string, content json.RawMessage) error {
	if !a.writing {
		edus.WithLabelValues("in", "error").Inc()
		return ErrNotWriter
	}
	err := a.receive(ctx, origin, content)
	if err != nil {
		edus.WithLabelValues("in", "error").Inc()
	}
	return err
}

func (a *App) receive(ctx context.Context, origin string, content json.RawMessage) error {
	content, err := pyjson.Dedupe(content)
	if err != nil {
		return err
	}
	root := gjson.ParseBytes(content)
	if !root.IsObject() {
		return errors.New("todevice: EDU content is not an object")
	}
	senderV := root.Get("sender")
	if !senderV.Exists() {
		return errors.New(`todevice: EDU has no "sender"`) // KeyError
	}
	if senderV.Type != gjson.String {
		return errors.New("todevice: EDU sender is not a string")
	}
	sender := senderV.Str
	if !strings.Contains(sender, ":") {
		// get_domain_from_id: SynapseError(400, "Invalid ID").
		return &replserver.Error{Code: 400, Errcode: "M_UNKNOWN", Msg: "Invalid ID: '" + sender + "'"}
	}
	if userid.Domain(sender) != origin {
		edus.WithLabelValues("in", "dropped").Inc()
		a.Log.Warn().Str("origin", origin).Str("sender", sender).Msg("Dropping device message with spoofed sender")
		return nil
	}
	if !userid.Compliant(sender) {
		edus.WithLabelValues("in", "dropped").Inc()
		a.Log.Warn().Str("origin", origin).Str("sender", sender).Msg("Dropping device message with non-compliant sender")
		return nil
	}
	typeV, idV, msgsV := root.Get("type"), root.Get("message_id"), root.Get("messages")
	for name, v := range map[string]gjson.Result{"type": typeV, "message_id": idV, "messages": msgsV} {
		if !v.Exists() {
			return fmt.Errorf("todevice: EDU has no %q", name) // KeyError
		}
	}
	if typeV.Type != gjson.String || idV.Type != gjson.String {
		// A non-string reaches the database as one (the message_id
		// comparison with text fails) or a dict key: a failure either way.
		return errors.New("todevice: EDU type or message_id is not a string")
	}
	if !msgsV.IsObject() {
		return errors.New("todevice: EDU messages is not an object")
	}
	msgType := typeV.Str
	var local []UserMessages
	for _, u := range members(msgsV) {
		if perr := userid.Check(u.key); perr != nil {
			return &replserver.Error{Code: 400, Errcode: "M_INVALID_PARAM", Msg: perr.Msg}
		}
		if userid.Domain(u.key) != a.serverName {
			a.Log.Warn().Str("user", u.key).Msg("To-device message to non-local user")
			return &replserver.Error{Code: 400, Errcode: "M_UNKNOWN", Msg: "Not a user here"}
		}
		if !clientreq.Truthy(json.RawMessage(u.value.Raw)) {
			continue
		}
		devices := members(u.value)
		if msgType == RoomKeyRequest {
			ok, err := a.allowed(ctx, nil, sender+"\x00(none)")
			if err != nil {
				return err
			}
			if !ok {
				messages.WithLabelValues("remote", "dropped").Add(float64(len(devices)))
				a.Log.Info().Str("sender", sender).Str("user", u.key).Msg("Dropping room_key_request due to rate limit")
				continue
			}
		}
		if !u.value.IsObject() {
			// by_device.items(): AttributeError.
			return errors.New("todevice: EDU messages for a user are not an object")
		}
		um := UserMessages{UserID: u.key, ByID: map[string]Message{}}
		for _, d := range devices {
			m, err := message(d.value, msgType, sender)
			if err != nil {
				return err
			}
			if _, seen := um.ByID[d.key]; !seen {
				um.Devices = append(um.Devices, d.key)
			}
			um.ByID[d.key] = m
		}
		local = appendUser(local, um)
		messages.WithLabelValues("remote", "local").Add(float64(len(devices)))
		if err := a.checkUnknownDevices(ctx, msgType, sender, devices); err != nil {
			return err
		}
	}
	var stored bool
	if err := a.stored(ctx, func(id int64) error {
		var err error
		stored, err = a.Store.AddFromRemote(ctx, a.Instance, id, origin, idV.Str, local, a.Now().UnixMilli())
		return err
	}); err != nil {
		return err
	}
	if stored {
		edus.WithLabelValues("in", "handled").Inc()
	} else {
		edus.WithLabelValues("in", "repeat").Inc()
	}
	return nil
}

// checkUnknownDevices is _check_for_unknown_devices: a room key request
// from a device we do not have cached for a user we share a room with
// marks the user's device cache stale and resyncs it in the background.
func (a *App) checkUnknownDevices(ctx context.Context, msgType, sender string, devices []member) error {
	if msgType != RoomKeyRequest {
		return nil
	}
	requesting := map[string]bool{}
	for _, d := range devices {
		if !d.value.IsObject() {
			return errors.New("todevice: a room key request is not an object") // .get: AttributeError
		}
		r := d.value.Get("requesting_device_id")
		// A missing or non-string ID (None, a number) is never a cached
		// device ID: it counts as unknown.
		key := "\x00" + r.Raw
		if r.Type == gjson.String {
			key = r.Str
		}
		requesting[key] = true
	}
	shares, err := a.Store.SharesRoom(ctx, sender)
	if err != nil {
		return err
	}
	if !shares {
		a.Log.Info().Str("sender", sender).Msg("Received device message from remote device we don't share a room with")
		return nil
	}
	cached, err := a.Store.CachedDevices(ctx, sender)
	if err != nil {
		return err
	}
	unknown := false
	for id := range requesting {
		unknown = unknown || !cached[id]
	}
	if !unknown {
		return nil
	}
	a.Log.Info().Str("sender", sender).Msg("Received device message from remote device not in our cache")
	if err := a.Store.MarkStale(ctx, sender, a.Now().UnixMilli()); err != nil {
		return err
	}
	if a.Resync == nil {
		resyncs.WithLabelValues("skipped").Inc()
		return nil
	}
	go func() {
		rctx, cancel := context.WithTimeout(context.Background(), resyncTimeout)
		defer cancel()
		if err := a.Resync(rctx, sender); err != nil {
			resyncs.WithLabelValues("error").Inc()
			a.Log.Warn().Err(err).Str("user", sender).Msg("device list resync failed")
			return
		}
		resyncs.WithLabelValues("ok").Inc()
	}()
	return nil
}
