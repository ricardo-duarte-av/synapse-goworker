package synapsecfg

import (
	"fmt"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Delayed is what delayed events (MSC4140) read from homeserver.yaml
// (config/server.py, v1.163.0), and the request ratelimiter adding one
// counts against (rc_message, config/ratelimiting.py).
type Delayed struct {
	// MaxDelayMS is max_event_delay_duration, 0 when unset.
	MaxDelayMS int64
	// PerUser is experimental_features.msc4140_max_delayed_events_per_user,
	// default 100.
	PerUser int
	// RcMessage is rc_message.
	RcMessage Ratelimit
}

// Enabled is msc4140_enabled.
func (d Delayed) Enabled() bool { return d.PerUser > 0 && d.MaxDelayMS > 0 }

type delayedRaw struct {
	MaxEventDelayDuration yaml.Node `yaml:"max_event_delay_duration"`
	RcMessage             yaml.Node `yaml:"rc_message"`
	RcMessagesPerSecond   any       `yaml:"rc_messages_per_second"`
	RcMessageBurstCount   any       `yaml:"rc_message_burst_count"`
}

func delayedSettings(b []byte, experimental map[string]any) (Delayed, error) {
	var r delayedRaw
	if err := yaml.Unmarshal(b, &r); err != nil {
		return Delayed{}, fmt.Errorf("synapsecfg: delayed events: %w", err)
	}
	d := Delayed{PerUser: 100}
	if r.MaxEventDelayDuration.Kind == yaml.ScalarNode && r.MaxEventDelayDuration.Tag != "!!null" {
		ms, err := durationNode(r.MaxEventDelayDuration)
		if err != nil {
			return Delayed{}, err
		}
		d.MaxDelayMS = ms
	}
	if v, ok := experimental["msc4140_max_delayed_events_per_user"]; ok {
		n, ok := v.(int)
		if !ok || n < 0 {
			return Delayed{}, fmt.Errorf("synapsecfg: msc4140_max_delayed_events_per_user must be a non-negative integer")
		}
		d.PerUser = n
	}
	var err error
	if r.RcMessage.Kind != 0 {
		if d.RcMessage, err = ratelimit("rc_message", nodeMap(&r.RcMessage), 0.2, 10); err != nil {
			return Delayed{}, err
		}
		return d, nil
	}
	// The old keys, when rc_message is absent.
	legacy := map[string]any{}
	if r.RcMessagesPerSecond != nil {
		legacy["per_second"] = r.RcMessagesPerSecond
	}
	if r.RcMessageBurstCount != nil {
		legacy["burst_count"] = r.RcMessageBurstCount
	}
	if d.RcMessage, err = ratelimit("rc_messages", legacy, 0.2, 10); err != nil {
		return Delayed{}, err
	}
	return d, nil
}

// durationNode is Config.parse_duration on a YAML node: an int is milliseconds; a string
// takes a suffix of s, m, h, d, w or y, none being milliseconds.
func durationNode(n yaml.Node) (int64, error) {
	if n.Tag == "!!int" {
		return strconv.ParseInt(n.Value, 10, 64)
	}
	v := n.Value
	if v == "" {
		return 0, fmt.Errorf("synapsecfg: empty duration")
	}
	sizes := map[byte]int64{'s': 1000, 'm': 60000, 'h': 3600000, 'd': 86400000, 'w': 604800000, 'y': 31536000000}
	size := int64(1)
	if s, ok := sizes[v[len(v)-1]]; ok {
		v, size = v[:len(v)-1], s
	}
	x, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("synapsecfg: duration %q: %w", n.Value, err)
	}
	return x * size, nil
}
