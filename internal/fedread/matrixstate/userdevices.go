package matrixstate

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/e2ekeys"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
)

// ErrNoDeviceStream is a device_lists stream position this process does not
// know: it does not write the stream and has seen no writer's position on
// the bus since it started. Synapse's answer carries the position, so the
// request is Synapse's.
var ErrNoDeviceStream = errors.New("matrixstate: device_lists stream position unknown")

// DeviceStream is get_device_stream_token().stream: the device_lists
// stream's current position, and whether it is known.
type DeviceStream func() (int64, bool)

// SetDeviceStream gives UserDevices the stream position. Until it is set,
// every query is ErrNoDeviceStream.
func (r *Resolver) SetDeviceStream(f DeviceStream) { r.deviceStream.Store(&f) }

// UserDevices is on_federation_query_user_devices (handlers/device.py,
// v1.164.1) for a local user already checked: the device stream's position,
// taken before the devices are read as Synapse takes it, then
// get_e2e_device_keys_for_federation_query's devices and the master and
// self-signing keys with no requester's signatures (null when absent).
// The answer is encoded as Synapse's json_encoder writes it.
func (r *Resolver) UserDevices(ctx context.Context, userID string) ([]byte, error) {
	f := r.deviceStream.Load()
	if f == nil {
		return nil, ErrNoDeviceStream
	}
	streamID, ok := (*f)()
	if !ok {
		return nil, ErrNoDeviceStream
	}
	q := e2ekeys.PoolQuerier(r.db.Pool())
	devices, err := e2ekeys.FederationDevices(ctx, q, userID, r.keyQuery.DisplayNames)
	if err != nil {
		return nil, err
	}
	master, selfSigning, _, err := e2ekeys.CrossSigning(ctx, q, []string{userID}, "")
	if err != nil {
		return nil, err
	}
	user, err := json.Marshal(userID)
	if err != nil {
		return nil, err
	}
	devs, err := json.Marshal(devices)
	if err != nil {
		return nil, err
	}
	raw := `{"user_id":` + string(user) +
		`,"stream_id":` + strconv.FormatInt(streamID, 10) +
		`,"devices":` + string(devs) +
		`,"master_key":` + rawOrNull(master[userID]) +
		`,"self_signing_key":` + rawOrNull(selfSigning[userID]) + `}`
	out, err := pyjson.Encode([]byte(raw))
	if err != nil {
		return nil, err
	}
	return []byte(out), nil
}

func rawOrNull(v json.RawMessage) string {
	if v == nil {
		return "null"
	}
	return string(v)
}

// deviceStreamHolder is the Resolver's DeviceStream, set after the
// device-lists writer exists (which is built after the federation reads).
type deviceStreamHolder = atomic.Pointer[DeviceStream]
