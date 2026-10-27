package clientauth

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cache"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// Publisher sends one replication command on a channel.
type Publisher interface {
	Publish(ctx context.Context, channel, line string) error
}

// Redis publishes on homeserver.yaml's Redis. It never subscribes: a
// subscribed connection cannot publish.
type Redis struct{ client *redis.Client }

// NewRedis connects as homeserver.yaml's redis section says.
func NewRedis(cfg synapsecfg.Redis) *Redis {
	opts := &redis.Options{Addr: cfg.Addr, Username: cfg.Username, Password: cfg.Password, DB: cfg.DBID}
	if cfg.Socket != "" {
		opts.Network = "unix"
		opts.Addr = cfg.Socket
	}
	return &Redis{client: redis.NewClient(opts)}
}

// Publish is redis.py's _async_send_command.
func (r *Redis) Publish(ctx context.Context, channel, line string) error {
	return r.client.Publish(ctx, channel, line).Err()
}

// Close closes the connection.
func (r *Redis) Close() error { return r.client.Close() }

// lastSeenGranularity is LAST_SEEN_GRANULARITY (client_ips.py): one record
// per user, token and address every two minutes.
const lastSeenGranularity = 120 * time.Second

// userIPs is insert_client_ip on a worker that does not write client_ips:
// rate-limited, then sent to the one that does as USER_IP, on the
// "<server_name>/USER_IP" channel (UserIpCommand.redis_channel_name).
type userIPs struct {
	pub      Publisher
	channel  string
	lastSeen *cache.LRU[[sha256.Size]byte, int64]
	queue    chan string
	stop     chan struct{}
	done     chan struct{}
	log      zerolog.Logger
}

func newUserIPs(pub Publisher, serverName string, log zerolog.Logger) *userIPs {
	u := &userIPs{
		pub:     pub,
		channel: serverName + "/USER_IP",
		// client_ip_last_seen is an LruCache of 50,000 entries; each here
		// is a hash and a timestamp.
		lastSeen: cache.NewLRU[[sha256.Size]byte, int64]("client_ip_last_seen", 50000*96,
			func(int64) int64 { return 96 }),
		queue: make(chan string, 1024),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
		log:   log,
	}
	go u.run()
	return u
}

// insert never blocks a request: a full queue drops the record, as a
// replication connection that is down would.
func (u *userIPs) insert(userID, token, ip, userAgent, deviceID string) {
	// The sliding-sync proxy's own polling is not a user's activity.
	if userAgent == "sync-v3-proxy-" {
		return
	}
	now := time.Now().UnixMilli()
	key := sha256.Sum256([]byte(userID + "\x00" + token + "\x00" + ip))
	if last, ok := u.lastSeen.Get(key); ok && now-last < lastSeenGranularity.Milliseconds() {
		return
	}
	u.lastSeen.Add(key, now)

	var device any
	if deviceID != "" {
		device = deviceID
	}
	// UserIpCommand.to_line: the user, then a JSON list.
	args, err := json.Marshal([]any{token, ip, userAgent, device, now})
	if err != nil {
		return
	}
	select {
	case u.queue <- "USER_IP " + userID + " " + string(args):
	default:
		userIPRecords.WithLabelValues("dropped").Inc()
	}
}

func (u *userIPs) run() {
	defer close(u.done)
	for {
		var line string
		select {
		case line = <-u.queue:
		case <-u.stop:
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := u.pub.Publish(ctx, u.channel, line)
		cancel()
		if err != nil {
			userIPRecords.WithLabelValues("failed").Inc()
			u.log.Debug().Err(err).Msg("client auth: USER_IP not sent")
			continue
		}
		userIPRecords.WithLabelValues("sent").Inc()
	}
}

// close stops sending; what is still queued is dropped.
func (u *userIPs) close() {
	close(u.stop)
	<-u.done
}
