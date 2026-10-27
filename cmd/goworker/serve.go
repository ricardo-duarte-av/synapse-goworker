package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/config"
	senderapp "github.com/ricardo-duarte-av/synapse-goworker/internal/sender/app"
	sendercfg "github.com/ricardo-duarte-av/synapse-goworker/internal/sender/synapsecfg"
)

var (
	buildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "goworker_build_info",
		Help: "Build information for the running worker.",
	}, []string{"tag", "commit", "build_time"})
	memoryLimit = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_memory_limit_bytes",
		Help: "The Go runtime's soft memory limit (memory.limit_mb), or 0 when unset.",
	})
	homeserverReloads = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_homeserver_yaml_reloads_total",
		Help: "Re-reads of homeserver.yaml after it changed, by outcome.",
	}, []string{"outcome"})
)

func setRuntimeMetrics(tag, commit, buildTime string, limitMB int) {
	buildInfo.WithLabelValues(tag, commit, buildTime).Set(1)
	memoryLimit.Set(float64(int64(limitMB) << 20))
}

// metricsServer serves /metrics, /health and optionally /debug/pprof. Nil
// when no address is configured.
func metricsServer(cfg config.Metrics) *http.Server {
	if cfg.Addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})
	if cfg.PProf {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return &http.Server{Addr: cfg.Addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
}

// probe is the container healthcheck. The image is distroless and has no
// curl, so the binary probes its own metrics listener.
func probe(cfg *config.Config) error {
	addr := cfg.Listen.Metrics.Addr
	if addr == "" {
		return nil
	}
	if addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + addr + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck returned %s", resp.Status)
	}
	return nil
}

// homeserverPoll is how often homeserver.yaml is checked for an edit. An
// edit is only reported: it is applied when Synapse applies it.
const homeserverPoll = time.Minute

// homeserverSettle is how long after a trigger the file is read: Synapse's
// processes restart in a burst, each sending REPLICATE.
const homeserverSettle = 2 * time.Second

// watchHomeserver re-reads homeserver.yaml and hands it to the sender, so
// that federation_sender_instances alone gives this process the job of
// sending, or takes it away.
//
// It is applied when Synapse applies it -- when a Synapse process restarts,
// which it announces with REPLICATE on the bus -- and not when the file is
// edited: Synapse's own senders read the file only at start, so taking over
// the moment it changed would send alongside a sender that has not yet been
// told to stop. SIGHUP applies it at once, for when no restart is coming.
func watchHomeserver(ctx context.Context, path string, sender *senderapp.Worker, log zerolog.Logger) {
	trigger := make(chan string, 1)
	fire := func(why string) {
		select {
		case trigger <- why:
		default:
		}
	}
	sender.SetOnReplicate(func() { fire("a Synapse process restarted") })
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)

	applied := modTime(path)
	reported := applied
	ticker := time.NewTicker(homeserverPoll)
	defer ticker.Stop()
	for {
		var why string
		select {
		case <-ctx.Done():
			return
		case <-hup:
			why = "SIGHUP"
		case why = <-trigger:
		case <-ticker.C:
			if mt := modTime(path); !mt.Equal(applied) && !mt.Equal(reported) {
				reported = mt
				log.Info().Msg("homeserver.yaml changed; it is applied when a Synapse process " +
					"restarts (REPLICATE), or now on SIGHUP")
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(homeserverSettle):
		}
		mt := modTime(path)
		if mt.Equal(applied) {
			continue
		}
		scfg, err := sendercfg.Load(path)
		if err != nil {
			// Keep the last good copy: a half-written file must not stop a
			// working sender.
			homeserverReloads.WithLabelValues("error").Inc()
			log.Warn().Err(err).Str("trigger", why).Msg("could not re-read homeserver.yaml; keeping the last copy")
			continue
		}
		applied, reported = mt, mt
		homeserverReloads.WithLabelValues("ok").Inc()
		log.Info().Str("trigger", why).Msg("applied the changed homeserver.yaml")
		sender.SetSynapse(scfg)
	}
}

func modTime(path string) time.Time {
	st, err := os.Stat(path)
	if err != nil {
		return time.Time{}
	}
	return st.ModTime()
}
