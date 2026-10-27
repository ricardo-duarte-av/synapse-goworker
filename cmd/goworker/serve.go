package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
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

// homeserverPoll is how often homeserver.yaml is checked for a change. An
// edit only takes effect in Synapse when main restarts, so a minute's delay
// here is well inside that.
const homeserverPoll = time.Minute

// watchHomeserver re-reads homeserver.yaml when it changes and hands it to
// the sender, so that listing this instance in federation_sender_instances
// (or removing it) gives it the job or takes it away without a restart.
func watchHomeserver(ctx context.Context, path string, sender *senderapp.Worker, log zerolog.Logger) {
	last := modTime(path)
	ticker := time.NewTicker(homeserverPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		mt := modTime(path)
		if mt.Equal(last) {
			continue
		}
		scfg, err := sendercfg.Load(path)
		if err != nil {
			// Keep the last good copy: a half-written file must not stop
			// a working sender.
			homeserverReloads.WithLabelValues("error").Inc()
			log.Warn().Err(err).Msg("homeserver.yaml changed but could not be read; keeping the last copy")
			continue
		}
		last = mt
		homeserverReloads.WithLabelValues("ok").Inc()
		log.Info().Msg("homeserver.yaml changed; re-read")
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
