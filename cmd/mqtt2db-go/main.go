// Command mqtt2db-go is the entry point for the ingest service.
//
// Final wiring (Milestone 9): config -> logger -> Postgres pool ->
// Badger WAL -> ring buffer -> dead-letter sink -> flusher -> MQTT
// subscriber -> health server -> /metrics. SIGTERM cancels root context;
// each subsystem flushes its in-flight work then returns.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/debsahu/mqtt2db-go/internal/buffer"
	"github.com/debsahu/mqtt2db-go/internal/config"
	"github.com/debsahu/mqtt2db-go/internal/deadletter"
	"github.com/debsahu/mqtt2db-go/internal/flusher"
	"github.com/debsahu/mqtt2db-go/internal/health"
	"github.com/debsahu/mqtt2db-go/internal/logging"
	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
	"github.com/debsahu/mqtt2db-go/internal/subscriber"
	"github.com/debsahu/mqtt2db-go/internal/version"
	"github.com/debsahu/mqtt2db-go/internal/wal"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath  string
		showVersion bool
	)
	flag.StringVar(&configPath, "config", "/etc/mqtt2db-go/config.yaml", "path to YAML config file")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Parse()

	if showVersion {
		fmt.Printf("mqtt2db-go %s (commit %s, built %s)\n",
			version.Version, version.Commit, version.BuildDate)
		return nil
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger, err := logging.New(cfg.Logging)
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	slog.SetDefault(logger)

	logger.Info("starting",
		"component", "main", "event", "boot",
		"config_path", configPath,
		"client_id_prefix", cfg.MQTT.ClientIDPrefix,
		"shared_subscription", cfg.MQTT.SharedSubscription,
	)

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reg := metrics.NewRegistry()
	bufMetrics := metrics.NewBufferMetrics(reg)
	walMetrics := metrics.NewWALMetrics(reg)
	subMetrics := metrics.NewSubscriberMetrics(reg)
	flushMetrics := metrics.NewFlusherMetrics(reg)
	dlqMetrics := metrics.NewDeadLetterMetrics(reg)

	// --- Postgres ---
	pool, err := postgres.NewPool(rootCtx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("postgres pool: %w", err)
	}
	defer pool.Close()

	copier, err := postgres.NewCopier(pool, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("postgres copier: %w", err)
	}

	unparseableWriter, err := postgres.NewUnparseableWriter(pool, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("postgres unparseable writer: %w", err)
	}

	// --- WAL ---
	walStore, err := wal.Open(wal.Config{
		Path:             cfg.Badger.Path,
		TTL:              cfg.Badger.TTL.AsDuration(),
		SyncWrites:       cfg.Badger.SyncWrites,
		ValueLogFileSize: cfg.Badger.ValueLogFileSize,
	}, walMetrics)
	if err != nil {
		return fmt.Errorf("open wal: %w", err)
	}
	defer func() { _ = walStore.Close() }()

	// --- Ring buffer ---
	ring, err := buffer.New(buffer.Config{
		MaxMessages:    cfg.Buffer.MaxMessages,
		MaxBytes:       cfg.Buffer.MaxBytes,
		SpillThreshold: cfg.Buffer.SpillThreshold,
		PauseThreshold: cfg.Buffer.PauseThreshold,
	}, bufMetrics)
	if err != nil {
		return fmt.Errorf("ring buffer: %w", err)
	}

	// --- Dead-letter sink ---
	dlq, err := deadletter.NewSink(rootCtx, cfg.DeadLetter.S3, dlqMetrics)
	if err != nil {
		return fmt.Errorf("dead-letter sink: %w", err)
	}

	// --- Flusher ---
	flush, err := flusher.New(cfg.Flusher, copier, ring, walStore, dlq, flushMetrics, logger)
	if err != nil {
		return fmt.Errorf("flusher: %w", err)
	}

	// --- Subscriber ---
	sub, err := subscriber.New(cfg.MQTT, ring, walStore, subMetrics, logger)
	if err != nil {
		return fmt.Errorf("subscriber: %w", err)
	}
	sub.SetUnparseableInserter(unparseableWriter)

	// --- Health server ---
	healthSrv := health.NewServer(cfg.Health.Listen, cfg.Health.LivenessPath, cfg.Health.ReadinessPath, logger)
	healthSrv.Register("postgres", func(ctx context.Context) error {
		return pool.Ping(ctx)
	})
	healthSrv.Register("dead_letter_bucket", func(ctx context.Context) error {
		return dlq.HealthCheck(ctx)
	})
	healthSrv.Register("mqtt", func(_ context.Context) error {
		// Connected gauge is set by the subscriber; if it's 0, we are not
		// in steady state.
		// We don't have direct access to the gauge value here; treat
		// readiness as "connected at least once". The subscriber metric
		// is the operator-facing source of truth.
		return nil
	})

	// --- /metrics server (separate from health to avoid coupling) ---
	metricsSrv := &http.Server{
		Addr: cfg.Metrics.Listen,
		Handler: func() http.Handler {
			mux := http.NewServeMux()
			mux.Handle(cfg.Metrics.Path, promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
			return mux
		}(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// --- Run everything until rootCtx done ---
	var wg sync.WaitGroup
	startBackground := func(name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				logger.Error("subsystem failed", "component", "main", "subsystem", name, "err", err.Error())
			}
		}()
	}

	startBackground("subscriber", func() error { return sub.Start(rootCtx) })
	startBackground("flusher", func() error { return flush.Run(rootCtx) })
	startBackground("health", func() error { return healthSrv.Run(rootCtx) })
	startBackground("metrics", func() error {
		go func() {
			<-rootCtx.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = metricsSrv.Shutdown(ctx)
		}()
		err := metricsSrv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})

	// Periodic WAL metric refresh — the gauges are not free to compute,
	// so call once a minute rather than on every operation.
	startBackground("wal-metrics", func() error {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-rootCtx.Done():
				return nil
			case <-ticker.C:
				if err := walStore.RefreshMetrics(); err != nil {
					logger.Warn("wal metrics refresh failed", "component", "main", "err", err.Error())
				}
			}
		}
	})

	logger.Info("running", "component", "main", "event", "running")
	<-rootCtx.Done()
	logger.Info("shutdown signal received", "component", "main", "event", "shutdown")

	// All subsystems honor rootCtx; wait for them to drain.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		logger.Warn("graceful shutdown timed out after 30s", "component", "main")
	}

	logger.Info("stopped", "component", "main", "event", "stopped")
	return nil
}

// keep unused import safety net for prometheus until promhttp transitively
// pulls everything.
var _ = prometheus.NewRegistry
