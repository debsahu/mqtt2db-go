// Package health serves /healthz and /readyz on a separate HTTP listener
// from /metrics so a slow scrape can't block readiness.
//
// Liveness is a flat 200 — if the process is running, it is "alive".
// Kubernetes restarts the pod on liveness failures and we don't want
// transient downstream issues to cause a restart loop.
//
// Readiness aggregates checks: any one returning a non-nil error reports
// 503. Operators add probes via Register; the metrics endpoint shipping
// alongside this service publishes the underlying gauges.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Probe is a single readiness check. Return nil when healthy.
type Probe func(ctx context.Context) error

// Server runs the health endpoints. Zero value is unusable; construct
// with NewServer.
type Server struct {
	addr          string
	livenessPath  string
	readinessPath string
	logger        *slog.Logger

	mu     sync.RWMutex
	probes map[string]Probe

	srv *http.Server
}

// NewServer wires up paths and ports. addr defaults to ":8080" when
// empty. Path defaults follow the conventions in CONFIGURATION.md.
func NewServer(addr, livenessPath, readinessPath string, logger *slog.Logger) *Server {
	if addr == "" {
		addr = ":8080"
	}
	if livenessPath == "" {
		livenessPath = "/healthz"
	}
	if readinessPath == "" {
		readinessPath = "/readyz"
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		addr:          addr,
		livenessPath:  livenessPath,
		readinessPath: readinessPath,
		logger:        logger.With("component", "health"),
		probes:        map[string]Probe{},
	}
}

// Register adds a readiness probe. Replaces any existing probe with the
// same name. Names appear in the readiness response body so operators
// can identify which check failed.
func (s *Server) Register(name string, p Probe) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probes[name] = p
}

// Run blocks until ctx is canceled, then shuts the HTTP listener down
// cleanly with a 5s grace period.
func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc(s.livenessPath, s.handleLive)
	mux.HandleFunc(s.readinessPath, s.handleReady)

	s.srv = &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("started", "event", "started", "addr", s.addr)
		errCh <- s.srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("health server: %w", err)
		}
		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("health server shutdown: %w", err)
	}
	s.logger.Info("stopped", "event", "stopped")
	return nil
}

func (s *Server) handleLive(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "alive"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	probes := make(map[string]Probe, len(s.probes))
	for k, v := range s.probes {
		probes[k] = v
	}
	s.mu.RUnlock()

	type result struct {
		Name string `json:"name"`
		OK   bool   `json:"ok"`
		Err  string `json:"err,omitempty"`
	}
	results := make([]result, 0, len(probes))
	allOK := true

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	for name, p := range probes {
		err := p(ctx)
		results = append(results, result{Name: name, OK: err == nil, Err: errString(err)})
		if err != nil {
			allOK = false
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if !allOK {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": map[bool]string{true: "ready", false: "unready"}[allOK],
		"checks": results,
	})
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
