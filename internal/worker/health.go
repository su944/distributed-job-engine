package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

func (p *Pool) Ready(ctx context.Context) error {
	if !p.running.Load() || p.draining.Load() {
		return errors.New("worker is not accepting claims")
	}
	ready, ok := p.repo.(interface{ WorkerReady(context.Context) error })
	if !ok {
		return errors.New("worker repository readiness unavailable")
	}
	return ready.WorkerReady(ctx)
}

func (p *Pool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(405)
		_, _ = w.Write([]byte(`{"error":{"code":"method_not_allowed"}}`))
		return
	}
	switch r.URL.Path {
	case "/health/live":
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "live"})
	case "/health/ready":
		ctx, cancel := context.WithTimeout(r.Context(), p.c.IOTimeout)
		defer cancel()
		if p.Ready(ctx) != nil {
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"error":{"code":"not_ready"}}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	case "/metrics":
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "worker_capacity %d\nworker_active %d\nworker_recorded_total %d\nworker_errors_total %d\n", p.c.Capacity, p.active.Load(), p.completed.Load(), p.failed.Load())
	default:
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"error":{"code":"not_found"}}`))
	}
}
