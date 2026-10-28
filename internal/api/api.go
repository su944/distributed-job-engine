// Package api provides the HTTP transport without owning database or process lifecycle.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

type API struct {
	repo      jobs.Repository
	ready     func(context.Context) error
	log       *slog.Logger
	types     map[string]bool
	dbTimeout time.Duration
	slots     chan struct{}
	draining  atomic.Bool
}

func New(repo jobs.Repository, ready func(context.Context) error, logger *slog.Logger, types map[string]bool, dbTimeout time.Duration) *API {
	copyTypes := make(map[string]bool, len(types))
	for k, v := range types {
		copyTypes[k] = v
	}
	return &API{repo: repo, ready: ready, log: logger, types: copyTypes, dbTimeout: dbTimeout, slots: make(chan struct{}, 64)}
}

func (a *API) Drain() { a.draining.Store(true) }

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func fail(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code}})
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		a.log.Info("http request", "method", r.Method, "route", routeName(r.URL.Path), "duration_ms", time.Since(start).Milliseconds())
	}()
	path := r.URL.Path
	switch path {
	case "/health/live":
		if !method(w, r, http.MethodGet) {
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "live"})
		return
	case "/health/ready":
		if !method(w, r, http.MethodGet) {
			return
		}
		if a.draining.Load() {
			fail(w, http.StatusServiceUnavailable, "not_ready")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), a.dbTimeout)
		defer cancel()
		if err := a.ready(ctx); err != nil {
			fail(w, http.StatusServiceUnavailable, "not_ready")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		return
	}
	if path != "/v1/jobs" && !strings.HasPrefix(path, "/v1/jobs/") {
		fail(w, http.StatusNotFound, "not_found")
		return
	}
	if a.draining.Load() {
		fail(w, http.StatusServiceUnavailable, "shutting_down")
		return
	}
	select {
	case a.slots <- struct{}{}:
		defer func() { <-a.slots }()
	default:
		w.Header().Set("Retry-After", "1")
		fail(w, http.StatusTooManyRequests, "overloaded")
		return
	}
	if path == "/v1/jobs" {
		if method(w, r, http.MethodPost) {
			a.submit(w, r)
		}
		return
	}
	if !method(w, r, http.MethodGet) {
		return
	}
	id := strings.TrimPrefix(path, "/v1/jobs/")
	if !jobs.ValidID(id) {
		fail(w, http.StatusBadRequest, "invalid_job_id")
		return
	}
	j, err := a.repo.Get(r.Context(), jobs.CanonicalID(id))
	if errors.Is(err, jobs.ErrNotFound) {
		fail(w, http.StatusNotFound, "job_not_found")
		return
	}
	if err != nil {
		a.storageError(w, "get", err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func routeName(path string) string {
	if path == "/v1/jobs" || path == "/health/live" || path == "/health/ready" {
		return path
	}
	if strings.HasPrefix(path, "/v1/jobs/") {
		return "/v1/jobs/{id}"
	}
	return "unmatched"
}

func method(w http.ResponseWriter, r *http.Request, want string) bool {
	if r.Method == want {
		return true
	}
	w.Header().Set("Allow", want)
	fail(w, http.StatusMethodNotAllowed, "method_not_allowed")
	return false
}

func (a *API) submit(w http.ResponseWriter, r *http.Request) {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		fail(w, http.StatusUnsupportedMediaType, "unsupported_media_type")
		return
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 {
		fail(w, http.StatusBadRequest, "invalid_request")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, jobs.MaxBodyBytes)
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var max *http.MaxBytesError
		if errors.As(err, &max) {
			fail(w, http.StatusRequestEntityTooLarge, "request_too_large")
		} else if code, ok := contextErrorCode(err); ok {
			fail(w, http.StatusServiceUnavailable, code)
		} else {
			fail(w, http.StatusBadRequest, "invalid_request")
		}
		return
	}
	s, err := jobs.ParseSubmission(keys[0], body, a.types)
	if err != nil {
		fail(w, http.StatusBadRequest, "invalid_request")
		return
	}
	j, created, err := a.repo.Submit(r.Context(), s)
	if errors.Is(err, jobs.ErrConflict) {
		fail(w, http.StatusConflict, "idempotency_conflict")
		return
	}
	if err != nil {
		a.storageError(w, "submit", err)
		return
	}
	w.Header().Set("Location", "/v1/jobs/"+j.ID)
	status := http.StatusOK
	if created {
		status = http.StatusAccepted
	}
	writeJSON(w, status, j)
}

func (a *API) storageError(w http.ResponseWriter, operation string, err error) {
	code := "database_unavailable"
	if errors.Is(err, jobs.ErrSubmissionUnknown) {
		code = "submission_outcome_unknown"
	} else if contextCode, ok := contextErrorCode(err); ok {
		code = contextCode
	}
	// Driver errors can contain SQL parameters and connection secrets; never log them.
	a.log.Error("storage operation failed", "operation", operation, "code", code)
	fail(w, http.StatusServiceUnavailable, code)
}

func contextErrorCode(err error) (string, bool) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "request_timeout", true
	case errors.Is(err, context.Canceled):
		return "request_canceled", true
	default:
		return "", false
	}
}
