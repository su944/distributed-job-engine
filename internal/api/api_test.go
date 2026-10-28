package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

const testID = "0b4027ce-0686-4191-8e77-d3bc5a9d944d"

type stubRepository struct {
	submit func(context.Context, jobs.Submission) (jobs.Job, bool, error)
	get    func(context.Context, string) (jobs.Job, error)
}

func (s stubRepository) Submit(ctx context.Context, in jobs.Submission) (jobs.Job, bool, error) {
	return s.submit(ctx, in)
}
func (s stubRepository) Get(ctx context.Context, id string) (jobs.Job, error) { return s.get(ctx, id) }
func testAPI(repo stubRepository) *API {
	return New(repo, func(context.Context) error { return nil }, slog.New(slog.NewJSONHandler(io.Discard, nil)), map[string]bool{"invoice.send": true}, time.Second)
}

func request(app http.Handler, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "test-key")
	w := httptest.NewRecorder()
	app.ServeHTTP(w, r)
	return w
}

func TestSubmissionResponses(t *testing.T) {
	for _, tc := range []struct {
		name    string
		created bool
		err     error
		status  int
		code    string
	}{
		{"created", true, nil, 202, ""}, {"replay", false, nil, 200, ""},
		{"conflict", false, jobs.ErrConflict, 409, "idempotency_conflict"},
		{"database", false, errors.New("sensitive SQL details"), 503, "database_unavailable"},
		{"ambiguous", false, jobs.ErrSubmissionUnknown, 503, "submission_outcome_unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := testAPI(stubRepository{submit: func(ctx context.Context, s jobs.Submission) (jobs.Job, bool, error) {
				if s.Key != "test-key" || s.MaxAttempts != 5 {
					t.Fatal("bad submission")
				}
				return jobs.Job{ID: testID, Status: jobs.Ready}, tc.created, tc.err
			}})
			w := request(app, "POST", "/v1/jobs", `{"type":"invoice.send","payload":{}}`)
			if w.Code != tc.status || w.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
			if tc.code != "" {
				if !strings.Contains(w.Body.String(), tc.code) {
					t.Fatal(w.Body)
				}
			} else if w.Header().Get("Location") != "/v1/jobs/"+testID {
				t.Fatal("missing location")
			}
		})
	}
}

func TestValidationAndRouting(t *testing.T) {
	app := testAPI(stubRepository{})
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"POST", "/v1/jobs", `{}`, 400}, {"POST", "/v1/jobs", `{"type":"invoice.send","payload":{}} {}`, 400},
		{"POST", "/v1/jobs", strings.Repeat("x", jobs.MaxBodyBytes+1), 413},
		{"GET", "/v1/jobs", "", 405}, {"PATCH", "/v1/jobs/" + testID, "", 405},
		{"GET", "/v1/jobs/not-a-uuid", "", 400}, {"GET", "/unknown", "", 404},
	} {
		w := request(app, tc.method, tc.path, tc.body)
		if w.Code != tc.status || !json.Valid(w.Body.Bytes()) {
			t.Fatalf("%s %s: %d %s", tc.method, tc.path, w.Code, w.Body)
		}
	}
	for _, headers := range []http.Header{
		{"Content-Type": {"application/json"}},
		{"Content-Type": {"text/plain"}, "Idempotency-Key": {"x"}},
		{"Content-Type": {"application/json"}, "Idempotency-Key": {"a", "b"}},
	} {
		r := httptest.NewRequest("POST", "/v1/jobs", strings.NewReader(`{"type":"invoice.send","payload":{}}`))
		r.Header = headers
		w := httptest.NewRecorder()
		app.ServeHTTP(w, r)
		if w.Code != 400 && w.Code != 415 {
			t.Fatal(w.Code)
		}
	}
}

func TestGetAndHealth(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int
	}{{nil, 200}, {jobs.ErrNotFound, 404}, {context.DeadlineExceeded, 503}} {
		app := testAPI(stubRepository{get: func(ctx context.Context, id string) (jobs.Job, error) {
			if id != testID {
				t.Fatal(id)
			}
			return jobs.Job{ID: id, Status: jobs.Ready}, tc.err
		}})
		if w := request(app, "GET", "/v1/jobs/"+strings.ToUpper(testID), ""); w.Code != tc.want {
			t.Fatal(w.Code)
		}
	}
	app := testAPI(stubRepository{})
	for _, path := range []string{"/health/live", "/health/ready"} {
		if w := request(app, "GET", path, ""); w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	app.ready = func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("missing health timeout")
		}
		return errors.New("down")
	}
	if w := request(app, "GET", "/health/ready", ""); w.Code != 503 {
		t.Fatal(w.Code)
	}
	app.Drain()
	if w := request(app, "GET", "/health/live", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := request(app, "POST", "/v1/jobs", `{}`); w.Code != 503 {
		t.Fatal(w.Code)
	}
}

func TestBackpressureAndSafeLogs(t *testing.T) {
	app := testAPI(stubRepository{})
	for range cap(app.slots) {
		app.slots <- struct{}{}
	}
	w := request(app, "POST", "/v1/jobs", `{}`)
	if w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatal(w.Code)
	}
	if w := request(app, "GET", "/health/live", ""); w.Code != 200 {
		t.Fatal("health blocked by admission")
	}
	var logs bytes.Buffer
	app = testAPI(stubRepository{submit: func(context.Context, jobs.Submission) (jobs.Job, bool, error) {
		return jobs.Job{}, false, errors.New("secret-password")
	}})
	app.log = slog.New(slog.NewJSONHandler(&logs, nil))
	request(app, "POST", "/v1/jobs", `{"type":"invoice.send","payload":{"secret":"sensitive-payload"}}`)
	for _, secret := range []string{"secret-password", "sensitive-payload", "test-key"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("log leaked %s", secret)
		}
	}
}

func TestRequestContextPropagation(t *testing.T) {
	key := struct{}{}
	app := testAPI(stubRepository{get: func(ctx context.Context, id string) (jobs.Job, error) {
		if ctx.Value(key) != "propagated" {
			t.Fatal("lost context")
		}
		return jobs.Job{}, context.Canceled
	}})
	r := httptest.NewRequest("GET", "/v1/jobs/"+testID, nil).WithContext(context.WithValue(context.Background(), key, "propagated"))
	w := httptest.NewRecorder()
	app.ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}
