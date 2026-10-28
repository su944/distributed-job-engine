package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

type failedBody struct{ err error }

func (b failedBody) Read([]byte) (int, error) { return 0, b.err }
func (b failedBody) Close() error             { return nil }

func TestContextErrorsHaveRequestCodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code string
	}{
		{"canceled", fmt.Errorf("query: %w", context.Canceled), "request_canceled"},
		{"deadline", fmt.Errorf("query: %w", context.DeadlineExceeded), "request_timeout"},
		{"ambiguous_cancellation", fmt.Errorf("%w: %w", jobs.ErrSubmissionUnknown, context.Canceled), "submission_outcome_unknown"},
	} {
		for _, operation := range []string{"submit", "get"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				app := testAPI(stubRepository{
					submit: func(context.Context, jobs.Submission) (jobs.Job, bool, error) { return jobs.Job{}, false, tc.err },
					get:    func(context.Context, string) (jobs.Job, error) { return jobs.Job{}, tc.err },
				})
				method, path, body := "GET", "/v1/jobs/"+testID, ""
				if operation == "submit" {
					method, path, body = "POST", "/v1/jobs", `{"type":"invoice.send","payload":{}}`
				}
				w := request(app, method, path, body)
				assertError(t, w, 503, tc.code)
			})
		}
	}
}

func TestBodyCancellationIsNotInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code string
	}{{context.Canceled, "request_canceled"}, {context.DeadlineExceeded, "request_timeout"}} {
		app := testAPI(stubRepository{})
		r := httptest.NewRequest("POST", "/v1/jobs", nil)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "key")
		r.Body = failedBody{fmt.Errorf("read body: %w", tc.err)}
		w := httptest.NewRecorder()
		app.ServeHTTP(w, r)
		assertError(t, w, 503, tc.code)
	}
}

func assertError(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	var result struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if w.Code != status || result.Error.Code != code || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("got %d %s, want %d %s", w.Code, w.Body, status, code)
	}
}
