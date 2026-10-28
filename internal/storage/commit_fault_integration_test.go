//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/su944/distributed-job-engine/internal/api"
	"github.com/su944/distributed-job-engine/internal/jobs"
	"github.com/su944/distributed-job-engine/internal/storage"
)

// commitFault changes only the test client's protocol transport. The real SQL,
// transaction, driver and HTTP error mapping all execute normally. One mode
// prevents COMMIT being sent; the other consumes the server's COMMIT success but
// reports EOF before the driver can observe it. No production hooks are needed.
type commitFault struct {
	selected atomic.Bool
	fired    atomic.Bool
	before   bool
}

type faultStream struct {
	r       io.Reader
	w       io.Writer
	fault   *commitFault
	armed   atomic.Bool
	pending []byte
}

func (s *faultStream) Write(p []byte) (int, error) {
	commit, _ := (&pgproto3.Query{String: "commit"}).Encode(nil)
	if bytes.Equal(p, commit) && s.fault.selected.CompareAndSwap(false, true) {
		if s.fault.before {
			s.fault.fired.Store(true)
			return 0, io.ErrUnexpectedEOF
		}
		s.armed.Store(true)
	}
	return s.w.Write(p)
}

func (s *faultStream) Read(p []byte) (int, error) {
	if !s.armed.Load() {
		return s.r.Read(p)
	}
	if s.fault.fired.Load() {
		return 0, io.ErrUnexpectedEOF
	}
	if len(s.pending) == 0 {
		var header [5]byte
		if _, err := io.ReadFull(s.r, header[:]); err != nil {
			return 0, err
		}
		size := int(binary.BigEndian.Uint32(header[1:]))
		if size < 4 || size > 1<<20 {
			return 0, errors.New("invalid fault-test protocol frame length")
		}
		payload := make([]byte, size-4)
		if _, err := io.ReadFull(s.r, payload); err != nil {
			return 0, err
		}
		if header[0] == 'C' && bytes.Equal(payload, []byte("COMMIT\x00")) {
			s.fault.fired.Store(true)
			return 0, io.ErrUnexpectedEOF
		}
		s.pending = append(header[:], payload...)
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

func faultDatabase(t *testing.T, db *sql.DB, fault *commitFault) *sql.DB {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var schema string
	if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("invalid TEST_DATABASE_URL")
	}
	cfg.RuntimeParams["search_path"] = schema
	// BuildFrontend wraps protocol I/O after any TLS layer, so the fault does not
	// require plaintext connections or interfere with PostgreSQL authentication.
	cfg.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
		stream := &faultStream{r: r, w: w, fault: fault}
		return pgproto3.NewFrontend(stream, stream)
	}
	broken := stdlib.OpenDB(*cfg)
	broken.SetMaxOpenConns(1)
	broken.SetMaxIdleConns(1)
	t.Cleanup(func() { broken.Close() })
	return broken
}

func integrationAPI(db *sql.DB) *api.API {
	return api.New(storage.NewRepository(db, "default"), func(ctx context.Context) error { return storage.Ready(ctx, db) }, slog.New(slog.NewJSONHandler(io.Discard, nil)), map[string]bool{"invoice.send": true, "email.send": true}, time.Second)
}

func postJob(ctx context.Context, handler *api.API, key, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/jobs", strings.NewReader(body)).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", key)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func expectCode(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if w.Code != status || response.Error.Code != code || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("got %d %s, want %d %s", w.Code, w.Body, status, code)
	}
}

func TestAmbiguousCommitRetryResolvesBothOutcomes(t *testing.T) {
	for _, before := range []bool{true, false} {
		name := "committed_reply_lost"
		if before {
			name = "commit_not_sent"
		}
		t.Run(name, func(t *testing.T) {
			db := database(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			fault := &commitFault{before: before}
			broken := faultDatabase(t, db, fault)
			app := integrationAPI(broken)
			w := postJob(ctx, app, "uncertain-key", body)
			expectCode(t, w, 503, "submission_outcome_unknown")
			if !fault.fired.Load() {
				t.Fatal("protocol fault never fired")
			}
			if w.Header().Get("Location") != "" {
				t.Fatal("failed submission advertised success")
			}
			wantCount, wantStatus := 1, 200
			if before {
				wantCount, wantStatus = 0, 202
			}
			count(t, db, "jobs", wantCount)
			count(t, db, "outbox_events", wantCount)
			var originalID string
			if !before {
				if err := db.QueryRowContext(ctx, `SELECT id::text FROM jobs WHERE idempotency_key=$1`, "uncertain-key").Scan(&originalID); err != nil {
					t.Fatal(err)
				}
			}
			// The same client pool must discard the failed connection and recover.
			w = postJob(ctx, app, "uncertain-key", body)
			if w.Code != wantStatus {
				t.Fatalf("retry: %d %s", w.Code, w.Body)
			}
			var j jobs.Job
			if err := json.Unmarshal(w.Body.Bytes(), &j); err != nil {
				t.Fatal(err)
			}
			if originalID != "" && j.ID != originalID {
				t.Fatal("committed job replaced on retry")
			}
			expectCode(t, postJob(ctx, app, "uncertain-key", strings.Replace(body, "inv_001", "different", 1)), 409, "idempotency_conflict")
			count(t, db, "jobs", 1)
			count(t, db, "outbox_events", 1)
			if stats := broken.Stats(); stats.InUse != 0 || stats.OpenConnections > 1 {
				t.Fatalf("failed connection leaked: %+v", stats)
			}
		})
	}
}
