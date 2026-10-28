//go:build integration

package storage_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/su944/distributed-job-engine/internal/storage"
)

func TestReadinessRequiresSubmissionTables(t *testing.T) {
	for _, table := range []string{"jobs", "outbox_events"} {
		t.Run(table, func(t *testing.T) {
			db := database(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Rename keeps the migration ledger intact and preserves foreign keys.
			if _, err := db.ExecContext(ctx, `ALTER TABLE `+table+` RENAME TO unavailable_table`); err != nil {
				t.Fatal(err)
			}
			if err := storage.Ready(ctx, db); err == nil {
				t.Fatalf("readiness accepted missing %s", table)
			}
			w := httptest.NewRecorder()
			integrationAPI(db).ServeHTTP(w, httptest.NewRequest("GET", "/health/ready", nil).WithContext(ctx))
			expectCode(t, w, 503, "not_ready")
		})
	}
}
