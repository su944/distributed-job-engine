package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/su944/distributed-job-engine/internal/config"
)

func TestInvalidConnectionStringDoesNotExposeSecret(t *testing.T) {
	c := config.Config{DatabaseURL: "postgres://user:secret-password@host:invalid-port/db"}
	_, err := Open(context.Background(), c)
	if err == nil || strings.Contains(err.Error(), "secret-password") {
		t.Fatalf("unsafe parse error: %v", err)
	}
}
