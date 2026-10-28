package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/su944/distributed-job-engine/internal/config"
	"github.com/su944/distributed-job-engine/internal/storage"
	"github.com/su944/distributed-job-engine/migrations"
)

func main() { os.Exit(run()) }

func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	c, err := config.Load(os.Getenv)
	if err != nil {
		logger.Error("configuration failed", "error", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := storage.Open(ctx, c)
	if err != nil {
		logger.Error("database startup failed", "error", err)
		return 1
	}
	defer db.Close()
	if err := migrations.Apply(ctx, db); err != nil {
		logger.Error("migration failed; inspect database schema and migration checksums")
		return 1
	}
	logger.Info("migrations applied", "version", 1)
	return 0
}
