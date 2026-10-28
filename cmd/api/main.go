package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/su944/distributed-job-engine/internal/api"
	"github.com/su944/distributed-job-engine/internal/config"
	"github.com/su944/distributed-job-engine/internal/server"
	"github.com/su944/distributed-job-engine/internal/storage"
)

func main() { os.Exit(run()) }

func run() int {
	c, err := config.Load(os.Getenv)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: c.LogLevel}))
	if err != nil {
		logger.Error("configuration failed", "error", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := storage.Open(ctx, c)
	if err != nil {
		logger.Error("database startup failed", "error", err)
		return 1
	}
	defer db.Close()
	ready := func(ctx context.Context) error {
		return storage.Ready(ctx, db)
	}
	app := api.New(storage.NewRepository(db, c.Namespace), ready, logger, c.JobTypes, c.DBTimeout)
	listener, err := net.Listen("tcp", c.HTTPAddr)
	if err != nil {
		logger.Error("HTTP listener failed")
		return 1
	}
	logger.Info("HTTP server starting", "address", listener.Addr().String())
	if err := server.Run(ctx, listener, app, app.Drain, logger, c); err != nil {
		logger.Error("HTTP server stopped with error")
		return 1
	}
	return 0
}
