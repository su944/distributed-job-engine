package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/su944/distributed-job-engine/internal/config"
	"github.com/su944/distributed-job-engine/internal/server"
	"github.com/su944/distributed-job-engine/internal/storage"
	"github.com/su944/distributed-job-engine/internal/worker"
)

func main() { os.Exit(run()) }

func run() int {
	getenv := func(k string) string {
		if k == "HTTP_ADDR" {
			if s := os.Getenv("WORKER_HTTP_ADDR"); s != "" {
				return s
			}
			return "127.0.0.1:8082"
		}
		return os.Getenv(k)
	}
	c, err := config.Load(getenv)
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: c.LogLevel}))
	if err != nil {
		log.Error("worker database/HTTP configuration invalid")
		return 1
	}
	wc, err := worker.LoadConfig(os.Getenv)
	if err != nil {
		log.Error("worker execution configuration invalid")
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := storage.Open(ctx, c)
	if err != nil {
		log.Error("worker database unavailable")
		return 1
	}
	defer db.Close()
	// Reserved renewal connections prevent a saturated claim/completion pool
	// from starving heartbeats. Both pools remain explicitly bounded.
	hc := c
	hc.DBMaxOpen = min(wc.Capacity, 4)
	hc.DBMaxIdle = min(c.DBMaxIdle, hc.DBMaxOpen)
	heartbeats, err := storage.Open(ctx, hc)
	if err != nil {
		log.Error("worker heartbeat database unavailable")
		return 1
	}
	defer heartbeats.Close()
	repo := storage.NewRepository(db, c.Namespace)
	if err := repo.WorkerReady(ctx); err != nil {
		log.Error("worker requires migration 4 and durable inbox")
		return 1
	}
	registry := worker.Registry{}
	if err := registry.Register("demo.echo", worker.Echo{}, true); err != nil {
		return 1
	}
	pool, err := worker.New(wc, repo, storage.NewRepository(heartbeats, c.Namespace), registry, log)
	if err != nil {
		log.Error("worker construction failed")
		return 1
	}
	listener, err := net.Listen("tcp", c.HTTPAddr)
	if err != nil {
		log.Error("worker health listener failed")
		return 1
	}
	serviceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	httpDone := make(chan error, 1)
	poolDone := make(chan error, 1)
	go func() { httpDone <- server.Run(serviceCtx, listener, pool, func() {}, log, c) }()
	go func() { poolDone <- pool.Run(serviceCtx) }()
	var poolErr, httpErr error
	select {
	case poolErr = <-poolDone:
		cancel()
		httpErr = <-httpDone
	case httpErr = <-httpDone:
		cancel()
		poolErr = <-poolDone
	}
	if poolErr != nil || httpErr != nil {
		log.Error("worker stopped with error; inspect durable job state")
		return 1
	}
	log.Info("worker drained")
	return 0
}
