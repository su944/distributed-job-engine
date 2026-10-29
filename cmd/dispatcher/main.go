package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/su944/distributed-job-engine/internal/config"
	"github.com/su944/distributed-job-engine/internal/dispatch"
	"github.com/su944/distributed-job-engine/internal/server"
	"github.com/su944/distributed-job-engine/internal/storage"
)

func main() { os.Exit(run()) }
func run() int {
	getenv := func(k string) string {
		if k == "HTTP_ADDR" {
			if s := os.Getenv("DISPATCH_HTTP_ADDR"); s != "" {
				return s
			}
			return "127.0.0.1:8081"
		}
		return os.Getenv(k)
	}
	c, err := config.Load(getenv)
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: c.LogLevel}))
	if err != nil {
		log.Error("dispatcher configuration invalid")
		return 1
	}
	kc, err := dispatch.LoadConfig(os.Getenv)
	if err != nil {
		log.Error("dispatcher Kafka configuration invalid")
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := storage.Open(ctx, c)
	if err != nil {
		log.Error("dispatcher database unavailable")
		return 1
	}
	defer db.Close()
	store := dispatch.NewSQLStore(db, kc.Source, kc.Group)
	if err := store.Ready(ctx); err != nil {
		log.Error("dispatcher migration 2 required")
		return 1
	}
	d, err := dispatch.New(kc, store, store.Ready, log)
	if err != nil {
		log.Error("dispatcher creation failed")
		return 1
	}
	defer d.Close()
	listener, err := net.Listen("tcp", c.HTTPAddr)
	if err != nil {
		log.Error("dispatcher health listener failed")
		return 1
	}
	serviceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	httpDone := make(chan error, 1)
	go func() { httpDone <- server.Run(serviceCtx, listener, d, d.Drain, log, c) }()
	// If health serving fails, also stop the consumer; failures cannot leave an
	// unobservable process running indefinitely.
	consumeDone := make(chan error, 1)
	go func() { consumeDone <- d.Run(serviceCtx) }()
	var consumeErr, httpErr error
	select {
	case consumeErr = <-consumeDone:
		cancel()
		httpErr = <-httpDone
	case httpErr = <-httpDone:
		cancel()
		consumeErr = <-consumeDone
	}
	if (consumeErr != nil && !errors.Is(consumeErr, context.Canceled)) || httpErr != nil {
		log.Error("dispatcher stopped with error; unacknowledged records will replay")
		return 1
	}
	log.Info("dispatcher stopped")
	return 0
}
