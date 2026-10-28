// Package server owns HTTP timeouts and bounded graceful shutdown.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"

	"github.com/su944/distributed-job-engine/internal/config"
)

func Run(ctx context.Context, listener net.Listener, handler http.Handler, drain func(), logger *slog.Logger, c config.Config) error {
	requestCtx, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()
	s := &http.Server{
		Handler:           http.TimeoutHandler(handler, c.RequestTimeout, `{"error":{"code":"request_timeout"}}`),
		ReadHeaderTimeout: c.ReadHeaderTimeout, ReadTimeout: c.ReadTimeout, WriteTimeout: c.WriteTimeout, IdleTimeout: c.IdleTimeout,
		MaxHeaderBytes: 16 << 10, ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelError),
		BaseContext: func(net.Listener) context.Context { return requestCtx },
	}
	// Set the JSON type before TimeoutHandler writes its fixed timeout response.
	s.Handler = jsonContentType{s.Handler}
	result := make(chan error, 1)
	go func() { result <- s.Serve(listener) }()
	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		drain()
		logger.Info("HTTP shutdown started")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), c.ShutdownTimeout)
		defer cancel()
		if err := s.Shutdown(shutdownCtx); err != nil {
			cancelRequests()
			_ = s.Close()
			<-result
			return err
		}
		<-result
		logger.Info("HTTP shutdown complete")
		return nil
	}
}

type jsonContentType struct{ http.Handler }

func (h jsonContentType) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	h.Handler.ServeHTTP(w, r)
}
