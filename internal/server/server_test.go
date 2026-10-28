package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/su944/distributed-job-engine/internal/config"
)

func testConfig(t *testing.T) config.Config {
	t.Helper()
	c, err := config.Load(func(k string) string {
		if k == "DATABASE_URL" {
			return "postgres://unused"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestGracefulShutdownDrainsInFlightRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, release, drained := make(chan struct{}), make(chan struct{}), make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		if r.Context().Err() != nil {
			t.Error("drain canceled active request")
		}
		w.Write([]byte(`{"status":"done"}`))
	})
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, listener, h, func() { close(drained) }, slog.New(slog.NewJSONHandler(io.Discard, nil)), testConfig(t))
	}()
	response := make(chan error, 1)
	go func() {
		client := http.Client{Timeout: 3 * time.Second}
		r, err := client.Get("http://" + listener.Addr().String())
		if err == nil {
			defer r.Body.Close()
			if r.StatusCode != 200 {
				err = io.ErrUnexpectedEOF
			}
		}
		response <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request never started")
	}
	cancel()
	select {
	case <-drained:
	case <-time.After(3 * time.Second):
		t.Fatal("drain not called")
	}
	close(release)
	if err := <-response; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not finish")
	}
}

func TestRequestTimeoutJSONAndCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := testConfig(t)
	c.RequestTimeout = 20 * time.Millisecond
	release, observed := make(chan struct{}), make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done(); close(observed); <-release })
	done := make(chan error, 1)
	go func() { done <- Run(ctx, listener, h, func() {}, slog.New(slog.NewJSONHandler(io.Discard, nil)), c) }()
	client := http.Client{Timeout: 3 * time.Second}
	r, err := client.Get("http://" + listener.Addr().String())
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	body, err := io.ReadAll(r.Body)
	r.Body.Close()
	close(release)
	if err != nil || r.StatusCode != 503 || r.Header.Get("Content-Type") != "application/json" || !strings.Contains(string(body), "request_timeout") {
		t.Fatalf("timeout response: %d %s %v", r.StatusCode, body, err)
	}
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("request context not canceled")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDeadlineCancelsRequests(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := testConfig(t)
	c.ShutdownTimeout = 25 * time.Millisecond
	started, stopped := make(chan struct{}), make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(stopped) })
	done := make(chan error, 1)
	go func() { done <- Run(ctx, listener, h, func() {}, slog.New(slog.NewJSONHandler(io.Discard, nil)), c) }()
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		client := http.Client{Timeout: 3 * time.Second}
		r, err := client.Get("http://" + listener.Addr().String())
		if err == nil {
			r.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request not started")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected shutdown deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown not bounded")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("request not canceled")
	}
	<-clientDone
}
