// Package config reads and validates environment settings without global state.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

type Config struct {
	DatabaseURL       string
	HTTPAddr          string
	Namespace         string
	JobTypes          map[string]bool
	LogLevel          slog.Level
	DBMaxOpen         int
	DBMaxIdle         int
	DBTimeout         time.Duration
	RequestTimeout    time.Duration
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
}

func Load(getenv func(string) string) (Config, error) {
	c := Config{DatabaseURL: getenv("DATABASE_URL"), HTTPAddr: "127.0.0.1:8080", Namespace: "default", JobTypes: map[string]bool{}, DBMaxOpen: 10, DBMaxIdle: 5, DBTimeout: 3 * time.Second, RequestTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, ShutdownTimeout: 20 * time.Second}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	if s := getenv("HTTP_ADDR"); s != "" {
		c.HTTPAddr = s
	}
	if _, port, err := net.SplitHostPort(c.HTTPAddr); err != nil {
		return c, errors.New("HTTP_ADDR must be host:port")
	} else if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return c, errors.New("invalid HTTP_ADDR port")
	}
	if s := getenv("JOB_NAMESPACE"); s != "" {
		c.Namespace = s
	}
	if !jobs.ValidType(c.Namespace) {
		return c, errors.New("invalid JOB_NAMESPACE")
	}
	types := getenv("JOB_TYPES")
	if types == "" {
		types = "invoice.send"
	}
	for s := range strings.SplitSeq(types, ",") {
		s = strings.TrimSpace(s)
		if !jobs.ValidType(s) {
			return c, errors.New("JOB_TYPES contains invalid name")
		}
		c.JobTypes[s] = true
	}
	if s := getenv("LOG_LEVEL"); s != "" {
		if err := c.LogLevel.UnmarshalText([]byte(s)); err != nil {
			return c, errors.New("invalid LOG_LEVEL")
		}
	}
	for name, target := range map[string]*int{"DB_MAX_OPEN": &c.DBMaxOpen, "DB_MAX_IDLE": &c.DBMaxIdle} {
		if s := getenv(name); s != "" {
			v, err := strconv.Atoi(s)
			if err != nil || v < 1 || v > 100 {
				return c, fmt.Errorf("%s must be between 1 and 100", name)
			}
			*target = v
		}
	}
	if c.DBMaxIdle > c.DBMaxOpen {
		return c, errors.New("DB_MAX_IDLE exceeds DB_MAX_OPEN")
	}
	for name, target := range map[string]*time.Duration{"DB_TIMEOUT": &c.DBTimeout, "REQUEST_TIMEOUT": &c.RequestTimeout, "HTTP_READ_TIMEOUT": &c.ReadTimeout, "HTTP_READ_HEADER_TIMEOUT": &c.ReadHeaderTimeout, "HTTP_WRITE_TIMEOUT": &c.WriteTimeout, "HTTP_IDLE_TIMEOUT": &c.IdleTimeout, "SHUTDOWN_TIMEOUT": &c.ShutdownTimeout} {
		if s := getenv(name); s != "" {
			d, err := time.ParseDuration(s)
			if err != nil || d <= 0 || d > 10*time.Minute {
				return c, fmt.Errorf("%s must be a positive duration <= 10m", name)
			}
			*target = d
		}
	}
	if c.WriteTimeout <= c.RequestTimeout || c.ShutdownTimeout < c.RequestTimeout || c.DBTimeout > c.RequestTimeout || c.ReadHeaderTimeout > c.ReadTimeout {
		return c, errors.New("inconsistent HTTP/database/shutdown timeouts")
	}
	return c, nil
}
