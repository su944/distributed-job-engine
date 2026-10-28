package config

import (
	"log/slog"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	env := map[string]string{"DATABASE_URL": "postgres://localhost/jobs"}
	c, err := Load(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != "127.0.0.1:8080" || c.DBMaxOpen != 10 || c.RequestTimeout != 5*time.Second || !c.JobTypes["invoice.send"] {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	env["LOG_LEVEL"] = "debug"
	env["JOB_TYPES"] = "email.send, invoice.send"
	env["DB_MAX_OPEN"] = "20"
	c, err = Load(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if c.LogLevel != slog.LevelDebug || !c.JobTypes["email.send"] || c.DBMaxOpen != 20 {
		t.Fatal("overrides not applied")
	}
}

func TestInvalidConfig(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"DATABASE_URL", ""}, {"HTTP_ADDR", "bad"}, {"HTTP_ADDR", ":99999"}, {"JOB_NAMESPACE", "bad space"},
		{"JOB_TYPES", "bad type"}, {"LOG_LEVEL", "wrong"}, {"DB_MAX_OPEN", "0"}, {"DB_MAX_IDLE", "11"},
		{"DB_MAX_OPEN", "101"}, {"DB_TIMEOUT", "6s"}, {"REQUEST_TIMEOUT", "0s"}, {"HTTP_READ_TIMEOUT", "-1s"},
		{"HTTP_WRITE_TIMEOUT", "1s"}, {"HTTP_IDLE_TIMEOUT", "1h"}, {"SHUTDOWN_TIMEOUT", "1s"},
		{"HTTP_READ_HEADER_TIMEOUT", "11s"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			_, err := Load(func(k string) string {
				if k == tc.key {
					return tc.value
				}
				if k == "DATABASE_URL" {
					return "postgres://localhost/jobs"
				}
				return ""
			})
			if err == nil {
				t.Fatal("expected invalid config")
			}
		})
	}
}
