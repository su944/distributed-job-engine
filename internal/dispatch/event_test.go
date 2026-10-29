package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

const testJob = "0b4027ce-0686-4191-8e77-d3bc5a9d944d"
const testEvent = `{"schema_version":1,"event_id":"78aad7c3-8c81-4a65-88b4-a4ba1e115f77","job_id":"0b4027ce-0686-4191-8e77-d3bc5a9d944d","kind":"JobReady","occurred_at":"2026-10-28T10:00:00Z"}`

func TestStrictEventValidation(t *testing.T) {
	r := Record{Topic: Topic, Key: []byte(testJob), Value: []byte(testEvent)}
	e, err := Decode(r)
	if err != nil || e.JobID != testJob {
		t.Fatalf("valid event: %+v %v", e, err)
	}
	for name, value := range map[string]string{
		"duplicate":   strings.Replace(testEvent, `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1),
		"unknown":     strings.Replace(testEvent, `"kind":`, `"extra":1,"kind":`, 1),
		"unsupported": strings.Replace(testEvent, `"schema_version":1`, `"schema_version":2`, 1),
		"missing":     strings.Replace(testEvent, `"kind":"JobReady",`, "", 1),
		"null":        strings.Replace(testEvent, `"kind":"JobReady"`, `"kind":null`, 1),
		"bad_id":      strings.Replace(testEvent, testJob, "not-uuid", 1),
		"bad_time":    strings.Replace(testEvent, "2026-10-28T10:00:00Z", "yesterday", 1),
		"wrong_kind":  strings.Replace(testEvent, "JobReady", "Complete", 1),
		"trailing":    testEvent + ` {}`, "array": `[]`, "large": strings.Repeat("x", MaxEventBytes+1), "invalid_utf8": string([]byte{0xff}),
	} {
		t.Run(name, func(t *testing.T) {
			r.Value = []byte(value)
			if _, err := Decode(r); err == nil {
				t.Fatal("invalid event accepted")
			}
		})
	}
	r.Value = []byte(testEvent)
	r.Key = []byte("wrong")
	if _, err := Decode(r); err == nil {
		t.Fatal("mismatched key accepted")
	}
}

type storeFunc func(context.Context, Record) (Outcome, error)

func (f storeFunc) Handle(ctx context.Context, r Record) (Outcome, error) { return f(ctx, r) }

func TestDurabilityBeforeOffsetCommit(t *testing.T) {
	for _, outcome := range []Outcome{Received, Duplicate, Quarantined} {
		t.Run(string(outcome), func(t *testing.T) {
			durable, committed := false, false
			d := &Dispatcher{log: slog.New(slog.NewJSONHandler(io.Discard, nil)), store: storeFunc(func(ctx context.Context, r Record) (Outcome, error) {
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("missing storage deadline")
				}
				durable = true
				return outcome, nil
			})}
			err := d.handle(context.Background(), &kgo.Record{Topic: Topic, Partition: 1, Offset: 42}, func(ctx context.Context, r *kgo.Record) error {
				if !durable || r.Offset != 42 {
					t.Fatal("offset advanced before durable outcome")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("missing commit deadline")
				}
				committed = true
				return nil
			})
			if err != nil || !committed {
				t.Fatalf("commit: %t %v", committed, err)
			}
		})
	}
	d := &Dispatcher{log: slog.New(slog.NewJSONHandler(io.Discard, nil)), store: storeFunc(func(context.Context, Record) (Outcome, error) { return "", errors.New("sensitive database error") })}
	if err := d.handle(context.Background(), &kgo.Record{}, func(context.Context, *kgo.Record) error { t.Fatal("failed storage was acknowledged"); return nil }); err == nil {
		t.Fatal("storage failure lost")
	}
	d.store = storeFunc(func(context.Context, Record) (Outcome, error) { return Received, nil })
	if err := d.handle(context.Background(), &kgo.Record{}, func(context.Context, *kgo.Record) error { return errors.New("broker down") }); err == nil {
		t.Fatal("commit failure lost")
	}
}

func TestConfigAndHealth(t *testing.T) {
	for _, broker := range []string{"", "host", "host:0", "host:99999"} {
		if _, err := LoadConfig(func(k string) string {
			if k == "KAFKA_BROKERS" {
				return broker
			}
			return ""
		}); err == nil {
			t.Fatal("bad broker accepted")
		}
	}
	if _, err := LoadConfig(func(k string) string {
		if k == "KAFKA_BROKERS" {
			return "127.0.0.1:19092"
		}
		return ""
	}); err != nil {
		t.Fatal(err)
	}
	d := &Dispatcher{}
	for path, want := range map[string]int{"/health/live": 200, "/health/ready": 503, "/metrics": 200, "/missing": 404} {
		w := httptest.NewRecorder()
		d.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != want {
			t.Fatal(w.Code)
		}
		if path != "/metrics" && !json.Valid(w.Body.Bytes()) {
			t.Fatal(w.Body)
		}
	}
}
