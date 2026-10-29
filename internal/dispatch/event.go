// Package dispatch durably receives notifications; it never claims or executes jobs.
package dispatch

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

const Topic = "jobs.ready.v1"
const MaxEventBytes = 8192

type Event struct {
	SchemaVersion int       `json:"schema_version"`
	EventID       string    `json:"event_id"`
	JobID         string    `json:"job_id"`
	Kind          string    `json:"kind"`
	OccurredAt    time.Time `json:"occurred_at"`
}

type Record struct {
	Topic      string
	Partition  int32
	Offset     int64
	Key, Value []byte
}
type InvalidEvent struct{ Code string }

func (e *InvalidEvent) Error() string { return e.Code }

// Decode rejects duplicate/unknown fields, nulls and trailing values. The envelope
// is flat; its scalar fields are decoded without float64 or permissive maps.
func Decode(r Record) (Event, error) {
	var e Event
	bad := func(code string) (Event, error) { return Event{}, &InvalidEvent{code} }
	if r.Topic != Topic || len(r.Value) > MaxEventBytes || !utf8.Valid(r.Value) {
		return bad("invalid_envelope")
	}
	d := json.NewDecoder(bytes.NewReader(r.Value))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return bad("invalid_json")
	}
	seen := map[string]bool{}
	for d.More() {
		t, err := d.Token()
		if err != nil {
			return bad("invalid_json")
		}
		name, ok := t.(string)
		if !ok || seen[name] {
			return bad("invalid_envelope")
		}
		seen[name] = true
		var target any
		switch name {
		case "schema_version":
			target = &e.SchemaVersion
		case "event_id":
			target = &e.EventID
		case "job_id":
			target = &e.JobID
		case "kind":
			target = &e.Kind
		case "occurred_at":
			target = &e.OccurredAt
		default:
			return bad("invalid_envelope")
		}
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil {
			return bad("invalid_json")
		}
		if bytes.Equal(raw, []byte("null")) || json.Unmarshal(raw, target) != nil {
			return bad("invalid_envelope")
		}
	}
	if end, err := d.Token(); err != nil || end != json.Delim('}') {
		return bad("invalid_json")
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return bad("invalid_json")
	}
	if len(seen) != 5 {
		return bad("invalid_envelope")
	}
	if e.SchemaVersion != 1 {
		return bad("unsupported_version")
	}
	if !jobs.ValidID(e.EventID) || !jobs.ValidID(e.JobID) || e.Kind != "JobReady" || e.OccurredAt.IsZero() {
		return bad("invalid_envelope")
	}
	e.EventID = strings.ToLower(e.EventID)
	e.JobID = strings.ToLower(e.JobID)
	e.OccurredAt = e.OccurredAt.UTC()
	if string(r.Key) != e.JobID {
		return bad("key_mismatch")
	}
	return e, nil
}

func (e Event) digest() [32]byte { b, _ := json.Marshal(e); return sha256.Sum256(b) }
