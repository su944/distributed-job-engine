// Package jobs defines the durable submission contract, independent of HTTP and SQL.
package jobs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type Status string

const (
	Ready              Status = "READY"
	Running            Status = "RUNNING"
	RetryWait          Status = "RETRY_WAIT"
	Succeeded          Status = "SUCCEEDED"
	Failed             Status = "FAILED"
	Reconcile          Status = "RECONCILE"
	DefaultMaxAttempts        = 5
	MaxAttempts               = 20
	MaxBodyBytes              = 256 << 10
)

var (
	ErrNotFound          = errors.New("job not found")
	ErrConflict          = errors.New("idempotency conflict")
	ErrSubmissionUnknown = errors.New("submission outcome unknown")
	typePattern          = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)
	idPattern            = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

// Job is the public status representation. Payloads, keys and hashes stay private.
type Job struct {
	ID          string    `json:"id"`
	Status      Status    `json:"status"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Submission is validated and canonicalized before it reaches persistence.
type Submission struct {
	Key         string
	Type        string
	Payload     json.RawMessage
	MaxAttempts int
	Hash        [32]byte
}

type Repository interface {
	Submit(context.Context, Submission) (job Job, created bool, err error)
	Get(context.Context, string) (Job, error)
}

func ValidType(s string) bool { return typePattern.MatchString(s) }
func ValidID(s string) bool   { return idPattern.MatchString(s) }

func UUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate UUID: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// ParseSubmission rejects ambiguous JSON (including duplicate keys) before hashing.
// Keys are opaque, printable ASCII; they are never trimmed or logged.
func ParseSubmission(key string, body []byte, allowed map[string]bool) (Submission, error) {
	if len(key) == 0 || len(key) > 128 {
		return Submission{}, errors.New("Idempotency-Key must be 1-128 printable ASCII bytes")
	}
	for _, c := range []byte(key) {
		if c < 33 || c > 126 {
			return Submission{}, errors.New("invalid Idempotency-Key")
		}
	}
	if len(body) > MaxBodyBytes {
		return Submission{}, errors.New("request too large")
	}
	value, err := parseJSON(body)
	if err != nil {
		return Submission{}, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return Submission{}, errors.New("request must be an object")
	}
	for k := range object {
		if k != "type" && k != "payload" && k != "max_attempts" {
			return Submission{}, fmt.Errorf("unknown field %q", k)
		}
	}
	typ, ok := object["type"].(string)
	if !ok || !ValidType(typ) || !allowed[typ] {
		return Submission{}, errors.New("unknown or invalid job type")
	}
	payload, ok := object["payload"].(map[string]any)
	if !ok {
		return Submission{}, errors.New("payload must be an object")
	}
	maxAttempts := DefaultMaxAttempts
	if v, exists := object["max_attempts"]; exists {
		n, ok := v.(json.Number)
		if !ok {
			return Submission{}, errors.New("max_attempts must be an integer")
		}
		i, err := n.Int64()
		if err != nil || i < 1 || i > MaxAttempts {
			return Submission{}, errors.New("max_attempts must be between 1 and 20")
		}
		maxAttempts = int(i)
	}
	if err := normalizeNumbers(payload); err != nil {
		return Submission{}, err
	}
	p, err := json.Marshal(payload)
	if err != nil {
		return Submission{}, fmt.Errorf("encode payload: %w", err)
	}
	canonical, err := json.Marshal(struct {
		Type        string          `json:"type"`
		Payload     json.RawMessage `json:"payload"`
		MaxAttempts int             `json:"max_attempts"`
	}{typ, p, maxAttempts})
	if err != nil {
		return Submission{}, err
	}
	return Submission{Key: key, Type: typ, Payload: p, MaxAttempts: maxAttempts, Hash: sha256.Sum256(canonical)}, nil
}

// Normalize strings solely for UUID comparisons, never for opaque idempotency keys.
func CanonicalID(id string) string { return strings.ToLower(id) }
