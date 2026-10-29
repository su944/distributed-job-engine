package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotClaimable     = errors.New("job is not claimable")
	ErrLeaseLost        = errors.New("job ownership is no longer valid")
	ErrInvalidOwnership = errors.New("invalid ownership request")
	// ErrOwnershipUnknown means COMMIT was attempted but not confirmed. No
	// execution permission is returned; do not blindly replay an external effect.
	ErrOwnershipUnknown = errors.New("ownership transaction outcome unknown")
)

// Ownership is an internal fencing identity, never a public API representation.
type Ownership struct {
	JobID      string
	Owner      string
	LeaseToken int64
}

type ClaimRequest struct {
	JobID          string
	Owner          string
	LeaseDuration  time.Duration
	AttemptTimeout time.Duration
}

type Claim struct {
	Ownership
	AcquiredAt     time.Time // database sample used for deadline/lease arithmetic
	Attempt        int
	Type           string
	Payload        json.RawMessage
	LeaseExpiresAt time.Time
	Deadline       time.Time
}

type JobClaimer interface {
	Claim(context.Context, ClaimRequest) (Claim, error)
}

// DispatchClaimRequest is trusted worker policy, not an API request.
type DispatchClaimRequest struct {
	Owner          string
	Types          []string
	LeaseDuration  time.Duration
	AttemptTimeout time.Duration
}

type DispatchedClaimer interface {
	ClaimDispatched(context.Context, DispatchClaimRequest) (Claim, error)
}

type LeaseRenewer interface {
	Renew(context.Context, Ownership, time.Duration) (time.Time, error)
}

type CompletionOutcome string

const (
	CompletionSucceeded CompletionOutcome = "SUCCEEDED"
	CompletionPermanent CompletionOutcome = "PERMANENT"
	CompletionUnknown   CompletionOutcome = "UNKNOWN"
	CompletionRetryable CompletionOutcome = "RETRYABLE"
)

type Completion struct {
	Ownership
	Outcome   CompletionOutcome
	Result    json.RawMessage
	ErrorCode string
}

type JobCompleter interface {
	Complete(context.Context, Completion) error
}

// Durations are bounded and stored at PostgreSQL microsecond precision.
func ValidOwnershipDuration(d time.Duration) bool {
	return d >= time.Microsecond && d <= 24*time.Hour && d%time.Microsecond == 0
}

func ValidOwner(owner string) bool {
	if len(owner) == 0 || len(owner) > 128 {
		return false
	}
	for _, c := range []byte(owner) {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

func (o Ownership) Valid() bool {
	return ValidID(o.JobID) && ValidOwner(o.Owner) && o.LeaseToken > 0
}

func (c Completion) Valid() bool {
	if !c.Ownership.Valid() || (c.ErrorCode != "" && !ValidType(c.ErrorCode)) {
		return false
	}
	switch c.Outcome {
	case CompletionSucceeded:
		if c.ErrorCode != "" {
			return false
		}
		if len(c.Result) == 0 {
			return true
		}
		if len(c.Result) > MaxBodyBytes || !json.Valid(c.Result) {
			return false
		}
		var object map[string]json.RawMessage
		return json.Unmarshal(c.Result, &object) == nil && object != nil
	case CompletionPermanent, CompletionUnknown, CompletionRetryable:
		return len(c.Result) == 0
	default:
		return false
	}
}
