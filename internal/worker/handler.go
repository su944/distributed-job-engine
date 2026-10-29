// Package worker executes only PostgreSQL-owned jobs from the durable receipt inbox.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

type Classification string

const (
	Success          Classification = "success"
	RetryableFailure Classification = "retryable"
	PermanentFailure Classification = "permanent"
	UnknownOutcome   Classification = "unknown"
)

type Result struct {
	Class Classification
	Value json.RawMessage
	Code  string
}

// Handlers must honor ctx cancellation and never spawn detached effects. JobID
// and token are available for stable provider keys/fences in future integrations.
type Handler interface {
	Handle(context.Context, jobs.Claim) Result
}

type HandlerFunc func(context.Context, jobs.Claim) Result

func (f HandlerFunc) Handle(ctx context.Context, c jobs.Claim) Result { return f(ctx, c) }

type Registration struct {
	Handler    Handler
	ReplaySafe bool
}

// Registry is built once before Pool construction; New copies its registrations.
type Registry map[string]Registration

func (r Registry) Register(typ string, h Handler, replaySafe bool) error {
	if r == nil || !jobs.ValidType(typ) || h == nil {
		return errors.New("invalid handler registration")
	}
	if _, exists := r[typ]; exists {
		return errors.New("handler already registered")
	}
	r[typ] = Registration{h, replaySafe}
	return nil
}

func (r Registry) types() []string {
	types := make([]string, 0, len(r))
	for typ := range r {
		types = append(types, typ)
	}
	sort.Strings(types)
	return types
}

// Echo returns precisely the stored object without network calls or side effects.
// Payloads/results are never logged and the existing API does not expose results.
type Echo struct{}

func (Echo) Handle(ctx context.Context, c jobs.Claim) Result {
	if ctx.Err() != nil {
		return Result{Class: UnknownOutcome, Code: "handler.canceled"}
	}
	return Result{Class: Success, Value: append(json.RawMessage(nil), c.Payload...)}
}

func invoke(ctx context.Context, h Handler, c jobs.Claim) (r Result) {
	defer func() {
		if recover() != nil {
			r = Result{Class: UnknownOutcome, Code: "handler.panic"}
		}
	}()
	return h.Handle(ctx, c)
}

func completion(c jobs.Claim, registration Registration, result Result) jobs.Completion {
	q := jobs.Completion{Ownership: c.Ownership, ErrorCode: result.Code}
	switch result.Class {
	case Success:
		q.Outcome = jobs.CompletionSucceeded
		q.Result = result.Value
	case PermanentFailure:
		q.Outcome = jobs.CompletionPermanent
	case RetryableFailure:
		q.Outcome = jobs.CompletionUnknown
		if registration.ReplaySafe {
			q.Outcome = jobs.CompletionRetryable
		}
	case UnknownOutcome:
		q.Outcome = jobs.CompletionUnknown
	}
	// Malformed handler output is uncertainty, never inferred success or retry.
	if !q.Valid() {
		return jobs.Completion{Ownership: c.Ownership, Outcome: jobs.CompletionUnknown, ErrorCode: "handler.invalid_result"}
	}
	return q
}
