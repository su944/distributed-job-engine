package jobs

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestOwnershipInputBounds(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second, time.Nanosecond, time.Second + time.Nanosecond, 25 * time.Hour} {
		if ValidOwnershipDuration(d) {
			t.Fatalf("accepted duration %s", d)
		}
	}
	for _, d := range []time.Duration{time.Microsecond, time.Second, 24 * time.Hour} {
		if !ValidOwnershipDuration(d) {
			t.Fatalf("rejected duration %s", d)
		}
	}
	for _, owner := range []string{"", "worker a", "worker\n", "é", strings.Repeat("a", 129)} {
		if ValidOwner(owner) {
			t.Fatalf("accepted owner %q", owner)
		}
	}
	o := Ownership{"0b4027ce-0686-4191-8e77-d3bc5a9d944d", "worker-1", 1}
	if !o.Valid() {
		t.Fatal("valid identity rejected")
	}
	for _, c := range []Completion{
		{Ownership: o, Outcome: CompletionSucceeded},
		{Ownership: o, Outcome: CompletionSucceeded, Result: json.RawMessage(`{"receipt":"ok"}`)},
		{Ownership: o, Outcome: CompletionPermanent, ErrorCode: "provider.rejected"},
		{Ownership: o, Outcome: CompletionUnknown},
	} {
		if !c.Valid() {
			t.Fatalf("valid completion rejected: %+v", c)
		}
	}
	for _, c := range []Completion{
		{Outcome: CompletionSucceeded},
		{Ownership: o, Outcome: "RETRYABLE"},
		{Ownership: o, Outcome: CompletionSucceeded, ErrorCode: "failed"},
		{Ownership: o, Outcome: CompletionSucceeded, Result: json.RawMessage(`null`)},
		{Ownership: o, Outcome: CompletionSucceeded, Result: json.RawMessage(`[]`)},
		{Ownership: o, Outcome: CompletionSucceeded, Result: json.RawMessage(`{} {}`)},
		{Ownership: o, Outcome: CompletionSucceeded, Result: json.RawMessage(strings.Repeat("x", MaxBodyBytes+1))},
		{Ownership: o, Outcome: CompletionPermanent, Result: json.RawMessage(`{}`)},
		{Ownership: o, Outcome: CompletionUnknown, ErrorCode: "raw error message"},
	} {
		if c.Valid() {
			t.Fatalf("invalid completion accepted: %+v", c)
		}
	}
}
