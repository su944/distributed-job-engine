package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

func TestOwnershipRejectsInvalidInputBeforeIO(t *testing.T) {
	r := NewRepository(nil, "default")
	ctx := context.Background()
	q := jobs.ClaimRequest{JobID: "0b4027ce-0686-4191-8e77-d3bc5a9d944d", Owner: "worker", LeaseDuration: time.Second, AttemptTimeout: time.Minute}
	for _, change := range []func(*jobs.ClaimRequest){
		func(q *jobs.ClaimRequest) { q.JobID = "bad" },
		func(q *jobs.ClaimRequest) { q.Owner = "" },
		func(q *jobs.ClaimRequest) { q.LeaseDuration = 0 },
		func(q *jobs.ClaimRequest) { q.AttemptTimeout = 25 * time.Hour },
	} {
		bad := q
		change(&bad)
		if _, err := r.Claim(ctx, bad); !errors.Is(err, jobs.ErrInvalidOwnership) {
			t.Fatal(err)
		}
	}
	if _, err := r.Renew(ctx, jobs.Ownership{}, time.Second); !errors.Is(err, jobs.ErrInvalidOwnership) {
		t.Fatal(err)
	}
	if err := r.Complete(ctx, jobs.Completion{}); !errors.Is(err, jobs.ErrInvalidOwnership) {
		t.Fatal(err)
	}
}
