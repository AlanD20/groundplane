package releaseruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Observer interface {
	ObserveRelease(context.Context, releaseexecution.Input) (*agentpb.ObservedProject, error)
}

// WaitCandidate waits only inside an acknowledged readiness boundary and the
// original attempt deadline. Lost observation or missing probe executable is a
// concrete failure, not an invitation to wait until an unrelated timeout.
func WaitCandidate(
	ctx context.Context,
	observer Observer,
	input releaseexecution.Input,
	report releaseexecution.CheckpointReport,
	progress func(*agentpb.ObservedProject),
) (*agentpb.ObservedProject, error) {
	if ctx == nil || observer == nil || report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil ||
		report.Progress.Validate(input) != nil ||
		!report.Progress.InFlight ||
		report.Progress.Phase != releaseexecution.Rollout {
		return nil, errs.New(errs.KindStateConflict, "Release has no acknowledged candidate-readiness boundary")
	}
	deadline := input.Attempt.Deadline
	if rolling := report.Progress.Rolling; rolling != nil {
		if rolling.Phase != rollingrelease.WaitCandidateHealthy {
			return nil, errs.New(errs.KindStateConflict, "rolling Release is not waiting for candidate health")
		}
		if rolling.ReplicaDeadline.Before(deadline) {
			deadline = rolling.ReplicaDeadline
		}
	} else if report.Progress.Action != releaseexecution.WaitCandidate {
		return nil, errs.New(errs.KindStateConflict, "Release is not waiting for candidate readiness")
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := bounded.Err(); err != nil {
			return nil, err
		}
		observed, err := observer.ObserveRelease(bounded, input)
		if err != nil {
			return nil, err
		}
		ready, err := releaseexecution.Readiness(input, report.Progress, observed)
		if progress != nil {
			progress(proto.CloneOf(observed))
		}
		if err != nil {
			return observed, err
		}
		if ready {
			return observed, nil
		}
		select {
		case <-bounded.Done():
			return observed, bounded.Err()
		case <-ticker.C:
		}
	}
}
