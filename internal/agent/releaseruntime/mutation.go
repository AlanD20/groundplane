package releaseruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type MutationHelper interface {
	ExecuteRelease(context.Context, *agentpb.ReleaseHelperRequest) (*agentpb.ReleaseHelperResponse, error)
}

// Mutate executes a freshly acknowledged native effect. Reconnect must use
// operation inspection before deciding what to do; calling this again is not a
// substitute for reconciliation. Helper errors conservatively mean unknown effects.
func Mutate(ctx context.Context, helper MutationHelper, input releaseexecution.Input, report releaseexecution.CheckpointReport) (*agentpb.ReleaseHelperResponse, error) {
	return executeHelper(ctx, helper, input, report, false)
}

// InspectMutation observes an interrupted action without replaying its writes.
// An incomplete observation must be resolved by recovery, not another mutation.
func InspectMutation(ctx context.Context, helper MutationHelper, input releaseexecution.Input, report releaseexecution.CheckpointReport) (*agentpb.ReleaseHelperResponse, error) {
	return executeHelper(ctx, helper, input, report, true)
}

func executeHelper(ctx context.Context, helper MutationHelper, input releaseexecution.Input, report releaseexecution.CheckpointReport, inspect bool) (*agentpb.ReleaseHelperResponse, error) {
	if ctx == nil || helper == nil || input.Attempt.StartedAt == nil || report.Ack == nil ||
		report.ValidateEnvelope(input.Attempt) != nil || report.Progress.Validate(input) != nil || !report.Progress.InFlight || report.Progress.Phase != releaseexecution.Rollout {
		return nil, errs.New(errs.KindStateConflict, "Release mutation has no acknowledged execution boundary")
	}
	startup := report.Progress.Action == releaseexecution.StartCandidate || report.Progress.Rolling != nil && report.Progress.Rolling.Phase == rollingrelease.StartCandidate
	removal := report.Progress.Action == releaseexecution.RemovePrevious || report.Progress.Action == releaseexecution.RetireSwitchedPrevious
	removal = removal || report.Progress.Rolling != nil && (report.Progress.Rolling.Phase == rollingrelease.RemovePredecessor || report.Progress.Rolling.Phase == rollingrelease.RemoveRetained)
	switching := report.Progress.Action == releaseexecution.SwitchTraffic || report.Progress.Rolling != nil && report.Progress.Rolling.Phase == rollingrelease.SwitchTraffic
	verifying := report.Progress.Action == releaseexecution.VerifyServing
	if !startup && !removal && !switching && !verifying {
		return nil, errs.New(errs.KindValidationFailed, "Release mutation does not select a supported host effect")
	}
	deadline := input.Attempt.Deadline
	if cursor := report.Progress.Rolling; cursor != nil && cursor.ReplicaDeadline.Before(deadline) {
		deadline = cursor.ReplicaDeadline
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	pending := report
	pending.Ack = nil
	request, err := releaseexecution.EncodeCheckpointRequest(input, pending)
	if err != nil {
		return nil, err
	}
	ack, err := releaseexecution.EncodeCheckpointAck(input, report)
	if err != nil {
		return nil, err
	}
	response, err := helper.ExecuteRelease(bounded, &agentpb.ReleaseHelperRequest{
		Assignment: &agentpb.ReleaseWorkAssignment{Attempt: proto.CloneOf(identity), Release: proto.CloneOf(input.Release)},
		Start:      &agentpb.WorkStartAck{Attempt: identity, StartedAt: timestamppb.New(*input.Attempt.StartedAt)},
		Report:     request, Acknowledgement: ack, InspectOnly: inspect,
	})
	if err != nil {
		return nil, err
	}
	if err := releaseexecution.ValidateHelperResult(input, report.Progress, response); err != nil {
		return nil, err
	}
	if response.Inspection != inspect {
		return nil, errs.New(errs.KindStateConflict, "Release helper returned a different execution mode")
	}
	return response, nil
}
