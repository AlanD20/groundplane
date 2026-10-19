package releasework

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PreservedServing is read-only evidence from the original failed attempt.
// Presence means that the complete candidate served before a later failure;
// it is not permission to resume that attempt or execute its hooks again.
type PreservedServing struct {
	Input      releaseexecution.Input
	Proof      *CompletionProof
	Conditions []keyvalue.Condition
}

func LoadPreservedServing(
	ctx context.Context,
	store Reader,
	input releaseexecution.RecoveryInput,
	revision int64,
) (PreservedServing, error) {
	if ctx == nil || store == nil || input.ValidateIdentity() != nil || input.Input.Release == nil || revision <= 0 {
		return PreservedServing{}, invalid()
	}
	if input.Group != nil || input.Forward.Outcome != workexecution.Failed ||
		input.Input.Release.FailurePolicy != agentpb.WorkReleaseFailurePolicy_WORK_RELEASE_FAILURE_POLICY_LEAVE_ACTIVE {
		return PreservedServing{}, nil
	}
	// Checkpoint hashes were recorded before Finish. Reconstruct only that
	// historical identity for proof decoding; no assignment is created or resumed.
	source := input.Input
	source.Attempt = input.Forward
	source.Attempt.FinishedAt, source.Attempt.Result, source.Attempt.Outcome = nil, nil, ""
	saved, err := Load(ctx, store, source, revision)
	if err != nil {
		return PreservedServing{}, err
	}
	result := PreservedServing{Input: source, Conditions: []keyvalue.Condition{
		{Key: ProgressKey(input.Forward.ID), ModRevision: saved.Revision},
	}}
	if saved.Record == nil || saved.Record.Serving == nil {
		return result, nil
	}
	checkpoint := input.Failure.Checkpoint
	if checkpoint == nil || checkpoint.Sequence < saved.Record.CheckpointSequence ||
		checkpoint.RecordedAt.Before(saved.Record.Progress.UpdatedAt) ||
		saved.Record.Progress.UpdatedAt.After(*input.Forward.FinishedAt) {
		return PreservedServing{}, invalid()
	}
	if checkpoint.Sequence == saved.Record.CheckpointSequence {
		report := releaseexecution.CheckpointReport{Sequence: saved.Record.CheckpointSequence,
			StageID: saved.Record.StageID, Progress: saved.Record.Progress}
		expected, err := report.Checkpoint(source.Attempt)
		if err != nil || !releaseexecution.EqualCheckpoint(expected, *checkpoint) {
			return PreservedServing{}, invalid()
		}
	} else if !saved.Record.Progress.InFlight || saved.Record.Progress.Phase != releaseexecution.PostHooks {
		// Only a Script running inside the saved hook boundary can advance
		// the original attempt's checkpoint past native Release progress.
		return PreservedServing{}, invalid()
	}
	evidence, response, condition, err := readServingEffect(
		ctx,
		store,
		source,
		saved.Record.Serving,
		revision,
		saved.Record.Progress.UpdatedAt,
	)
	if err != nil {
		return PreservedServing{}, err
	}
	runtime, err := executionplan.PrepareReleaseWorkRuntime(source.Attempt, source.Release)
	if err != nil {
		return PreservedServing{}, err
	}
	if proxy := response.ProxyEvidence; proxy != nil {
		if proxy.ReleaseId != runtime.ReleaseID || proxy.ServiceId != runtime.ServiceID ||
			proxy.Target != runtime.Target ||
			proxy.ProxyGeneration != runtime.ProxyGeneration ||
			!bytes.Equal(proxy.ConfigSha256, runtime.ProxyConfigSHA256) {
			return PreservedServing{}, invalid()
		}
	} else if runtime.ProxyGeneration != 0 || len(runtime.ProxyConfigSHA256) != 0 {
		return PreservedServing{}, invalid()
	}
	result.Conditions = append(result.Conditions, condition)
	result.Proof = &CompletionProof{
		Runtime:    runtime,
		Progress:   saved,
		Serving:    response,
		Evidence:   evidence,
		Conditions: result.Conditions,
	}
	return result, nil
}
