package executiondiagnostics

import (
	"context"
	"fmt"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type HookResult struct {
	Key         string
	State       workexecution.StageState
	Observed    string
	FailureCode string
	FinishedAt  time.Time
}

// ReleaseHookResult projects retained, operation-owned evidence. It does not
// interpret stdout, depend on the Agent's disposable delivery journal, rewrite
// diagnostics, or turn a parent attempt failure into a guessed command result.
func (repository *Repository) ReleaseHookResult(ctx context.Context, attempt workexecution.Attempt,
	revision int64,
) (*HookResult, error) {
	if attempt.FinishedAt == nil || attempt.StartedAt == nil || attempt.Direction != workexecution.Forward ||
		attempt.Outcome != workexecution.Failed {
		return nil, nil
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Revision: revision, Keys: []string{
		releasework.ProgressKey(attempt.ID), workqueue.CheckpointKey(attempt.ID),
	}})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		return nil, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, nil // Other operation owners have different progress records.
	}
	if read.Values[1] == nil || len(read.Values[0].Value) > 16<<10 {
		return nil, invalid()
	}
	parent, err := recordcodec.Decode[releasework.Record](read.Values[0].Value, "release-work-progress")
	if err != nil || parent.TaskID != attempt.TaskID || parent.WorkItemID != attempt.WorkItemID ||
		parent.Input != attempt.Input || parent.Progress.AttemptID != attempt.ID {
		return nil, invalid()
	}
	progress := parent.Progress
	if !progress.InFlight ||
		progress.Phase != releaseexecution.PreHooks && progress.Phase != releaseexecution.PostHooks {
		return nil, nil
	}
	if !scriptexecutions.ValidRawScriptExecutionID(parent.ActiveHookID) || progress.BeganAt == nil ||
		progress.BeganAt.Before(*attempt.StartedAt) || progress.UpdatedAt.After(*attempt.FinishedAt) {
		return nil, invalid()
	}
	key, err := releaseexecution.HookDiagnosticKey(progress.Phase, progress.Hook)
	if err != nil {
		return nil, err
	}
	checkpoint, err := workqueue.DecodeCheckpoint(read.Values[1].Value)
	if err != nil || checkpoint.AttemptID != attempt.ID || checkpoint.Sequence < parent.CheckpointSequence ||
		checkpoint.RecordedAt.After(*attempt.FinishedAt) {
		return nil, invalid()
	}
	if checkpoint.Sequence == parent.CheckpointSequence {
		active := attempt
		active.FinishedAt, active.Result, active.Outcome = nil, nil, ""
		expected, err := (releaseexecution.CheckpointReport{
			Sequence: parent.CheckpointSequence, StageID: parent.StageID, Progress: progress,
		}).Checkpoint(active)
		if err != nil || !releaseexecution.EqualCheckpoint(checkpoint, expected) {
			return nil, invalid()
		}
		return &HookResult{Key: key, State: workexecution.StageFailed, FinishedAt: *attempt.FinishedAt,
			FailureCode: "script.start_not_authorized",
			Observed:    "The hook command did not start: no Script start authorization was recorded before this attempt failed."}, nil
	}
	current, previous, err := repository.readHookCheckpoints(ctx, attempt, parent.ActiveHookID, checkpoint, revision)
	if err != nil {
		return nil, err
	}
	finishedAt := current.At
	if current.State == scriptexecutions.ScriptExecutionCleanupProven {
		if previous == nil || previous.State != scriptexecutions.ScriptExecutionOutcomeRecorded ||
			previous.Input != current.Input || previous.At.After(current.At) {
			return nil, invalid()
		}
		current = *previous
	}
	if current.State != scriptexecutions.ScriptExecutionOutcomeRecorded || current.Evidence.Outcome == nil {
		return nil, nil
	}
	outcome := current.Evidence.Outcome
	success := outcome.Reason == scriptexecutions.ScriptOutcomeNormalExit && outcome.ExitCode != nil &&
		*outcome.ExitCode == 0
	if success && previous == nil {
		return nil, nil // A zero exit alone does not prove hook cleanup.
	}
	result := &HookResult{Key: key, State: workexecution.StageSucceeded, FinishedAt: finishedAt,
		Observed: "Recorded Script outcome: " + string(outcome.Reason)}
	if outcome.ExitCode != nil {
		result.Observed += fmt.Sprintf("; exit code %d", *outcome.ExitCode)
	}
	if !success {
		result.State, result.FailureCode, result.FinishedAt = workexecution.StageFailed, "script.execution_failed", current.At
	}
	return result, nil
}
