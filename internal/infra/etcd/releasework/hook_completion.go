package releasework

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// HookCompletionConditions consumes the Script owner's committed outcome and
// cleanup proof. A normal zero exit without cleanup, an older attempt's result,
// or a caller-selected hook cannot complete the Release phase.
func HookCompletionConditions(ctx context.Context, store Reader, view workqueue.ResumeView, input releaseexecution.Input, previous Snapshot, report releaseexecution.CheckpointReport) ([]keyvalue.Condition, error) {
	if store == nil || previous.Record == nil || previous.Revision <= 0 || view.Checkpoint.Record == nil ||
		!view.ActiveAttempt || !view.CurrentExecutor || !workexecution.SameAssignment(view.Attempt.Attempt, input.Attempt) ||
		report.Progress.InFlight || report.Progress.Evidence == nil || report.Ack != nil {
		return nil, invalid()
	}
	prior := previous.Record.Progress
	hook, err := prior.ActiveHook(input)
	if err != nil {
		return nil, err
	}
	if err := releaseexecution.ValidateTransition(input, &prior, report.Progress); err != nil {
		return nil, err
	}
	cursor := *view.Checkpoint.Record
	if cursor.Sequence <= previous.Record.CheckpointSequence || cursor.Sequence == ^uint64(0) || report.Sequence != cursor.Sequence+1 ||
		cursor.AttemptID != input.Attempt.ID || *report.Progress.Evidence != cursor.Evidence || report.Progress.UpdatedAt.Before(cursor.RecordedAt) {
		return nil, invalid()
	}
	executionKey := scriptexecutions.ScriptExecutionKey(hook.ExecutionId)
	evidenceKey := scriptexecutions.CheckpointEvidenceKey(hook.ExecutionId, input.Attempt.ID, cursor.Sequence)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{executionKey, evidenceKey}, Revision: view.Work.ReadRevision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 2 {
		return nil, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	for index, key := range []string{executionKey, evidenceKey} {
		value := read.Values[index]
		if value == nil || value.Key != key || value.ModRevision != view.Checkpoint.Revision {
			return nil, invalid()
		}
	}
	record, err := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](read.Values[0].Value, "script-execution")
	if err != nil || scriptexecutions.ValidateScriptExecutionRecord(record) != nil {
		return nil, invalid()
	}
	hookInput := workexecution.Input{ID: hook.Input.InputId, SHA256: hex.EncodeToString(hook.Input.Sha256)}
	if record.ID != hook.ExecutionId || record.ScriptID != hook.ScriptId || record.Input != hookInput ||
		record.WorkInput == nil || *record.WorkInput != input.Attempt.Input || record.CurrentTaskID != input.Attempt.TaskID ||
		record.WorkItemID != input.Attempt.WorkItemID || record.AttemptID != input.Attempt.ID || record.Executor == nil || *record.Executor != input.Attempt.Executor ||
		record.OperationID != input.Release.OperationId || record.EnvironmentID != input.Release.EnvironmentId || record.ServiceID != input.Release.ServiceId ||
		record.ReleaseID != input.Release.ReleaseId || record.RenderGeneration != input.Release.RenderGeneration || record.State != scriptexecutions.ScriptExecutionCleanupProven ||
		record.ReconciliationRequired || record.ControllerCleanup != "" || record.Outcome == nil || record.Outcome.Reason != scriptexecutions.ScriptOutcomeNormalExit ||
		record.Outcome.ExitCode == nil || *record.Outcome.ExitCode != 0 || record.Cleanup == nil || !record.UpdatedAt.Equal(cursor.RecordedAt) {
		return nil, invalid()
	}
	digest := sha256.Sum256(read.Values[1].Value)
	evidenceID, err := scriptexecutions.CheckpointEvidenceID(input.Attempt.TaskID, hook.ExecutionId, input.Attempt.ID, cursor.Sequence)
	if err != nil {
		return nil, err
	}
	expected := workexecution.Input{ID: evidenceID, SHA256: hex.EncodeToString(digest[:])}
	if cursor.Evidence != expected || record.LastCheckpointSHA256 != expected.SHA256 {
		return nil, invalid()
	}
	evidence, err := recordcodec.Decode[scriptexecutions.ScriptCheckpointInput](read.Values[1].Value, "script-checkpoint-evidence")
	if err != nil || evidence.PayloadSHA256 != "" {
		return nil, invalid()
	}
	evidence.PayloadSHA256 = expected.SHA256
	if scriptexecutions.ValidateScriptCheckpointInput(evidence) != nil || evidence.ExecutionID != record.ID || evidence.TaskID != record.CurrentTaskID ||
		evidence.WorkItemID != record.WorkItemID || evidence.AttemptID != record.AttemptID || evidence.Executor != *record.Executor ||
		evidence.OperationID != record.OperationID || evidence.Input != record.Input || evidence.State != record.State ||
		evidence.Sequence != cursor.Sequence || evidence.StageID != cursor.StageID || !evidence.At.Equal(cursor.RecordedAt) {
		return nil, invalid()
	}
	return []keyvalue.Condition{{Key: executionKey, ModRevision: read.Values[0].ModRevision}, {Key: evidenceKey, ModRevision: read.Values[1].ModRevision}}, nil
}
