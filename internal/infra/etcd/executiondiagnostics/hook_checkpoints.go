package executiondiagnostics

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
)

func (repository *Repository) readHookCheckpoints(ctx context.Context, attempt workexecution.Attempt,
	executionID string, checkpoint workexecution.Checkpoint, revision int64,
) (scriptexecutions.ScriptCheckpointInput, *scriptexecutions.ScriptCheckpointInput, error) {
	var empty scriptexecutions.ScriptCheckpointInput
	if checkpoint.Sequence < 2 {
		return empty, nil, invalid()
	}
	keys := []string{
		scriptexecutions.CheckpointEvidenceKey(executionID, attempt.ID, checkpoint.Sequence),
		scriptexecutions.CheckpointEvidenceKey(executionID, attempt.ID, checkpoint.Sequence-1),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return empty, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 || read.Values[0] == nil {
		return empty, nil, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	current, err := decodeHookCheckpoint(read.Values[0].Value, attempt, executionID, checkpoint.Sequence)
	if err != nil || current.PayloadSHA256 != checkpoint.Evidence.SHA256 ||
		!current.At.Equal(checkpoint.RecordedAt) || current.StageID != checkpoint.StageID {
		return empty, nil, invalid()
	}
	evidenceID, err := scriptexecutions.CheckpointEvidenceID(
		attempt.TaskID,
		executionID,
		attempt.ID,
		checkpoint.Sequence,
	)
	if err != nil || checkpoint.Evidence.ID != evidenceID {
		return empty, nil, invalid()
	}
	if current.State != scriptexecutions.ScriptExecutionCleanupProven {
		return current, nil, nil
	}
	if read.Values[1] == nil {
		return empty, nil, invalid()
	}
	previous, err := decodeHookCheckpoint(read.Values[1].Value, attempt, executionID, checkpoint.Sequence-1)
	if err != nil {
		return empty, nil, err
	}
	return current, &previous, nil
}

func decodeHookCheckpoint(raw []byte, attempt workexecution.Attempt, executionID string,
	sequence uint64,
) (scriptexecutions.ScriptCheckpointInput, error) {
	var empty scriptexecutions.ScriptCheckpointInput
	if len(raw) > 16<<10 {
		return empty, invalid()
	}
	input, err := recordcodec.Decode[scriptexecutions.ScriptCheckpointInput](raw, "script-checkpoint-evidence")
	if err != nil || input.PayloadSHA256 != "" {
		return empty, invalid()
	}
	digest := sha256.Sum256(raw)
	input.PayloadSHA256 = hex.EncodeToString(digest[:])
	if scriptexecutions.ValidateScriptCheckpointInput(input) != nil || input.TaskID != attempt.TaskID ||
		input.WorkItemID != attempt.WorkItemID || input.AttemptID != attempt.ID || input.ExecutionID != executionID ||
		input.Executor != attempt.Executor || input.Sequence != sequence || attempt.StartedAt == nil ||
		input.At.Before(*attempt.StartedAt) || attempt.FinishedAt == nil || input.At.After(*attempt.FinishedAt) {
		return empty, invalid()
	}
	return input, nil
}
