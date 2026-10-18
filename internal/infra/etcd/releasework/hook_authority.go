package releasework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
)

// HookCheckpointConditions binds Script progress to the Release-selected hook.
// ActiveHookID is written only by Prepare after resolving the captured ordered
// hook list. This reader cannot select or advance that list itself.
func HookCheckpointConditions(ctx context.Context, store Reader, attempt workexecution.Attempt, executionID string, revision int64) ([]keyvalue.Condition, error) {
	if store == nil || attempt.Validate() != nil || attempt.StartedAt == nil || attempt.FinishedAt != nil ||
		revision <= 0 || !scriptexecutions.ValidRawScriptExecutionID(executionID) {
		return nil, invalid()
	}
	key := ProgressKey(attempt.ID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision || len(value.Value) > maximumProgressBytes {
		return nil, invalid()
	}
	record, err := recordcodec.Decode[Record](value.Value, "release-work-progress")
	if err != nil {
		return nil, err
	}
	progress := record.Progress
	if record.TaskID != attempt.TaskID || record.WorkItemID != attempt.WorkItemID || record.Input != attempt.Input || record.ActiveHookID != executionID ||
		progress.AttemptID != attempt.ID || !progress.InFlight || progress.BeganAt == nil || progress.BeganAt.Before(*attempt.StartedAt) ||
		progress.UpdatedAt.Before(*progress.BeganAt) || progress.Sequence%2 != 1 ||
		progress.Phase != releaseexecution.PreHooks && progress.Phase != releaseexecution.PostHooks ||
		progress.Rolling != nil || progress.Action != "" || record.CheckpointSequence == 0 || ids.Validate(ids.KindStep, record.StageID) != nil {
		return nil, invalid()
	}
	return []keyvalue.Condition{{Key: key, ModRevision: value.ModRevision}}, nil
}
