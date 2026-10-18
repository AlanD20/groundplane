package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RetireUnpublishedHookStage removes only the exact snapshots named by a
// cancelled preparation. References and executions must be absent atomically.
// False requests another pass after this bounded stage transaction.
func (ledger *ReleaseLedger) RetireUnpublishedHookStage(ctx context.Context, view workqueue.View) (bool, error) {
	task, item := view.Task.Task, view.Item.Item
	if ctx == nil || ledger == nil || ledger.store == nil || task.Validate() != nil || item.Validate() != nil ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.Progress.Unsettled() != 0 ||
		item.TaskID != task.ID || item.Operation != workexecution.Release || item.Phase != workexecution.Settled ||
		item.Input != nil || item.AttemptNumber != 0 || item.ActiveAttemptID != "" {
		return false, errs.New(errs.KindStateConflict, "Hook preparation cleanup requires unpublished collecting work")
	}
	key := blueprintReleaseHookStageKey(item.ID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 {
		return false, errs.New(errs.KindStateConflict, "Hook preparation cleanup read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return true, nil
	}
	var stage blueprintReleaseHookStage
	if json.Unmarshal(value.Value, &stage) != nil || value.Key != key || value.ModRevision <= 0 ||
		stage.Schema != 1 || stage.TaskID != task.ID || stage.WorkItemID != item.ID || stage.Input.Validate() != nil ||
		ids.Validate(ids.KindOperation, stage.OperationID) != nil || stage.Count < 1 || stage.Count > 16 || len(stage.Snapshots) != stage.Count {
		return false, errs.New(errs.KindStateConflict, "Hook preparation cleanup descriptor is invalid")
	}
	conditions := []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision}, {Key: key, ModRevision: value.ModRevision},
		{Key: ReleaseWorkInputKey(item.ID)}}
	sources, err := scriptsourcepublication.NewAuthority(ledger.store)
	if err != nil {
		return false, err
	}
	drained, err := sources.AbandonOwnedPreparation(ctx, stage.OperationID, conditions)
	if err != nil || !drained {
		return false, err
	}
	conditions = append(conditions, keyvalue.Condition{Key: sourceref.RootKey(stage.OperationID)},
		keyvalue.Condition{Key: sourceref.PreparationKey(stage.OperationID)},
		keyvalue.Condition{Key: sourceref.ReversePrefix(stage.OperationID), Prefix: true})
	keys := make([]string, len(stage.Snapshots))
	seen := make(map[string]bool)
	for index, snapshot := range stage.Snapshots {
		if !scriptexecutions.ValidRawScriptExecutionID(snapshot.ExecutionID) || !scriptexecutions.ValidRawScriptExecutionID(snapshot.SnapshotID) ||
			!scriptexecutions.ValidLowerSHA256(snapshot.SHA256) || seen[snapshot.SnapshotID] {
			return false, errs.New(errs.KindStateConflict, "Hook preparation snapshot identity is invalid")
		}
		seen[snapshot.SnapshotID] = true
		keys[index] = scriptexecutions.ScriptRunnerSnapshotKey(snapshot.SnapshotID)
		conditions = append(conditions, keyvalue.Condition{Key: scriptexecutions.ScriptExecutionKey(snapshot.ExecutionID)})
	}
	snapshots, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if err != nil {
		return false, err
	}
	if snapshots == nil || snapshots.ReadRevision != view.ReadRevision || len(snapshots.Values) != len(keys) {
		return false, errs.New(errs.KindStateConflict, "Hook preparation snapshot read is incomplete")
	}
	defer keyvalue.ClearValues(snapshots.Values)
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}}
	for index, snapshot := range snapshots.Values {
		condition := keyvalue.Condition{Key: keys[index]}
		if snapshot != nil {
			digest := sha256.Sum256(snapshot.Value)
			if snapshot.Key != keys[index] || snapshot.ModRevision <= 0 || hex.EncodeToString(digest[:]) != stage.Snapshots[index].SHA256 {
				return false, errs.New(errs.KindStateConflict, "Hook preparation snapshot changed; cleanup refused")
			}
			condition.ModRevision = snapshot.ModRevision
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: snapshot.Key})
		}
		conditions = append(conditions, condition)
	}
	result, err := ledger.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err == nil && !result.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Hook preparation still has retained sources or changed during cleanup")
	}
	return false, err
}
