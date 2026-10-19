package etcd

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RetireUnpreparedGroupPreflight accounts for source/snapshot staging even when
// cancellation won before executable input publication. A generic no-effect
// receipt alone cannot prove these retained preparations were released.
func (repository *ScriptRepository) RetireUnpreparedGroupPreflight(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if repository == nil || repository.store == nil || task.FinishedAt == nil || task.CollectionStartedAt == nil ||
		task.Progress.Unsettled() != 0 || at.Before(task.UpdatedAt) || item.Input != nil || item.Operation != workexecution.Script ||
		item.Phase != workexecution.Settled || item.AttemptNumber != 0 || item.ActiveAttemptID != "" {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Unprepared Group hook cleanup requires collecting no-effect work",
		)
	}
	groups, err := releasegroups.NewExecutionRepository(repository.store)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	root, err := groups.Load(ctx, view.Task, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	index := slices.IndexFunc(
		root.Record.Preflight,
		func(hook releasegroups.PreflightHook) bool { return hook.WorkItemID == item.ID },
	)
	if index < 0 {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Unprepared Script is not captured Group preflight",
		)
	}
	key := GroupReleasePreflightCaptureKey(task.ID, item.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Unprepared Group hook lost its source cleanup descriptor",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	capture, err := recordcodec.Decode[GroupReleasePreflightCapture](value.Value, "group-release-preflight")
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer capture.Execution.Clear()
	execution := capture.Execution
	if value.Key != key || value.ModRevision <= 0 || execution.Input != root.Record.Preflight[index].Input ||
		execution.CurrentTaskID != task.ID || execution.WorkItemID != item.ID || scriptexecutions.ValidateScriptExecutionRecord(execution) != nil {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Unprepared Group hook cleanup descriptor differs from captured input",
		)
	}
	guards := []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: releasegroups.ExecutionCaptureKey(task.ID), ModRevision: root.Revision},
		{Key: key, ModRevision: value.ModRevision},
		{Key: scriptexecutions.WorkKey(item.ID)}, {Key: scriptexecutions.ScriptExecutionKey(execution.ID)}}
	authority, err := scriptsourcepublication.NewAuthority(repository.store)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	drained, err := authority.AbandonOwnedPreparation(ctx, execution.OperationID, guards)
	if err != nil || !drained {
		return false, workqueue.Change{}, err
	}
	guards = append(guards, keyvalue.Condition{Key: sourceref.RootKey(execution.OperationID)},
		keyvalue.Condition{Key: sourceref.PreparationKey(execution.OperationID)},
		keyvalue.Condition{Key: sourceref.ReversePrefix(execution.OperationID), Prefix: true})
	snapshotKey := scriptexecutions.ScriptRunnerSnapshotKey(execution.SnapshotID)
	snapshots, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{snapshotKey}})
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if snapshots == nil || len(snapshots.Values) != 1 {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Unprepared Group hook snapshot cleanup read is incomplete",
		)
	}
	defer keyvalue.ClearValues(snapshots.Values)
	snapshotGuard := keyvalue.Condition{Key: snapshotKey}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}}
	if saved := snapshots.Values[0]; saved != nil {
		expected, err := recordcodec.Encode("script-runner-snapshot", scriptsourceevidence.StoredScriptRunnerSnapshot{
			ExecutionID: execution.ID, SnapshotID: execution.SnapshotID, SHA256: execution.SnapshotSHA256, Payload: execution.Snapshot,
		})
		if err != nil {
			return false, workqueue.Change{}, err
		}
		matches := saved.Key == snapshotKey && saved.ModRevision > 0 && bytes.Equal(saved.Value, expected)
		clear(expected)
		if !matches {
			return false, workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Unprepared Group hook snapshot changed; cleanup refused",
			)
		}
		snapshotGuard.ModRevision = saved.ModRevision
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: snapshotKey})
	}
	return true, workqueue.Change{Conditions: append(guards, snapshotGuard), Mutations: mutations}, nil
}
