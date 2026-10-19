package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (ledger *ReleaseLedger) RetireUnpublishedRollout(ctx context.Context, view workqueue.View) (bool, error) {
	task, item := view.Task.Task, view.Item.Item
	if ctx == nil || task.Validate() != nil || item.Validate() != nil || task.CollectionStartedAt == nil ||
		task.FinishedAt == nil || task.Progress.Unsettled() != 0 || item.TaskID != task.ID ||
		item.Operation != workexecution.Release || item.Input != nil || item.AttemptNumber != 0 || item.Phase != workexecution.Settled {
		return false, errs.New(errs.KindStateConflict, "Rollout cleanup requires unpublished collecting work")
	}
	key := releaserender.ServiceRolloutKey(task.Scope.EnvironmentID, task.ID, item.ID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 {
		return false, errs.New(errs.KindStateConflict, "Rollout cleanup read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return true, nil
	}
	capture, err := releaserender.DecodeServiceRolloutCapture(value.Value)
	if err != nil {
		return false, err
	}
	defer clear(capture.CandidateArtifact)
	if value.Key != key || value.ModRevision <= 0 || capture.MatchesWork(task, item) != nil {
		return false, errs.New(errs.KindStateConflict, "Rollout cleanup ownership differs")
	}
	guards := []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision}, {Key: key, ModRevision: value.ModRevision},
		{Key: ReleaseWorkInputKey(item.ID)}, {Key: blueprintReleaseHookStageKey(item.ID)},
		{Key: releases.ReleasePublicationKey(capture.PublicationID)}}
	sources, err := scriptsourcepublication.NewAuthority(ledger.store)
	if err != nil {
		return false, err
	}
	done, err := sources.AbandonOwnedPreparation(ctx, capture.OperationID, guards)
	if err != nil || !done {
		return false, err
	}
	pins, err := tasksecretpins.NewEtcdRepository(ledger.store, task.Scope.ProjectID)
	if err != nil {
		return false, err
	}
	pinGuards := make([]tasksecretpins.Condition, len(guards))
	for index, guard := range guards {
		pinGuards[index] = tasksecretpins.Condition{Key: guard.Key, ModRevision: guard.ModRevision}
	}
	done, err = pins.AbandonOwned(ctx, capture.OperationID, task.ID, item.ID, pinGuards)
	if err != nil || !done {
		return false, err
	}
	for _, absent := range []string{sourceref.RootKey(capture.OperationID), sourceref.PreparationKey(capture.OperationID),
		tasksecretpins.RootKey(capture.OperationID), tasksecretpins.PreparationKey(capture.OperationID)} {
		guards = append(guards, keyvalue.Condition{Key: absent})
	}
	guards = append(guards, keyvalue.Condition{Key: sourceref.ReversePrefix(capture.OperationID), Prefix: true},
		keyvalue.Condition{Key: tasksecretpins.ReversePrefix(capture.OperationID), Prefix: true})
	result, err := ledger.store.Transact(ctx, guards, []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: key},
		{
			Type: keyvalue.MutationDelete,
			Key:  BlueprintProxyServiceRolloutKey(task.Scope.EnvironmentID, task.ID, item.ID),
		},
	})
	keyvalue.ClearValues(result.FailureReads)
	if err == nil && !result.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Rollout cleanup raced publication or retained inputs")
	}
	return false, err
}
