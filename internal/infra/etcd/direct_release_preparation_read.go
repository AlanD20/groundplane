package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (ledger *ReleaseLedger) ReadDirectReleasePreparation(ctx context.Context, work workqueue.View) (DirectReleasePreparation, keyvalue.Condition, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return DirectReleasePreparation{}, keyvalue.Condition{}, err
	}
	task, item := work.Task.Task, work.Item.Item
	if ledger == nil || ledger.store == nil || work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		work.ReadRevision < work.Task.Revision || work.ReadRevision < work.Item.Revision ||
		task.Validate() != nil || item.Validate() != nil || item.TaskID != task.ID || item.Operation != workexecution.Release ||
		task.Action != workexecution.ActionDeploy && task.Action != workexecution.ActionRollback {
		return DirectReleasePreparation{}, keyvalue.Condition{}, errs.New(errs.KindValidationFailed, "Direct Release preparation requires its native work snapshot")
	}
	key := DirectReleasePreparationKey(item.ID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: work.ReadRevision})
	if err != nil {
		return DirectReleasePreparation{}, keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != 1 {
		return DirectReleasePreparation{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Direct Release preparation snapshot is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > work.ReadRevision {
		return DirectReleasePreparation{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Direct Release has no accepted preparation")
	}
	request, err := DecodeDirectReleasePreparation(value.Value, task.Intent)
	if err != nil {
		return DirectReleasePreparation{}, keyvalue.Condition{}, err
	}
	if request.Configuration.WorkItemID != item.ID || request.Configuration.TaskID != task.ID ||
		request.Source.ReadRevision > value.ModRevision {
		return DirectReleasePreparation{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Direct Release preparation belongs to another work item")
	}
	return request, keyvalue.Condition{Key: key, ModRevision: value.ModRevision}, nil
}

// DirectReleasePreparationConditions compares the exact accepted source. It
// does not authorize effects; publication/assignment/Start/checkpoint owners
// join these comparisons to their own native state transaction. Recovery does
// not use current desired-state conditions.
func (ledger *ReleaseLedger) DirectReleasePreparationConditions(ctx context.Context,
	work workqueue.View, request DirectReleasePreparation,
	rollout keyvalue.Versioned[releaserender.ServiceRolloutCapture],
) ([]keyvalue.Condition, error) {
	if err := request.Matches(work.Task.Task, work.Item.Item, rollout.Record); err != nil {
		return nil, err
	}
	stored, condition, err := ledger.ReadDirectReleasePreparation(ctx, work)
	if err != nil {
		return nil, err
	}
	expectedBytes, _, err := EncodeDirectReleasePreparation(request)
	if err != nil {
		return nil, err
	}
	defer clear(expectedBytes)
	storedBytes, _, err := EncodeDirectReleasePreparation(stored)
	if err != nil {
		return nil, err
	}
	defer clear(storedBytes)
	if !bytes.Equal(expectedBytes, storedBytes) || rollout.Revision <= 0 || rollout.Revision > work.ReadRevision {
		return nil, errs.New(errs.KindStateConflict, "Direct Release preparation changed before publication")
	}
	guards, err := ledger.directReleaseSourceConditions(ctx, work.Task.Task.Scope, request.Source, request.Intent.ServiceID, work.ReadRevision)
	if err != nil {
		return nil, err
	}
	return append(guards, condition, keyvalue.Condition{
		Key:         releaserender.ServiceRolloutKey(work.Task.Task.Scope.EnvironmentID, work.Task.Task.ID, work.Item.Item.ID),
		ModRevision: rollout.Revision,
	}), nil
}

func (ledger *ReleaseLedger) directReleaseSourceConditions(ctx context.Context, scope workexecution.Scope,
	source DirectReleaseSource, serviceID string, revision int64,
) ([]keyvalue.Condition, error) {
	if ledger == nil || ledger.store == nil || ctx == nil || scope.Validate() != nil ||
		scope.EnvironmentID == "" || revision < source.ReadRevision {
		return nil, errs.New(errs.KindValidationFailed, "Direct Release source requires its captured hierarchy snapshot")
	}
	guards := []keyvalue.Condition{
		{Key: hierarchy.EnvironmentKey(scope.EnvironmentID), ModRevision: source.EnvironmentRevision},
		{Key: hierarchy.ProjectKey(scope.ProjectID), ModRevision: source.ProjectRevision},
		{Key: blueprints.EnvironmentBlueprintHeadKey(scope.EnvironmentID), ModRevision: source.DesiredRevision},
		{Key: hierarchy.EnvironmentMutationEpochKey(scope.EnvironmentID), ModRevision: source.EpochRevision},
		{Key: deletions.TombstoneKey("environment", scope.EnvironmentID)},
		{Key: deletions.TombstoneKey("project", scope.ProjectID)},
		{Key: deletions.TombstoneKey("service", serviceID)},
	}
	if scope.TenantID != "" {
		guards = append(guards, keyvalue.Condition{Key: hierarchy.TenantKey(scope.TenantID), ModRevision: source.TenantRevision},
			keyvalue.Condition{Key: deletions.TombstoneKey("tenant", scope.TenantID)})
	}
	keys := make([]string, len(guards))
	for index, guard := range guards {
		keys[index] = guard.Key
	}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, errs.New(errs.KindStateConflict, "Direct Release source snapshot is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, guard := range guards {
		value := read.Values[index]
		if guard.ModRevision == 0 && value != nil || guard.ModRevision > 0 &&
			(value == nil || value.Key != guard.Key || value.ModRevision != guard.ModRevision) {
			return nil, errs.New(errs.KindStateConflict, "Service or Environment configuration changed after this Release was accepted; submit a new Deploy or Rollback")
		}
	}
	return guards, nil
}
