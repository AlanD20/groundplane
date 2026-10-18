package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishDirectReleaseWithWork accepts one explicitly selected Service release.
// The pending native item and all of its frozen preparation become visible in
// the same transaction. Nothing is assignable until its complete executable
// input is prepared; staging a Release is not acknowledgement of runtime.
func (ledger *ReleaseLedger) PublishDirectReleaseWithWork(ctx context.Context,
	task workexecution.Task, item workexecution.Item, request DirectReleasePreparation,
	rollout releaserender.ServiceRolloutCapture, addresses networkreservations.ProxyAddresses,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if ledger == nil || ledger.store == nil || task.Action != workexecution.ActionDeploy && task.Action != workexecution.ActionRollback ||
		task.TargetID != item.TargetID || item.TaskID != task.ID || item.Operation != workexecution.Release ||
		item.Input != nil || item.Phase != workexecution.Waiting || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment || marker.Locator.ScopeID != task.Scope.EnvironmentID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Direct Release acceptance requires its pending native Service work")
	}
	if err := request.Matches(task, item, rollout); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	graph, err := request.WorkGraph(task, item)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	var change workqueue.Change
	if len(graph) == 1 {
		change, err = workqueue.PrepareSingleTask(task, item, marker)
	} else {
		change, err = workqueue.PrepareStagedGraph(task, graph, []workexecution.Item{item}, marker)
	}
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	preparationValue, _, err := EncodeDirectReleasePreparation(request)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(preparationValue)
	rolloutValue, err := releaserender.EncodeServiceRolloutCapture(rollout)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(rolloutValue)
	guards, err := ledger.directReleaseSourceConditions(ctx, task.Scope, request.Source, item.TargetID, request.Source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	host, err := hostbindings.ReadLocal(ctx, ledger.store, request.Source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if host.Binding.HostID != item.Placement.HostID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Direct Release target host changed during acceptance")
	}
	rolloutKey := releaserender.ServiceRolloutKey(task.Scope.EnvironmentID, task.ID, item.ID)
	preparationKey := DirectReleasePreparationKey(task.ID)
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, host.Conditions...)
	change.Conditions = append(change.Conditions, addresses.Conditions()...)
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: rolloutKey}, keyvalue.Condition{Key: preparationKey},
		keyvalue.Condition{Key: releases.ReleaseManifestStagingKey(rollout.PublicationID), ModRevision: request.Manifest.Revision},
		keyvalue.Condition{Key: releases.ReleasePublicationKey(rollout.PublicationID)},
	)
	change.Mutations = append(change.Mutations, addresses.Mutations()...)
	change.Mutations = append(change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: rolloutKey, Value: append([]byte(nil), rolloutValue...)},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: preparationKey, Value: append([]byte(nil), preparationValue...)},
	)
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask,
		change.Conditions, change.Mutations, resourceWorkPublicationConflict(change.Conditions))
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replay, err := NewIdempotencyRepository(ledger.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return replay.Apply(ctx, marker, plan)
}

func (ledger *ReleaseLedger) ResolveReleaseHost(ctx context.Context, revision int64) (workexecution.Placement, error) {
	if ledger == nil || ledger.store == nil || revision <= 0 {
		return workexecution.Placement{}, errs.New(errs.KindValidationFailed, "Release host selection requires a fixed planning snapshot")
	}
	host, err := hostbindings.ReadLocal(ctx, ledger.store, revision)
	if err != nil {
		return workexecution.Placement{}, err
	}
	return workexecution.Placement{Executor: workexecution.Agent, HostID: host.Binding.HostID}, nil
}
