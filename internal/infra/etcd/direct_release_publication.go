package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareDirectReleaseCapture returns immutable Service execution inputs and
// their source comparisons. The caller joins them to promotion of the original
// accepted Task; preparing this change grants no runtime execution authority.
func (ledger *ReleaseLedger) PrepareDirectReleaseCapture(ctx context.Context,
	task workexecution.Task, item workexecution.Item, request DirectReleasePreparation,
	rollout releaserender.ServiceRolloutCapture, addresses networkreservations.ProxyAddresses,
) (workqueue.Change, error) {
	return ledger.prepareDirectReleaseCapture(ctx, task, item, request, rollout, addresses, "")
}

// PrepareRemoteDirectReleaseCapture admits only the matching durable host
// binding. The caller must also compare its executor and remote readiness
// witnesses in the transaction publishing this change.
func (ledger *ReleaseLedger) PrepareRemoteDirectReleaseCapture(ctx context.Context,
	task workexecution.Task, item workexecution.Item, request DirectReleasePreparation,
	rollout releaserender.ServiceRolloutCapture, addresses networkreservations.ProxyAddresses,
	executor workexecution.Identity,
) (workqueue.Change, error) {
	if executor.Validate() != nil || executor.Executor != workexecution.Agent ||
		item.Placement != (workexecution.Placement{Executor: workexecution.Agent, HostID: executor.HostID}) ||
		task.Action != workexecution.ActionDeploy || request.GroupID != "" {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"remote release requires its explicitly selected host and Agent",
		)
	}
	return ledger.prepareDirectReleaseCapture(ctx, task, item, request, rollout, addresses, executor.AgentID)
}

func (ledger *ReleaseLedger) prepareDirectReleaseCapture(ctx context.Context,
	task workexecution.Task, item workexecution.Item, request DirectReleasePreparation,
	rollout releaserender.ServiceRolloutCapture, addresses networkreservations.ProxyAddresses,
	explicitAgentID string,
) (workqueue.Change, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, err
	}
	if ledger == nil || ledger.store == nil ||
		task.Action != workexecution.ActionDeploy && task.Action != workexecution.ActionRollback ||
		task.TargetID != item.TargetID ||
		item.TaskID != task.ID ||
		item.Operation != workexecution.Release ||
		item.Input != nil ||
		item.Phase != workexecution.Waiting ||
		item.Wait == nil ||
		item.Wait.Reason != workexecution.WaitInput {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Direct Release acceptance requires its pending native Service work",
		)
	}
	if err := request.Matches(task, item, rollout); err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{}
	preparationValue, _, err := EncodeDirectReleasePreparation(request)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(preparationValue)
	rolloutValue, err := releaserender.EncodeServiceRolloutCapture(rollout)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(rolloutValue)
	guards, err := ledger.directReleaseSourceConditions(
		ctx,
		task.Scope,
		request.Source,
		item.TargetID,
		request.Source.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	var host hostbindings.Snapshot
	if explicitAgentID == "" {
		host, err = hostbindings.ReadLocal(ctx, ledger.store, request.Source.ReadRevision)
	} else {
		host, err = hostbindings.Read(ctx, ledger.store, explicitAgentID, request.Source.ReadRevision)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	if host.Binding.HostID != item.Placement.HostID {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Direct Release target host changed during acceptance",
		)
	}
	rolloutKey := releaserender.ServiceRolloutKey(task.Scope.EnvironmentID, task.ID, item.ID)
	preparationKey := DirectReleasePreparationKey(task.ID)
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, host.Conditions...)
	change.Conditions = append(change.Conditions, addresses.Conditions()...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: rolloutKey},
		keyvalue.Condition{Key: preparationKey},
		keyvalue.Condition{
			Key:         releases.ReleaseManifestStagingKey(rollout.PublicationID),
			ModRevision: request.Manifest.Revision,
		},
		keyvalue.Condition{Key: releases.ReleasePublicationKey(rollout.PublicationID)},
	)
	change.Mutations = append(change.Mutations, addresses.Mutations()...)
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: rolloutKey, Value: append([]byte(nil), rolloutValue...)},
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   preparationKey,
			Value: append([]byte(nil), preparationValue...),
		},
	)
	return change, nil
}

func (ledger *ReleaseLedger) ResolveReleaseHost(ctx context.Context, revision int64) (workexecution.Placement, error) {
	if ledger == nil || ledger.store == nil || revision <= 0 {
		return workexecution.Placement{}, errs.New(
			errs.KindValidationFailed,
			"Release host selection requires a fixed planning snapshot",
		)
	}
	host, err := hostbindings.ReadLocal(ctx, ledger.store, revision)
	if err != nil {
		return workexecution.Placement{}, err
	}
	return workexecution.Placement{Executor: workexecution.Agent, HostID: host.Binding.HostID}, nil
}
