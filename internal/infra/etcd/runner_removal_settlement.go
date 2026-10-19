package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRunnerRemovalResult joins resource release with native completion.
// A failed attempt with unknown effects keeps both allocation and deletion fence.
func (repository *RunnerRepository) PrepareRunnerRemovalResult(ctx context.Context,
	input keyvalue.Versioned[runnerwork.Input], result keyvalue.Versioned[runnerwork.Result],
) (workqueue.Change, error) {
	capture, outcome := input.Record, result.Record
	if input.Revision <= 0 || result.Revision <= 0 || outcome.Validate(capture) != nil ||
		capture.Operation != runnerallocation.RuntimeOperationRemove {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner result does not own a captured removal")
	}
	work, err := runnerwork.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions, err := work.RemovalAuthority(ctx, capture, input.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: append(conditions,
		keyvalue.Condition{Key: runnerwork.InputKey(capture.WorkItemID), ModRevision: input.Revision},
		keyvalue.Condition{Key: runnerwork.ResultKey(outcome.Attempt.ID), ModRevision: result.Revision})}
	id := capture.Desired.ID
	tombstone := deletions.TombstoneKey(string(deletions.DeletionTargetRunner), id)
	switch outcome.Effects {
	case runnerwork.EffectsUnknown:
		return change, nil
	case runnerwork.NoEffects:
		change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: tombstone}}
		return change, nil
	case runnerwork.RuntimeRemoved:
		// Validate above requires every removal receipt, including daemon,
		// network and filesystem cleanup. Outcome remains independent of cleanup.
	default:
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner removal result lacks cleanup evidence")
	}
	record := runners.RunnerRecord{Desired: capture.Desired, RunnerLifecycleRecord: capture.Removal.Lifecycle}
	allocation, err := repository.ReadRunnerAllocationEvidence(ctx, record, input.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer runners.ClearRunnerAllocationEvidence(allocation)
	quota, err := runners.DecodeRunnerTenantQuota(allocation.Quota.Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	quota, err = quota.Release(id)
	if err != nil {
		return workqueue.Change{}, err
	}
	system, err := runners.DecodeSystemPoolRegistry(allocation.System.Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	system, err = system.ReleaseRunner(id, record.Allocation.NetworkCIDR)
	if err != nil {
		return workqueue.Change{}, err
	}
	quotaValue, err := recordcodec.Encode("runner_tenant_quota", quota)
	if err != nil {
		return workqueue.Change{}, err
	}
	systemValue, err := recordcodec.Encode("system_pool_registry", system)
	if err != nil {
		clear(quotaValue)
		return workqueue.Change{}, err
	}
	for _, value := range []*keyvalue.KeyValue{allocation.Owner, allocation.Slug, allocation.Quota, allocation.Host, allocation.System} {
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision},
		)
	}
	change.Mutations = []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: allocation.Quota.Key, Value: quotaValue},
		{Type: keyvalue.MutationPut, Key: allocation.System.Key, Value: systemValue},
	}
	for _, key := range []string{tombstone, runners.RunnerKey(id), runners.RunnerLifecycleKey(id),
		runners.RunnerRuntimeOwnershipKey(id), runnerwork.AppliedKey(id), runnerwork.AllocationPlanKey(id),
		runners.RunnerObservationKey(id), allocation.Owner.Key, allocation.Slug.Key, allocation.Host.Key} {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	return change, nil
}
