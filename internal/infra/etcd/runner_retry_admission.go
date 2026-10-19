package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRunnerRetry joins the resource transition to the same work-item Retry.
// Retrying never allocates another Runner, changes its captured plan, or grants
// permission to overwrite an existing daemon/container ownership record.
func (repository *RunnerRepository) PrepareRunnerRetry(ctx context.Context,
	current keyvalue.Versioned[runners.RunnerRecord], input runnerwork.Input,
) (workqueue.Change, error) {
	record := current.Record
	if input.Validate() != nil || current.Revision <= 0 || record.LifecycleRevision <= 0 ||
		record.CreateTaskID != input.TaskID || record.ProvisioningState != runners.RunnerProvisioningFailed ||
		record.ContainerID != "" || record.Allocation != input.Plan.Allocation ||
		!runnerCreationDesiredEqual(record.Desired, input.Desired) || record.RuntimeEpoch > input.Plan.RuntimeEpoch {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"Runner creation does not have a cleaned, unchanged allocation to retry",
		)
	}
	parents, err := repository.ResolveRunnerParents(ctx, record.Desired)
	if err != nil {
		return workqueue.Change{}, err
	}
	allocation, err := repository.ReadRunnerAllocationEvidence(ctx, record, current.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	record.ProvisioningState = runners.RunnerProvisioningProvisioning
	value, err := runners.EncodeRunnerLifecycleRecord(record.RunnerLifecycleRecord)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions := []keyvalue.Condition{
		{Key: runners.RunnerKey(record.Desired.ID), ModRevision: current.Revision},
		{Key: runners.RunnerLifecycleKey(record.Desired.ID), ModRevision: record.LifecycleRevision},
		{Key: runners.RunnerRuntimeOwnershipKey(record.Desired.ID)},
		{Key: deletions.TombstoneKey(string(deletions.DeletionTargetRunner), record.Desired.ID)},
		{
			Key:         runners.RunnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, record.Desired.ID),
			ModRevision: allocation.Owner.ModRevision,
		},
		{
			Key:         runners.RunnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug),
			ModRevision: allocation.Slug.ModRevision,
		},
		{Key: runners.RunnerTenantQuotaKey(record.Desired.TenantID), ModRevision: allocation.Quota.ModRevision},
		{Key: runners.RunnerHostSlotKey(record.Allocation.Slot), ModRevision: allocation.Host.ModRevision},
		{Key: runners.SystemPoolRegistryKey, ModRevision: allocation.System.ModRevision},
	}
	conditions = append(conditions, parents.AdmissionConditions()...)
	return workqueue.Change{Conditions: conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: runners.RunnerLifecycleKey(record.Desired.ID), Value: value},
	}}, nil
}
