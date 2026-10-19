package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RunnerRemovalSource struct {
	input      runnerwork.Input
	parents    runners.RunnerParents
	conditions []keyvalue.Condition
}

func (source RunnerRemovalSource) Input() runnerwork.Input { return source.input.Clone() }

func (repository *RunnerRepository) PrepareRunnerRemoval(ctx context.Context,
	current keyvalue.Versioned[runners.RunnerRecord], taskID, workID, planID string, at time.Time,
) (RunnerRemovalSource, error) {
	record := current.Record
	if current.Revision <= 0 || current.ReadRevision <= 0 || runners.ValidateRunnerRecord(record) != nil ||
		record.ProvisioningState == runners.RunnerProvisioningProvisioning {
		return RunnerRemovalSource{}, errs.New(errs.KindStateConflict, "Runner creation must settle before removal")
	}
	id := record.Desired.ID
	keys := []string{runnerwork.AllocationPlanKey(id), runners.RunnerRuntimeOwnershipKey(id), runnerwork.AppliedKey(id),
		deletions.TombstoneKey(
			string(deletions.DeletionTargetRunner),
			id,
		), workqueue.ClaimKey(workexecution.Claim{ScopeID: id})}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: current.ReadRevision})
	if err != nil {
		return RunnerRemovalSource{}, err
	}
	if read == nil {
		return RunnerRemovalSource{}, errs.New(errs.KindInternal, "Runner removal source read is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != current.ReadRevision || len(read.Values) != len(keys) || read.Values[0] == nil {
		return RunnerRemovalSource{}, errs.New(errs.KindStateConflict, "Runner captured allocation plan is unavailable")
	}
	if read.Values[3] != nil || read.Values[4] != nil {
		return RunnerRemovalSource{}, errs.New(
			errs.KindResourceInUse,
			"Runner still has active work or unresolved effects; finish its cleanup before removal",
		)
	}
	plan, err := runnerwork.DecodeAllocationPlan(read.Values[0].Value)
	if err != nil {
		return RunnerRemovalSource{}, err
	}
	input := runnerwork.Input{TaskID: taskID, WorkItemID: workID, PlanID: planID,
		Operation: runnerallocation.RuntimeOperationRemove, Desired: record.Desired, Plan: plan, CreatedAt: at,
		Removal: &runnerwork.RemovalSource{
			DesiredRevision:   current.Revision,
			LifecycleRevision: record.LifecycleRevision,
			Lifecycle:         record.RunnerLifecycleRecord,
		}}
	if read.Values[1] != nil {
		ownership, err := runners.DecodeRunnerRuntimeOwnership(read.Values[1].Value)
		if err != nil {
			return RunnerRemovalSource{}, err
		}
		input.Removal.Ownership = &ownership
	}
	if record.ProvisioningState == runners.RunnerProvisioningReady {
		if read.Values[2] == nil {
			return RunnerRemovalSource{}, errs.New(errs.KindStateConflict, "Runner applied runtime is unavailable")
		}
		applied, err := runnerwork.DecodeApplied(read.Values[2].Value)
		if err != nil || applied.Plan.Digest() != plan.Digest() || applied.Evidence.ContainerID != record.ContainerID ||
			applied.Evidence.Software != record.Software {
			return RunnerRemovalSource{}, errs.New(
				errs.KindStateConflict,
				"Runner applied runtime no longer matches its allocation",
			)
		}
	} else if read.Values[1] != nil || read.Values[2] != nil {
		return RunnerRemovalSource{}, errs.New(errs.KindStateConflict, "Failed Runner has unresolved applied ownership")
	}
	if err := input.Validate(); err != nil {
		return RunnerRemovalSource{}, err
	}
	parents, err := repository.ResolveRunnerParents(ctx, record.Desired)
	if err != nil {
		return RunnerRemovalSource{}, err
	}
	allocation, err := repository.ReadRunnerAllocationEvidence(ctx, record, current.ReadRevision)
	if err != nil {
		return RunnerRemovalSource{}, err
	}
	defer runners.ClearRunnerAllocationEvidence(allocation)
	conditions := []keyvalue.Condition{
		{Key: runners.RunnerKey(id), ModRevision: current.Revision},
		{Key: runners.RunnerLifecycleKey(id), ModRevision: record.LifecycleRevision},
	}
	for index, key := range keys {
		conditions = append(
			conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	for _, value := range []*keyvalue.KeyValue{allocation.Owner, allocation.Slug, allocation.Quota, allocation.Host, allocation.System} {
		conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
	}
	conditions = append(conditions, parents.AdmissionConditions()...)
	return RunnerRemovalSource{input: input.Clone(), parents: parents, conditions: conditions}, nil
}
