package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RunnerCreationSource holds an uncommitted allocation. Publication compares
// every allocation and hierarchy source before making the Runner or work visible.
type RunnerCreationSource struct {
	record     runners.RunnerRecord
	config     runnerallocation.RunnerAllocationConfig
	parents    runners.RunnerParents
	conditions []keyvalue.Condition
	mutations  []keyvalue.Mutation
}

func (source RunnerCreationSource) Runner() runners.RunnerRecord {
	return runners.CloneRunnerRecord(source.record)
}
func (source *RunnerCreationSource) Clear() { keyvalue.ClearMutationValues(source.mutations) }

func (repository *RunnerRepository) PrepareRunnerCreation(
	ctx context.Context, config runnerallocation.RunnerAllocationConfig,
	desired runners.RunnerDesiredRecord, taskID string, at time.Time,
) (RunnerCreationSource, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return RunnerCreationSource{}, err
	}
	desired, err := runners.NormalizeRunnerDesired(desired)
	if err != nil {
		return RunnerCreationSource{}, err
	}
	slots, err := config.Validate()
	if err != nil {
		return RunnerCreationSource{}, err
	}
	parents, err := repository.ResolveRunnerParents(ctx, desired)
	if err != nil {
		return RunnerCreationSource{}, err
	}
	state, err := repository.getRunnerAllocationState(ctx, desired.TenantID, desired.ID, config, slots)
	if err != nil {
		return RunnerCreationSource{}, err
	}
	quota, err := state.quota.Record.Claim(desired.ID)
	if err != nil {
		return RunnerCreationSource{}, err
	}
	system, subnet, err := state.system.Record.ReserveRunner(config.RunnerPool, desired.ID)
	if err != nil {
		return RunnerCreationSource{}, err
	}
	allocation, err := config.HostPool.Allocation(state.host.slot, subnet)
	if err != nil {
		return RunnerCreationSource{}, err
	}
	record, err := runners.NewProvisioningRunner(desired, allocation, taskID, at)
	if err != nil {
		return RunnerCreationSource{}, err
	}
	source := RunnerCreationSource{record: record, config: config, parents: parents}
	source.conditions = []keyvalue.Condition{
		{Key: runners.RunnerKey(desired.ID)},
		{Key: runners.RunnerLifecycleKey(desired.ID)},
		{Key: runners.RunnerTenantSlugKey(desired.TenantID, desired.Slug)},
		{Key: runners.RunnerOwnerKey(desired.OwnerKind, desired.OwnerID, desired.ID)},
		{Key: runners.RunnerTenantQuotaKey(desired.TenantID), ModRevision: state.quota.Revision},
		{Key: runners.SystemPoolRegistryKey, ModRevision: state.system.Revision},
		{Key: deletions.TombstoneKey(string(deletions.DeletionTargetRunner), desired.ID)},
		state.host.condition,
	}
	source.conditions = append(source.conditions, parents.AdmissionConditions()...)
	desiredValue, err := runners.EncodeRunnerDesiredRecord(record.Desired)
	if err != nil {
		return RunnerCreationSource{}, err
	}
	lifecycleValue, err := runners.EncodeRunnerLifecycleRecord(record.RunnerLifecycleRecord)
	if err != nil {
		return RunnerCreationSource{}, err
	}
	quotaValue, err := recordcodec.Encode("runner_tenant_quota", quota)
	if err != nil {
		return RunnerCreationSource{}, err
	}
	hostValue, err := runners.EncodeRunnerHostSlotRecord(state.host.record)
	if err != nil {
		return RunnerCreationSource{}, err
	}
	systemValue, err := recordcodec.Encode("system_pool_registry", system)
	if err != nil {
		return RunnerCreationSource{}, err
	}
	source.mutations = []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: runners.RunnerKey(desired.ID), Value: desiredValue},
		{Type: keyvalue.MutationPut, Key: runners.RunnerLifecycleKey(desired.ID), Value: lifecycleValue},
		{
			Type:  keyvalue.MutationPut,
			Key:   runners.RunnerTenantSlugKey(desired.TenantID, desired.Slug),
			Value: []byte(desired.ID),
		},
		{
			Type:  keyvalue.MutationPut,
			Key:   runners.RunnerOwnerKey(desired.OwnerKind, desired.OwnerID, desired.ID),
			Value: []byte(desired.ID),
		},
		{Type: keyvalue.MutationPut, Key: runners.RunnerTenantQuotaKey(desired.TenantID), Value: quotaValue},
		{Type: keyvalue.MutationPut, Key: runners.RunnerHostSlotKey(state.host.slot), Value: hostValue},
		{Type: keyvalue.MutationPut, Key: runners.SystemPoolRegistryKey, Value: systemValue},
	}
	return source, nil
}

func (repository *RunnerRepository) PrepareRunnerCreationCapture(
	ctx context.Context, source RunnerCreationSource, input runnerwork.Input,
	task workexecution.Task, item workexecution.Item,
) (workqueue.Change, error) {
	record := source.record
	desired := record.Desired
	ref, err := input.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(source.conditions) == 0 || len(source.mutations) == 0 ||
		task.Validate() != nil || task.Actor != workexecution.OperatorActor || task.Action != workexecution.ActionCreate ||
		task.TargetID != desired.ID || task.ExpectedItems != 1 || task.PublicationComplete || task.Progress.Total() != 0 ||
		task.Scope != (workexecution.Scope{TenantID: desired.TenantID, ProjectID: desired.ProjectID(), EnvironmentID: desired.EnvironmentID()}) ||
		task.ID != record.CreateTaskID || task.ID != input.TaskID || task.Intent != ref ||
		input.Operation != runnerallocation.RuntimeOperationCreate || input.Plan.Allocation != record.Allocation ||
		input.Plan.AllocationConfig != source.config || input.Plan.RuntimeEpoch != record.RuntimeEpoch+1 ||
		!input.CreatedAt.Equal(
			record.CreatedAt,
		) || record.CreatedAt.Before(task.CreatedAt) || record.CreatedAt.After(task.UpdatedAt) ||
		!runnerCreationDesiredEqual(input.Desired, desired) {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Runner creation differs from its captured allocation or input",
		)
	}
	if task.AcceptedIntent == nil || item.Validate() != nil || item.TaskID != task.ID ||
		item.ID != input.WorkItemID || item.Operation != workexecution.Runner || item.TargetID != desired.ID ||
		item.Placement != (workexecution.Placement{Executor: workexecution.Controller}) ||
		item.Input == nil || *item.Input != task.Intent || !item.CreatedAt.Equal(input.CreatedAt) || item.Deadline.After(task.Deadline) {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Runner creation work differs from its accepted Task",
		)
	}
	inputConditions, inputMutations, err := runnerwork.PreparePublication(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearMutationValues(inputMutations)
	conditions := append([]keyvalue.Condition(nil), source.conditions...)
	conditions = append(conditions, inputConditions...)
	mutations := append([]keyvalue.Mutation(nil), source.mutations...)
	mutations = append(mutations, inputMutations...)
	allocationPlan, err := runnerwork.EncodeAllocationPlan(input.Plan)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions = append(conditions, keyvalue.Condition{Key: runnerwork.AllocationPlanKey(desired.ID)})
	mutations = append(
		mutations,
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   runnerwork.AllocationPlanKey(desired.ID),
			Value: allocationPlan,
		},
	)
	initiation, err := newRunnerTaskInitiation(desired, source.parents, taskjournal.TaskActorOperator)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions = append(conditions, initiation.fences...)
	return ownedRunnerCapture(workqueue.Change{Conditions: conditions, Mutations: mutations}), nil
}

func runnerCreationDesiredEqual(left, right runners.RunnerDesiredRecord) bool {
	return left.ID == right.ID && left.Slug == right.Slug && left.OwnerKind == right.OwnerKind &&
		left.OwnerID == right.OwnerID && left.TenantID == right.TenantID && left.ParentProjectID == right.ParentProjectID &&
		left.GitHubURL == right.GitHubURL && left.RunnerGroup == right.RunnerGroup && left.AutoUpdate == right.AutoUpdate && left.GitHubName == right.GitHubName && left.CPUMillis == right.CPUMillis && left.MemoryBytes == right.MemoryBytes && left.ImageRef == right.ImageRef && slices.Equal(left.Labels, right.Labels)
}
