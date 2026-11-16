package runner

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *ProvisioningService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.submitter,
			"runner-create",
			workexecution.ActionCreate,
			ids.KindRunner,
			service.prepareAcceptedCreation,
		),
	}
}

func (service *RemovalService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.submitter,
			"runner-remove",
			workexecution.ActionRemove,
			ids.KindRunner,
			service.prepareAcceptedRemoval,
		),
	}
}

func (service *ProvisioningService) prepareAcceptedCreation(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, request api.RunnerCreateRequest,
) (taskdependencies.Prepared, error) {
	task := accepted.Task
	if request.RegistrationToken != "" {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindInternal,
			"Runner token cannot be stored in accepted Task input",
		)
	}
	workID := ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "runner-create-work")
	key := TokenKey{TaskID: task.ID, WorkItemID: workID, Attempt: 1}
	if !service.broker.Available(key) {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindValidationFailed,
			"The transient GitHub registration token is no longer available. Register the Runner again with a fresh token; no host resources were allocated.",
		)
	}
	desired, err := service.normalizeCreate(ctx, request, task.TargetID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if _, err := service.runners.ResolveRunner(ctx, desired.TenantID, desired.Slug); err == nil {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindRunnerSlugConflict,
			"A Runner with this slug already exists in the Tenant",
		)
	} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindRunnerNotFound {
		return taskdependencies.Prepared{}, err
	}
	if task.Scope != (workexecution.Scope{TenantID: desired.TenantID, ProjectID: desired.ProjectID(), EnvironmentID: desired.EnvironmentID()}) {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Runner ownership changed after registration was requested",
		)
	}
	if err = service.policy.RequireControllerEndpoint(); err != nil {
		return taskdependencies.Prepared{}, err
	}
	source, err := service.runners.PrepareRunnerCreation(ctx, service.allocation, desired, task.ID, at)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer source.Clear()
	record := source.Runner()
	plan, err := corerunner.NewPlan(corerunner.Target{RunnerID: desired.ID, TenantID: desired.TenantID,
		OwnerKind: corerunner.OwnerKind(desired.OwnerKind), OwnerID: desired.OwnerID,
		GitHubURL: desired.GitHubURL, RunnerGroup: desired.RunnerGroup, GitHubName: desired.GitHubName,
		CPUMillis: desired.CPUMillis, MemoryBytes: desired.MemoryBytes, Labels: desired.Labels, ImageRef: desired.ImageRef,
		RuntimeEpoch: record.RuntimeEpoch + 1, AllocationConfig: service.allocation, Allocation: record.Allocation}, service.policy)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	input := runnerwork.Input{TaskID: task.ID, WorkItemID: workID,
		PlanID: ids.DeriveAt(
			ids.KindPlan,
			task.CreatedAt,
			task.ID,
			"runner-create-plan",
		), Operation: runnerallocation.RuntimeOperationCreate,
		Desired: desired, Plan: plan, CreatedAt: at}
	ref, err := input.Reference()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent, task.ExpectedItems, task.UpdatedAt = ref, 1, at
	item := runnerNativeItem(
		task,
		workID,
		workexecution.Runner,
		at,
		time.Duration(runnerCreateTimeoutSeconds)*time.Second,
	)
	change, err := service.runners.PrepareRunnerCreationCapture(ctx, source, input, task, item)
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: change}, err
}

func (service *RemovalService) prepareAcceptedRemoval(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, _ api.TaskRunAfterRequest,
) (taskdependencies.Prepared, error) {
	task := accepted.Task
	current, err := service.repository.GetRunner(ctx, task.TargetID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	source, err := service.repository.PrepareRunnerRemoval(ctx, current, task.ID,
		ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "runner-remove-work"),
		ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "runner-remove-plan"), at)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	input := source.Input()
	if err = corerunner.ValidatePlan(input.Plan); err != nil {
		return taskdependencies.Prepared{}, err
	}
	ref, err := input.Reference()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent, task.ExpectedItems, task.UpdatedAt = ref, 1, at
	item := runnerNativeItem(
		task,
		input.WorkItemID,
		workexecution.Runner,
		at,
		time.Duration(runnerRemoveTimeoutSeconds)*time.Second,
	)
	change, err := service.repository.PrepareRunnerRemovalCapture(ctx, source, task, item)
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: change}, err
}
