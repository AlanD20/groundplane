package runner

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/runnersoftware"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerupdates"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type acceptedRunnerUpdate struct {
	Request   api.RunnerUpdateRequest `json:"request"`
	Automatic bool                    `json:"automatic"`
}

func (service *ConnectionCheckService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.submitter,
			"runner-check",
			workexecution.ActionCheck,
			ids.KindRunner,
			service.prepareAcceptedCheck,
		),
	}
}

func (service *SoftwareService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.submitter,
			"runner-software-update",
			workexecution.ActionUpdate,
			ids.KindRunner,
			service.prepareAcceptedUpdate,
		),
	}
}

func runnerRequestID(task workexecution.Task, purpose string) string {
	return strings.TrimPrefix(
		ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, purpose),
		string(ids.KindOperation)+"_",
	)
}

func (service *ConnectionCheckService) prepareAcceptedCheck(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, input api.RunnerCheckRequest,
) (taskdependencies.Prepared, error) {
	task := accepted.Task
	if input.Revision == 0 {
		current, err := service.runners.GetRunner(ctx, task.TargetID)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		input.Revision = current.Revision
	}
	source, err := service.runners.LoadRunnerRuntimeSource(ctx, task.TargetID, input.Revision)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	runtime := source.Runner()
	plan, err := service.runners.LoadAppliedRunnerPlan(ctx, task.TargetID, runtime.RuntimeEpoch, runtime.ContainerID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if err = corerunner.ValidatePlan(plan); err != nil {
		return taskdependencies.Prepared{}, err
	}
	owner := runnerchecks.Owner{TaskID: task.ID,
		WorkItemID:  ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "runner-check-work"),
		PlanID:      ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "runner-check-plan"),
		OperationID: ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "runner-check-operation")}
	request := runnerconnectioncheck.Request{
		RequestID:          runnerRequestID(task, "runner-check-request"),
		RunnerID:           task.TargetID,
		RuntimeEpoch:       runtime.RuntimeEpoch,
		PlanSHA256:         plan.Digest(),
		ControllerEndpoint: plan.Egress.ControllerEndpoint.String(),
		RunnerHome:         plan.Paths.RunnerHome,
	}
	record, err := runnerchecks.NewRecord(owner, source.Configuration(), plan, request, at)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	hash, err := runnerchecks.PlanHash(record)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent, task.ExpectedItems, task.UpdatedAt = workexecution.Input{ID: owner.PlanID, SHA256: hash}, 1, at
	item := runnerNativeItem(task, owner.WorkItemID, workexecution.RunnerCheck, at, time.Minute)
	change, err := service.runners.PrepareRunnerCheck(ctx, source, record, task, item)
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: change}, err
}

func (service *SoftwareService) prepareAcceptedUpdate(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, input acceptedRunnerUpdate,
) (taskdependencies.Prepared, error) {
	task := accepted.Task
	if input.Request.Revision == 0 {
		current, err := service.runners.GetRunner(ctx, task.TargetID)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		input.Request.Revision = current.Revision
	}
	source, err := service.runners.LoadRunnerRuntimeSource(ctx, task.TargetID, input.Request.Revision)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	runtime := source.Runner()
	if runtime.Desired.AutoUpdate != input.Automatic {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Runner automatic-update policy changed after this update was requested",
		)
	}
	release, err := service.catalog.Resolve(ctx, input.Request.Version)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	plan, err := service.runners.LoadAppliedRunnerPlan(ctx, task.TargetID, runtime.RuntimeEpoch, runtime.ContainerID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if err = corerunner.ValidatePlan(plan); err != nil {
		return taskdependencies.Prepared{}, err
	}
	owner := runnerupdates.Owner{TaskID: task.ID,
		WorkItemID:  ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "runner-update-work"),
		PlanID:      ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "runner-update-plan"),
		OperationID: ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "runner-update-operation")}
	request := runnersoftware.Request{
		Release:            release,
		RequestID:          runnerRequestID(task, "runner-update-request"),
		RunnerID:           task.TargetID,
		RuntimeEpoch:       runtime.RuntimeEpoch,
		PlanSHA256:         plan.Digest(),
		ControllerEndpoint: plan.Egress.ControllerEndpoint.String(),
		RunnerHome:         plan.Paths.RunnerHome,
	}
	record, err := runnerupdates.NewRecord(owner, source.Configuration(), plan, request, at)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	hash, err := runnerupdates.PlanHash(record)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent, task.ExpectedItems, task.UpdatedAt = workexecution.Input{ID: owner.PlanID, SHA256: hash}, 1, at
	item := runnerNativeItem(task, owner.WorkItemID, workexecution.RunnerSoftware, at, 30*time.Minute)
	change, err := service.runners.PrepareRunnerSoftwareUpdate(ctx, source, record, task, item)
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: change}, err
}
