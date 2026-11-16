package scripts

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *scriptMutationService) AcceptedPreparations() []taskdependencies.Registration {
	registrations := []taskdependencies.Registration{acceptedaction.Registration(service.accepted, "script-run",
		workexecution.ActionScript, ids.KindScript, service.prepareAcceptedScriptRun)}
	if service.deletions != nil {
		registrations = append(registrations, service.deletions.AcceptedPreparations()...)
	}
	return registrations
}

func (service *scriptMutationService) prepareAcceptedScriptRun(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, _ api.TaskRunAfterRequest,
) (taskdependencies.Prepared, error) {
	ctx = imagefence.WithScope(ctx)
	task := accepted.Task
	sources, err := service.repository.LoadExecutionSources(ctx, task.TargetID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if sources.Environment.Record.ID != task.Scope.EnvironmentID || sources.Project.Record.ID != task.Scope.ProjectID ||
		sources.Project.Record.TenantID != task.Scope.TenantID {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Script ownership changed after this Run was accepted",
		)
	}
	workID := ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "script-work")
	operationID := ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "script-operation")
	planID := ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "script-plan")
	executionID := strings.TrimPrefix(
		ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "script-execution"),
		string(ids.KindOperation)+"_",
	)
	snapshotID := strings.TrimPrefix(
		ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "script-snapshot"),
		string(ids.KindPlan)+"_",
	)
	preparation, err := service.preparation.Prepare(ctx, sources)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	runner, err := taskplanning.PrepareManualScriptRunner(ctx, taskplanning.ScriptRunnerInput{
		TaskID: task.ID, WorkItemID: workID, OperationID: operationID, PlanID: planID,
		ExecutionID: executionID, SnapshotID: snapshotID, Sources: sources, Preparation: preparation})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	execution, err := scriptexecutions.NewExecution(scriptexecutions.ExecutionInput{
		TaskID: task.ID, WorkItemID: workID, OperationID: operationID, PlanID: planID,
		ScriptSetGeneration: sources.Script.Record.ScriptSetGeneration,
		Snapshot:            runner.Snapshot, Projection: runner.Projection, Run: runner.Run, Body: runner.Body,
	}, at)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer execution.Clear()
	task.Intent, task.ExpectedItems = execution.Input, 1
	task.UpdatedAt = at
	deadline := acceptedaction.ExecutionDeadline(task, at, 15*time.Minute)
	item := workexecution.Item{ID: workID, TaskID: task.ID, Operation: workexecution.Script,
		TargetID: task.TargetID, TargetName: task.TargetName,
		Placement: workexecution.Placement{
			Executor: workexecution.Agent,
			HostID:   sources.Host.Binding.HostID,
		}, Input: &execution.Input,
		Claims: []workexecution.Claim{{ScopeID: sources.Service.Record.Desired.ID},
			{HostID: sources.Host.Binding.HostID, ScopeID: sources.Service.Record.Desired.ID}},
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: at, UpdatedAt: at, Deadline: deadline}
	publication, err := service.repository.PrepareScriptWork(ctx, sources, execution, task, item)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: publication.Change,
		Finalize: publication.Finish}, nil
}
