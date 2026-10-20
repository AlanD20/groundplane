package scripts

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const scriptRunRoute = "/scripts/{id}/run"

func (service *scriptMutationService) RunScript(
	ctx context.Context,
	scriptID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if service == nil || service.repository == nil || service.idempotency == nil || service.preparation == nil ||
		ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Script execution service is not configured",
		)
	}
	if ids.Validate(ids.KindScript, scriptID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Script id is invalid")
	}
	ctx = imagefence.WithScope(ctx)
	sources, err := service.repository.LoadExecutionSources(ctx, scriptID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := scriptMutationIntent{
		method: http.MethodPost, route: scriptRunRoute,
		environmentID: sources.Environment.Record.ID,
		path:          []requestidempotency.PathBinding{{Name: "id", Value: scriptID}},
		noBody:        true,
	}
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := scriptMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		return scriptReplayResponse(resolution)
	}

	now := service.now().UTC()
	taskID, workID := ids.New(ids.KindTask), ids.New(ids.KindWorkItem)
	operationID, planID := ids.New(ids.KindOperation), ids.New(ids.KindPlan)
	prepared, err := service.preparation.Prepare(ctx, sources)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	runner, err := taskplanning.PrepareManualScriptRunner(ctx, taskplanning.ScriptRunnerInput{
		TaskID: taskID, WorkItemID: workID, OperationID: operationID, PlanID: planID,
		ExecutionID: ids.NewULID(), SnapshotID: ids.NewULID(), Sources: sources,
		Preparation: prepared,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	execution, err := scriptexecutions.NewExecution(scriptexecutions.ExecutionInput{
		TaskID: taskID, WorkItemID: workID, OperationID: operationID, PlanID: planID,
		ScriptSetGeneration: sources.Script.Record.ScriptSetGeneration,
		Snapshot:            runner.Snapshot, Projection: runner.Projection, Run: runner.Run, Body: runner.Body,
	}, now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer execution.Clear()
	task := workexecution.Task{
		ID: taskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionScript,
		Scope: workexecution.Scope{TenantID: sources.Project.Record.TenantID,
			ProjectID: sources.Project.Record.ID, EnvironmentID: sources.Environment.Record.ID},
		TargetID: scriptID, TargetName: sources.Script.Record.Desired.Slug, Intent: execution.Input,
		CreatedAt: now, UpdatedAt: now, Deadline: now.Add(15 * time.Minute), ExpectedItems: 1,
	}
	item := workexecution.Item{
		ID: workID, TaskID: taskID, Operation: workexecution.Script, TargetID: scriptID,
		TargetName: task.TargetName,
		Placement: workexecution.Placement{
			Executor: workexecution.Agent,
			HostID:   sources.Host.Binding.HostID,
		}, Input: &execution.Input,
		Claims: []workexecution.Claim{
			{ScopeID: sources.Service.Record.Desired.ID},
			{HostID: sources.Host.Binding.HostID, ScopeID: sources.Service.Record.Desired.ID},
		},
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: now, UpdatedAt: now, Deadline: task.Deadline,
	}

	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: responseBody,
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator,
		ReplayTarget: &idempotencyrecord.IdempotencyReplayTarget{
			Kind: idempotencyrecord.IdempotencyReplayTargetScript,
			ID:   scriptID,
		},
		Intent: evidence.durable,
		Response: idempotencyrecord.IdempotencyResponse{
			Status: response.Status, ContentKind: response.ContentKind, Body: append([]byte(nil), response.Body...),
		},
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.PublishScriptWork(ctx, sources, execution, task, item, marker)
	return service.resolveScriptMutation(ctx, locator, evidence, result, mutationErr, response)
}
