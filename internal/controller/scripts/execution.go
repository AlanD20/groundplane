package scripts

import (
	"context"
	"encoding/hex"
	"encoding/json"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
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
	owner, err := taskjournal.EnvironmentTaskOwner(sources.Project.Record, sources.Environment.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: owner, Actor: taskjournal.TaskActorOperator, Executor: taskjournal.TaskExecutorAgent,
		PlanID: ids.New(ids.KindPlan), Type: taskjournal.TaskScript, Target: scriptID,
		Params: map[string]string{
			scriptexecutions.ScriptExecutionIDParam: ids.NewULID(),
			scriptexecutions.ScriptGenerationParam:  jsonNumber(sources.BodyGeneration.Record.Generation),
		},
		Steps:          []taskjournal.TaskStepRecord{{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)}},
		TimeoutSeconds: executionplan.ScriptExecutionTimeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	prepared, err := service.preparation.Prepare(ctx, sources)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	plan, err := taskplanning.BuildManualScriptPlan(ctx, taskplanning.ManualScriptPlanInput{
		TaskID: task.ID, OperationID: task.OperationID, PlanID: task.PlanID, StepID: task.Steps[0].ID,
		ExecutionID: task.Params[scriptexecutions.ScriptExecutionIDParam], SnapshotID: ids.NewULID(), Sources: sources,
		Preparation: prepared,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task.RenderGeneration = int32(plan.RenderGeneration)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	execution, err := etcd.NewScriptExecutionRecord(task, plan, now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(execution.Plan)
	defer clear(execution.Snapshot)

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
	result, mutationErr := service.repository.PublishExecutionWithTask(ctx, sources, execution, task, marker)
	return service.resolveScriptMutation(ctx, locator, evidence, result, mutationErr, response)
}

func jsonNumber(value uint64) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
