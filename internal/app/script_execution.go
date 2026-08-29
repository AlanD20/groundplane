package app

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const scriptRunRoute = "/scripts/{id}/run"

func (service *scriptMutationService) RunScript(
	ctx context.Context,
	scriptID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if service == nil || service.repository == nil || service.idempotency == nil || service.artifacts == nil || ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script execution service is not configured")
	}
	if ids.Validate(ids.KindScript, scriptID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Script id is invalid")
	}
	sources, err := service.repository.LoadExecutionSources(ctx, scriptID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	intent := scriptMutationIntent{
		method: http.MethodPost, route: scriptRunRoute,
		environmentID: sources.Environment.Record.ID,
		path:          []idempotentintent.PathBinding{{Name: "id", Value: scriptID}},
		noBody:        true,
	}
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := scriptMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return scriptReplayResponse(resolution)
	}

	now := service.now().UTC()
	owner, err := etcd.EnvironmentTaskOwner(sources.Project.Record, sources.Environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: owner, Actor: etcd.TaskActorOperator, Executor: etcd.TaskExecutorAgent,
		PlanID: ids.New(ids.KindPlan), Type: etcd.TaskScript, Target: scriptID,
		Params: map[string]string{
			etcd.ScriptExecutionIDParam: ids.NewULID(),
			etcd.ScriptGenerationParam:  jsonNumber(sources.BodyGeneration.Record.Generation),
		},
		Steps:          []etcd.TaskStepRecord{{ID: ids.New(ids.KindStep)}},
		TimeoutSeconds: executionplan.ScriptExecutionTimeoutSeconds,
		Status:         etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	entryBindings, err := service.artifacts.BuildScriptEntryBindings(ctx, sources)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	plan, err := controller.BuildManualScriptPlan(ctx, controller.ManualScriptPlanInput{
		TaskID: task.ID, OperationID: task.OperationID, PlanID: task.PlanID, StepID: task.Steps[0].ID,
		ExecutionID: task.Params[etcd.ScriptExecutionIDParam], SnapshotID: ids.NewULID(), Sources: sources,
		EntryBindings: entryBindings,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task.RenderGeneration = int32(plan.RenderGeneration)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	execution, err := etcd.NewScriptExecutionRecord(task, plan, now)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(execution.Plan)
	defer clear(execution.Snapshot)

	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: responseBody,
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator:      locator,
		ReplayTarget: &etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetScript, ID: scriptID},
		Intent:       evidence.durable,
		Response: etcd.IdempotencyResponse{
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
