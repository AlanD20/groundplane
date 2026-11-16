package scripts

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptremoval"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
	"time"
)

const scriptDeletionRoute = "/scripts/{id}"

type scriptDeletionRepository interface {
	GetEnvironment(context.Context, string) (keyvalue.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (keyvalue.Versioned[hierarchyrecord.ProjectRecord], error)
	GetService(context.Context, string) (keyvalue.Versioned[servicerecord.ServiceRecord], error)
	GetScript(context.Context, string) (keyvalue.Versioned[scriptrecord.Record], error)
	PrepareScriptRemovalAuthority(context.Context, keyvalue.Versioned[hierarchyrecord.EnvironmentRecord],
		keyvalue.Versioned[hierarchyrecord.ProjectRecord], keyvalue.Versioned[servicerecord.ServiceRecord],
		keyvalue.Versioned[scriptrecord.Record], scriptremoval.Input, workexecution.Task, int64, idempotency.IdempotencyLocator,
	) (workqueue.Change, workexecution.Item, error)
}

func (repository *MutationRepository) PrepareScriptRemovalAuthority(
	ctx context.Context,
	environment keyvalue.Versioned[hierarchyrecord.EnvironmentRecord],
	project keyvalue.Versioned[hierarchyrecord.ProjectRecord],
	target keyvalue.Versioned[servicerecord.ServiceRecord],
	current keyvalue.Versioned[scriptrecord.Record],
	input scriptremoval.Input,
	task workexecution.Task,
	revision int64,
	locator idempotency.IdempotencyLocator,
) (workqueue.Change, workexecution.Item, error) {
	return repository.scripts.PrepareScriptRemovalAuthority(
		ctx,
		environment,
		project,
		target,
		current,
		input,
		task,
		revision,
		locator,
	)
}

type scriptDeletionService struct {
	repository scriptDeletionRepository
	accepted   *acceptedaction.Submitter
	now        func() time.Time
}

func NewDeletionService(
	repository scriptDeletionRepository,
	accepted *acceptedaction.Submitter,
) (*scriptDeletionService, error) {
	if repository == nil || accepted == nil {
		return nil, errs.New(errs.KindInternal, "Script deletion service is not configured")
	}
	return &scriptDeletionService{repository: repository, accepted: accepted, now: time.Now}, nil
}

type acceptedScriptRemoval struct {
	After []string `json:"after,omitempty"`
	Key   string   `json:"key"`
}

func (service *scriptDeletionService) RemoveScript(
	ctx context.Context,
	scriptID string,
	input api.TaskRunAfterRequest,
	key string,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindScript, scriptID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Script id is invalid")
	}
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetScript, ID: scriptID}
	locator, indexed, err := service.accepted.ReplayLocator(ctx, target, http.MethodDelete, scriptDeletionRoute, key)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	var scope workexecution.Scope
	var name string
	if !indexed {
		current, err := service.repository.GetScript(ctx, scriptID)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		scope = workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environment.Record.ID,
		}
		name = current.Record.Desired.Slug
		locator = idempotency.IdempotencyLocator{
			ScopeKind: idempotency.IdempotencyScopeEnvironment,
			ScopeID:   environment.Record.ID,
			Method:    http.MethodDelete,
			Route:     scriptDeletionRoute,
			Key:       key,
		}
	}
	intent, err := service.accepted.Begin(
		ctx,
		requestidempotency.CanonicalIntentV1{Method: http.MethodDelete, Route: scriptDeletionRoute,
			Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: locator.ScopeID},
			Path:  []requestidempotency.PathBinding{{Name: "id", Value: scriptID}}, Query: requestidempotency.Object(),
			Body: requestidempotency.JSONBody(
				requestidempotency.Object(
					requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)},
				),
			)},
		locator,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer intent.Destroy()
	if replay, exists := intent.Replay(); exists {
		return replay, nil
	}
	if indexed {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Script deletion replay target is inconsistent",
		)
	}
	at := service.now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionRemove,
		TargetID:   scriptID,
		TargetName: name,
		Scope:      scope,
		After:      append([]string(nil), input.After...),
		CreatedAt:  at,
		UpdatedAt:  at,
		Deadline:   at.Add(acceptedaction.QueueTimeout),
	}
	submission := intent.Submission(task, "script-remove")
	submission.ReplayTarget = &target
	return acceptedaction.Submit(ctx, service.accepted, submission, acceptedScriptRemoval{After: input.After, Key: key})
}

func (service *scriptDeletionService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.accepted,
			"script-remove",
			workexecution.ActionRemove,
			ids.KindScript,
			service.prepareAcceptedRemoval,
		),
	}
}

func (service *scriptDeletionService) prepareAcceptedRemoval(
	ctx context.Context,
	accepted workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	request acceptedScriptRemoval,
) (taskdependencies.Prepared, error) {
	task := accepted.Task
	current, err := service.repository.GetScript(ctx, task.TargetID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	target, err := service.repository.GetService(ctx, current.Record.ServiceID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if task.Scope != (workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID}) {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Script ownership changed while this removal was queued",
		)
	}
	input := scriptremoval.Input{
		TaskID:     task.ID,
		WorkItemID: ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "script-remove-work"),
		PlanID: ids.DeriveAt(
			ids.KindPlan,
			task.CreatedAt,
			task.ID,
			"script-remove-input",
		),
		TenantID:            task.Scope.TenantID,
		ProjectID:           task.Scope.ProjectID,
		EnvironmentID:       task.Scope.EnvironmentID,
		ServiceID:           current.Record.ServiceID,
		ScriptID:            task.TargetID,
		ScriptRevision:      current.Revision,
		ScriptSetGeneration: current.Record.ScriptSetGeneration,
		CreatedAt:           at,
	}
	task.Intent, err = input.Reference()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.ExpectedItems = 1
	task.UpdatedAt = at
	locator := idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID:   task.Scope.EnvironmentID,
		Method:    http.MethodDelete,
		Route:     scriptDeletionRoute,
		Key:       request.Key,
	}
	authority, item, err := service.repository.PrepareScriptRemovalAuthority(
		ctx,
		environment,
		project,
		target,
		current,
		input,
		task,
		accepted.Revision,
		locator,
	)
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, err
}
