package releasegroup

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type acceptedGroupRemoval struct {
	After []string `json:"after,omitempty"`
	Key   string   `json:"key"`
}

func (service *MutationService) RemoveReleaseGroup(
	ctx context.Context,
	groupID string,
	request api.TaskRunAfterRequest,
	key string,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindReleaseGroup, groupID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "release group id is invalid")
	}
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetReleaseGroup, ID: groupID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx,
		target,
		http.MethodDelete,
		releaseGroupRemoveRoute,
		key,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	var name string
	var scope workexecution.Scope
	if !indexed {
		current, err := service.groups.Get(ctx, groupID)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		environment, err := service.hierarchy.GetEnvironment(ctx, current.Group.EnvironmentID)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		project, err := service.hierarchy.GetProject(ctx, environment.Record.ProjectID)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		scope = workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environment.Record.ID,
		}
		name = current.Group.Name
		locator = idempotency.IdempotencyLocator{
			ScopeKind: idempotency.IdempotencyScopeEnvironment,
			ScopeID:   scope.EnvironmentID,
			Method:    http.MethodDelete,
			Route:     releaseGroupRemoveRoute,
			Key:       key,
		}
	}
	intent, err := service.accepted.Begin(
		ctx,
		requestidempotency.CanonicalIntentV1{Method: http.MethodDelete, Route: releaseGroupRemoveRoute,
			Scope: requestidempotency.Scope{
				Kind: requestidempotency.ScopeEnvironment,
				ID:   locator.ScopeID,
			}, Path: []requestidempotency.PathBinding{{Name: "id", Value: groupID}}, Query: requestidempotency.Object(),
			Body: requestidempotency.JSONBody(
				requestidempotency.Object(
					requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(request.After)},
				),
			)},
		locator,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer intent.Destroy()
	if response, exists := intent.Replay(); exists {
		return response, nil
	}
	if indexed {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"release group replay target is inconsistent",
		)
	}
	at := service.now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionRemove,
		TargetID:   groupID,
		TargetName: name,
		Scope:      scope,
		After:      request.After,
		CreatedAt:  at,
		UpdatedAt:  at,
		Deadline:   at.Add(acceptedaction.QueueTimeout),
	}
	submission := intent.Submission(task, "release-group-remove")
	submission.ReplayTarget = &target
	return acceptedaction.Submit(
		ctx,
		service.accepted,
		submission,
		acceptedGroupRemoval{After: request.After, Key: key},
	)
}

func (service *MutationService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.accepted,
			"release-group-remove",
			workexecution.ActionRemove,
			ids.KindReleaseGroup,
			service.prepareAcceptedRemoval,
		),
	}
}

func (service *MutationService) prepareAcceptedRemoval(
	ctx context.Context,
	snapshot workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	request acceptedGroupRemoval,
) (taskdependencies.Prepared, error) {
	task := snapshot.Task
	current, err := service.groups.Get(ctx, task.TargetID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	environment, err := service.hierarchy.GetEnvironment(ctx, current.Group.EnvironmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	project, err := service.hierarchy.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if task.Scope != (workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID}) {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Release Group ownership changed while waiting",
		)
	}
	prepared, err := service.tasks.PrepareReleaseGroupRemove(ctx, current.Group, current.Revision)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	input := releasegroups.RemovalInput{
		TaskID:        task.ID,
		WorkItemID:    ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "group-remove-work"),
		PlanID:        ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "group-remove-input"),
		Scope:         task.Scope,
		GroupID:       task.TargetID,
		GroupName:     current.Group.Name,
		GroupRevision: prepared.GroupRevision(),
		CreatedAt:     at,
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
		Route:     releaseGroupRemoveRoute,
		Key:       request.Key,
	}
	authority, item, err := service.tasks.PrepareReleaseGroupRemovalAuthority(
		ctx,
		environment,
		project,
		prepared,
		input,
		task,
		snapshot.Revision,
		locator,
	)
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, err
}
