package network

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

type acceptedRouteMutation struct {
	Create  *api.RouteCreate                  `json:"create,omitempty"`
	Edit    *api.RouteEdit                    `json:"edit,omitempty"`
	After   []string                          `json:"after,omitempty"`
	Locator idempotency.IdempotencyLocator    `json:"locator"`
	Intent  idempotency.ProtectedIntentRecord `json:"intent"`
}

func (service *routeMutationService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.accepted,
			"route-create",
			workexecution.ActionCreate,
			ids.KindRoute,
			service.prepareAcceptedRoute,
		),
		acceptedaction.Registration(
			service.accepted,
			"route-edit",
			workexecution.ActionUpdate,
			ids.KindRoute,
			service.prepareAcceptedRoute,
		),
		acceptedaction.Registration(
			service.accepted,
			"route-remove",
			workexecution.ActionRemove,
			ids.KindRoute,
			service.prepareAcceptedRoute,
		),
	}
}

func (service *routeMutationService) submitRouteWork(ctx context.Context,
	environment keyvalue.Versioned[hierarchy.EnvironmentRecord], project keyvalue.Versioned[hierarchy.ProjectRecord],
	routeID, name string, action workexecution.Action, locator idempotency.IdempotencyLocator,
	evidence routeMutationEvidence, input acceptedRouteMutation,
) (idempotency.IdempotencyResponse, error) {
	at := service.now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     action,
		TargetID:   routeID,
		TargetName: name,
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environment.Record.ID,
		},
		After: append(
			[]string(nil),
			input.After...),
		CreatedAt: at,
		UpdatedAt: at,
		Deadline:  at.Add(acceptedaction.QueueTimeout),
	}
	kind := "route-create"
	if action == workexecution.ActionUpdate {
		kind = "route-edit"
	} else if action == workexecution.ActionRemove {
		kind = "route-remove"
	}
	input.Locator, input.Intent = locator, evidence.durable
	return acceptedaction.Submit(
		ctx,
		service.accepted,
		acceptedaction.Submission{
			Task:     task,
			Kind:     kind,
			Locator:  locator,
			Intent:   evidence.durable,
			Evidence: evidence.candidate,
			ReplayTarget: &idempotency.IdempotencyReplayTarget{
				Kind: idempotency.IdempotencyReplayTargetRoute,
				ID:   routeID,
			},
		},
		input,
	)
}

func (service *routeMutationService) prepareAcceptedRoute(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, input acceptedRouteMutation,
) (prepared taskdependencies.Prepared, failure error) {
	defer clear(input.Intent.Ciphertext)
	task := accepted.Task
	var record routes.Record
	var previous *keyvalue.Versioned[routes.Record]
	if task.Action == workexecution.ActionCreate {
		if input.Create == nil || input.Edit != nil {
			return prepared, errs.New(errs.KindValidationFailed, "Route creation input is missing")
		}
		request := input.Create
		var err error
		record, err = routes.NewRecord(
			task.Scope.EnvironmentID,
			core.Route{
				ID:              task.TargetID,
				Host:            request.Host,
				Path:            request.Path,
				Exposure:        request.Exposure,
				TargetServiceID: request.TargetServiceID,
				TargetPort:      request.TargetPort,
			},
		)
		if err != nil {
			return prepared, err
		}
	} else {
		current, err := service.repository.GetRoute(ctx, task.TargetID)
		if err != nil {
			return prepared, err
		}
		previous = &current
		record = current.Record
		if task.Action == workexecution.ActionUpdate {
			if input.Edit == nil || input.Create != nil {
				return prepared, errs.New(errs.KindValidationFailed, "Route edit input is missing")
			}
			replacement := record.Desired
			replacement.Exposure = input.Edit.Exposure
			record, err = routes.ReplaceDesired(record, replacement)
			if err != nil {
				return prepared, err
			}
		}
	}
	environment, project, target, err := service.routeHierarchy(
		ctx,
		record.EnvironmentID,
		record.Desired.TargetServiceID,
	)
	if err != nil {
		return prepared, err
	}
	if task.Scope != (workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID}) {
		return prepared, errs.New(errs.KindStateConflict, "Route ownership changed after this action was requested")
	}
	if task.Action != workexecution.ActionRemove &&
		!core.ServiceExposesTCPPort(target.Record.Desired.Expose, record.Desired.TargetPort) {
		return prepared, errs.New(
			errs.KindValidationFailed,
			"Route target Service does not expose the requested TCP port",
		)
	}
	current, found, err := service.repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return prepared, err
	}
	if !found {
		return prepared, errs.New(
			errs.KindStateConflict,
			"Route requires the Environment's authored Service definition",
		)
	}
	staged, err := service.publisher.stage(
		ctx,
		routeWorkStageRequest{Current: current, Route: record, Action: task.Action, ReadRevision: current.ReadRevision,
			Locator: input.Locator, Evidence: routeMutationEvidence{durable: input.Intent}, CreatedAt: task.CreatedAt, TaskID: task.ID, AcceptedTaskRevision: accepted.Revision},
	)
	if err != nil {
		return prepared, err
	}
	finish := func(ctx context.Context, applied bool) error {
		if applied {
			return nil
		}
		err := service.publisher.desired.AbandonEnvironmentBlueprintStage(ctx, staged.Claim)
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindStateConflict {
			return nil
		}
		return err
	}
	prepared.Finalize = finish
	defer func() {
		if failure != nil {
			bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if cleanup := finish(bounded, false); cleanup != nil {
				failure = errs.WrapJoined(errs.KindInternal, failure, cleanup)
			}
		}
	}()
	return service.publisher.prepare(
		ctx,
		routeWorkRequest{
			Project:       project,
			Environment:   environment,
			Target:        target,
			Current:       previous,
			Route:         staged.Route,
			Provider:      staged.Provider,
			Claim:         staged.Claim,
			Candidate:     staged.Candidate,
			EpochRevision: staged.EpochRevision,
			Action:        task.Action,
			Accepted:      accepted,
			At:            at,
		},
		finish,
	)
}
