package releaseoperation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func acceptReleaseRequest[T any](
	ctx context.Context,
	service *Service,
	environmentID, targetID, targetName string,
	action workexecution.Action,
	kind string,
	after []string,
	input T,
	locator idempotency.IdempotencyLocator,
	durable idempotency.ProtectedIntentRecord,
	protected requestidempotency.ProtectedEvidence,
	guards ...keyvalue.Condition,
) (idempotency.IdempotencyResponse, error) {
	// Only ancestor identity is needed at acceptance. No image, predecessor,
	// configuration baseline, proxy address or runtime claim is selected here.
	scope, err := service.ledger.LoadTaskAcceptanceScope(ctx, environmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	return publishReleaseRequest(ctx, service, scope, targetID, targetName, action, kind, after, input,
		locator, durable, protected, nil, guards...)
}

func publishReleaseRequest[T any](
	ctx context.Context,
	service *Service,
	scope workexecution.Scope,
	targetID, targetName string,
	action workexecution.Action,
	kind string,
	after []string,
	input T,
	locator idempotency.IdempotencyLocator,
	durable idempotency.ProtectedIntentRecord,
	protected requestidempotency.ProtectedEvidence,
	publicationForTask func(workexecution.Task) (workqueue.Change, error),
	guards ...keyvalue.Condition,
) (idempotency.IdempotencyResponse, error) {
	at := service.now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Scope:  scope,
		Action: action, TargetID: targetID, TargetName: targetName, After: append([]string(nil), after...),
		CreatedAt: at, UpdatedAt: at, Deadline: at.Add(service.timeout)}
	return acceptedaction.Submit(
		ctx,
		service.acceptedActions,
		acceptedaction.Submission{
			Task:               task,
			Kind:               kind,
			Guards:             guards,
			Locator:            locator,
			Intent:             durable,
			Evidence:           protected,
			PublicationForTask: publicationForTask,
		},
		input,
	)
}

func releaseAfterValue(after []string) requestidempotency.Value {
	values := make([]requestidempotency.Value, len(after))
	for index, id := range after {
		values[index] = requestidempotency.String(id)
	}
	return requestidempotency.List(values...)
}

func (service *Service) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.acceptedActions,
			"service-deploy",
			workexecution.ActionDeploy,
			ids.KindService,
			service.prepareAcceptedServiceDeploy,
		),
		acceptedaction.Registration(
			service.acceptedActions,
			"service-rollback",
			workexecution.ActionRollback,
			ids.KindService,
			service.prepareAcceptedServiceRollback,
		),
		acceptedaction.Registration(
			service.acceptedActions,
			"group-deploy",
			workexecution.ActionDeploy,
			ids.KindReleaseGroup,
			service.prepareAcceptedGroupDeploy,
		),
		acceptedaction.Registration(
			service.acceptedActions,
			"group-rollback",
			workexecution.ActionRollback,
			ids.KindReleaseGroup,
			service.prepareAcceptedGroupRollback,
		),
	}
}

func (service *Service) prepareAcceptedServiceDeploy(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	_ int64,
	_ time.Time,
	request acceptedServiceDeploy,
) (taskdependencies.Prepared, error) {
	ctx = imagefence.WithScope(ctx)
	scope, err := service.ledger.LoadPlanningScope(ctx, task.Task.Scope.EnvironmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	// Native promotion compares this same planning snapshot through its source
	// guards, so a rewrite after this check cannot publish a Release graph.
	if request.Source != nil && !request.Source.matches(task.Task.Scope, scope) {
		return taskdependencies.Prepared{}, staleDeploySource()
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, []string{task.Task.TargetID})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if request.HostID != "" {
		if service.remotePlacement == nil || request.Source != nil {
			return taskdependencies.Prepared{}, remotePlacementUnavailable()
		}
		if request.RemoteDesiredRevision <= 0 || planning[0].Service.Revision != request.RemoteDesiredRevision {
			return taskdependencies.Prepared{}, staleDeploySource()
		}
	}
	candidate, err := service.deployCandidate(
		ctx,
		scope,
		planning[0],
		request.Tag,
		string(request.Strategy),
		request.OnFailure,
		request.Image,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if request.HostID != "" {
		return service.prepareRemoteServiceWork(ctx, scope, candidate, task.Task, request.HostID)
	}
	return service.prepareServiceWork(ctx, scope, candidate, task.Task)
}

func (service *Service) prepareAcceptedServiceRollback(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	_ int64,
	_ time.Time,
	request domain.ServiceRollbackInput,
) (taskdependencies.Prepared, error) {
	ctx = imagefence.WithScope(ctx)
	scope, err := service.ledger.LoadPlanningScope(ctx, task.Task.Scope.EnvironmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, []string{task.Task.TargetID})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	candidate, err := service.rollbackCandidate(ctx, scope, planning[0], request.Tag, "")
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return service.prepareServiceWork(ctx, scope, candidate, task.Task)
}
