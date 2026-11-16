package agentmanagement

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const agentRemovalRoute = "/agents/{id}"

type agentRemovalTargets interface {
	Health(context.Context, string) (localagent.Health, error)
}
type agentRemovalService struct {
	targets  agentRemovalTargets
	tasks    agentCaptureRepository
	accepted *acceptedaction.Submitter
	now      func() time.Time
}

func NewRemovalService(
	targets agentRemovalTargets,
	tasks agentCaptureRepository,
	accepted *acceptedaction.Submitter,
) (*agentRemovalService, error) {
	if targets == nil || tasks == nil || accepted == nil {
		return nil, errs.New(errs.KindInternal, "Agent removal service dependencies are invalid")
	}
	return &agentRemovalService{targets: targets, tasks: tasks, accepted: accepted, now: time.Now}, nil
}

func (service *agentRemovalService) RemoveAgent(
	ctx context.Context,
	agentID string,
	input api.TaskRunAfterRequest,
	key string,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindAgent, agentID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Agent id is invalid")
	}
	intent, err := beginAgentAction(
		ctx,
		service.accepted,
		http.MethodDelete,
		agentRemovalRoute,
		agentID,
		requestidempotency.Object(
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)},
		),
		key,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer intent.Destroy()
	if response, found := intent.Replay(); found {
		return response, nil
	}
	task := newAcceptedAgentTask(service.now().UTC(), agentID, workexecution.ActionRemove, input.After)
	return acceptedaction.Submit(ctx, service.accepted, intent.Submission(task, "agent-remove"), input)
}
func (service *agentRemovalService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.accepted,
			"agent-remove",
			workexecution.ActionRemove,
			ids.KindAgent,
			service.prepareAcceptedRemoval,
		),
	}
}

func (service *agentRemovalService) prepareAcceptedRemoval(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	_ api.TaskRunAfterRequest,
) (taskdependencies.Prepared, error) {
	health, err := service.targets.Health(ctx, task.Task.TargetID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if health.Agent.ID != task.Task.TargetID {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindInternal,
			"Agent removal target lookup returned another Agent",
		)
	}
	capture := agentoperations.Capture{
		TaskID:     task.Task.ID,
		WorkItemID: ids.DeriveAt(ids.KindWorkItem, task.Task.CreatedAt, task.Task.ID, "agent-work"),
		PlanID: ids.DeriveAt(
			ids.KindPlan,
			task.Task.CreatedAt,
			task.Task.ID,
			"agent-input",
		),
		AgentID:            task.Task.TargetID,
		Action:             workexecution.ActionRemove,
		Image:              health.Agent.Image,
		StartingGeneration: health.Agent.Generation,
		CreatedAt:          at,
	}
	return prepareAgentCapture(ctx, service.tasks, task, capture, at)
}
