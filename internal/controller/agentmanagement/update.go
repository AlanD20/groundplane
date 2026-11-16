package agentmanagement

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
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

const agentUpdateRoute = "/agents/{id}/update"

type agentUpdateTargets interface {
	Health(context.Context, string) (localagent.Health, error)
}
type agentImageVerifier interface {
	Verify(context.Context, string, int) error
}
type agentUpdateService struct {
	targets  agentUpdateTargets
	tasks    agentCaptureRepository
	images   agentImageVerifier
	accepted *acceptedaction.Submitter
	now      func() time.Time
}

func NewUpdateService(
	targets agentUpdateTargets,
	tasks agentCaptureRepository,
	images agentImageVerifier,
	accepted *acceptedaction.Submitter,
) (*agentUpdateService, error) {
	if targets == nil || tasks == nil || images == nil || accepted == nil {
		return nil, errs.New(errs.KindInternal, "Agent update service dependencies are invalid")
	}
	return &agentUpdateService{targets: targets, tasks: tasks, images: images, accepted: accepted, now: time.Now}, nil
}

func (service *agentUpdateService) UpdateAgent(
	ctx context.Context,
	agentID string,
	input api.AgentUpdate,
	key string,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindAgent, agentID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Agent id is invalid")
	}
	if !imageref.IsDigestPinned(input.Image) {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Agent update requires a digest-pinned image",
		)
	}
	body := requestidempotency.Object(
		requestidempotency.Field{Name: "image", Value: requestidempotency.String(input.Image)},
		requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)},
	)
	intent, err := beginAgentAction(ctx, service.accepted, http.MethodPost, agentUpdateRoute, agentID, body, key)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer intent.Destroy()
	if response, found := intent.Replay(); found {
		return response, nil
	}
	task := newAcceptedAgentTask(service.now().UTC(), agentID, workexecution.ActionUpdate, input.After)
	return acceptedaction.Submit(ctx, service.accepted, intent.Submission(task, "agent-update"), input)
}
func (service *agentUpdateService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.accepted,
			"agent-update",
			workexecution.ActionUpdate,
			ids.KindAgent,
			service.prepareAcceptedUpdate,
		),
	}
}

func (service *agentUpdateService) prepareAcceptedUpdate(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	input api.AgentUpdate,
) (taskdependencies.Prepared, error) {
	health, err := service.targets.Health(ctx, task.Task.TargetID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if health.Agent.ID != task.Task.TargetID {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindInternal,
			"Agent update target lookup returned another Agent",
		)
	}
	if health.Agent.Phase != localagent.PhaseReady {
		return taskdependencies.Prepared{}, errs.New(errs.KindResourceInUse, "Agent is not ready for update")
	}
	if health.Agent.Image == input.Image {
		return taskdependencies.Prepared{}, errs.New(errs.KindStateConflict, "Agent already runs the selected image")
	}
	if err := service.images.Verify(ctx, input.Image, agentprotocol.SchemaVersion); err != nil {
		return taskdependencies.Prepared{}, err
	}
	if health.Agent.Generation == 0 || health.Agent.Generation > ^uint64(0)-2 {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Agent generation has no replacement and recovery capacity",
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
		Action:             workexecution.ActionUpdate,
		Image:              input.Image,
		PreviousImage:      health.Agent.Image,
		StartingGeneration: health.Agent.Generation,
		CreatedAt:          at,
	}
	return prepareAgentCapture(ctx, service.tasks, task, capture, at)
}
