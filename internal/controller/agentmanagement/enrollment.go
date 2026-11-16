package agentmanagement

import (
	"context"
	"maps"
	"net/http"
	"time"

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

const agentEnrollmentRoute = "/agents"

type agentCaptureRepository interface {
	PrepareAgentWorkAuthority(context.Context, agentoperations.Capture) (workqueue.Change, error)
}
type agentImageSource interface {
	DesiredAgentImage(context.Context) (string, error)
}
type agentEnrollmentService struct {
	images   agentImageSource
	config   localagent.Config
	tasks    agentCaptureRepository
	accepted *acceptedaction.Submitter
	now      func() time.Time
}
type agentEnrollmentRequest struct {
	Config localagent.Config `json:"config"`
}

func NewEnrollmentService(
	images agentImageSource,
	config localagent.Config,
	tasks agentCaptureRepository,
	accepted *acceptedaction.Submitter,
) (*agentEnrollmentService, error) {
	if images == nil || tasks == nil || accepted == nil || config.PullIntervalSeconds <= 0 ||
		config.MaxConcurrentTasks < 1 || config.MaxConcurrentTasks > 256 || config.MaxParallelOperationsPerTask < 1 || config.MaxParallelOperationsPerTask > 256 ||
		config.MaxConcurrentTasks*config.MaxParallelOperationsPerTask > 256 {
		return nil, errs.New(errs.KindInternal, "Agent enrollment service dependencies are invalid")
	}
	config.Labels = maps.Clone(config.Labels)
	return &agentEnrollmentService{images: images, config: config, tasks: tasks, accepted: accepted, now: time.Now}, nil
}

func (service *agentEnrollmentService) EnrollAgent(
	ctx context.Context,
	input api.TaskRunAfterRequest,
	key string,
) (idempotency.IdempotencyResponse, error) {
	intent, err := beginAgentAction(
		ctx,
		service.accepted,
		http.MethodPost,
		agentEnrollmentRoute,
		"",
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
	at := service.now().UTC()
	task := newAcceptedAgentTask(at, ids.New(ids.KindAgent), workexecution.ActionCreate, input.After)
	return acceptedaction.Submit(
		ctx,
		service.accepted,
		intent.Submission(task, "agent-enroll"),
		agentEnrollmentRequest{Config: service.config},
	)
}
func (service *agentEnrollmentService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.accepted,
			"agent-enroll",
			workexecution.ActionCreate,
			ids.KindAgent,
			service.prepareAcceptedEnrollment,
		),
	}
}

func (service *agentEnrollmentService) prepareAcceptedEnrollment(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	input agentEnrollmentRequest,
) (taskdependencies.Prepared, error) {
	image, err := service.images.DesiredAgentImage(ctx)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if !imageref.IsDigestPinned(image) {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindValidationFailed,
			"Agent enrollment requires a selected digest-pinned image",
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
		AgentID:   task.Task.TargetID,
		Action:    workexecution.ActionCreate,
		Image:     image,
		CreatedAt: at,
		Enrollment: &agentoperations.Enrollment{
			PullIntervalSeconds: input.Config.PullIntervalSeconds, MaxConcurrentTasks: input.Config.MaxConcurrentTasks,
			MaxParallelOperationsPerTask: input.Config.MaxParallelOperationsPerTask, Labels: maps.Clone(input.Config.Labels)},
	}
	return prepareAgentCapture(ctx, service.tasks, task, capture, at)
}
