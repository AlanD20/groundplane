package tasks

import (
	"context"
	"encoding/json"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const taskAbortReasonOperatorRequested = "operator_requested"

type taskAbortRepository interface {
	GetTask(context.Context, string) (etcdstore.Versioned[etcd.TaskRecord], error)
	GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
	AbortPendingTask(context.Context, string, time.Time) (etcdstore.Versioned[etcd.TaskRecord], error)
}

type taskAbortAgentChannel interface {
	TaskTerminal(context.Context, string, uint64, string, string) (<-chan error, error)
	AbortTask(context.Context, string, uint64, string, string, string) error
}

type taskAbortControllerRunner interface {
	AbortTask(context.Context, string) error
}

type taskAbortService struct {
	repository taskAbortRepository
	agents     taskAbortAgentChannel
	controller taskAbortControllerRunner
	now        func() time.Time
}

func NewAbortService(
	repository taskAbortRepository,
	agents taskAbortAgentChannel,
	controller taskAbortControllerRunner,
) (*taskAbortService, error) {
	if repository == nil || agents == nil || controller == nil {
		return nil, errs.New(errs.KindInternal, "Task abort service is not configured")
	}
	return &taskAbortService{
		repository: repository, agents: agents, controller: controller, now: time.Now,
	}, nil
}

func (service *taskAbortService) AbortTask(
	ctx context.Context,
	taskID string,
	_ string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Task abort context is required")
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Task id is invalid")
	}
	for attempt := 0; attempt < 8; attempt++ {
		current, err := service.repository.GetTask(ctx, taskID)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		if current.Record.Type == taskjournal.TaskBackupPrune {
			return idempotencyrecord.IdempotencyResponse{}, errs.Newf(
				errs.KindTaskNotAbortable,
				"internal task %s of type %s is not operator-abortable",
				taskID,
				current.Record.Type,
			)
		}
		switch current.Record.Status {
		case taskjournal.TaskStatusAborted:
			if current.Record.StartedAt == nil && current.Record.Params[releaserender.TaskReleasePublicationParam] != "" &&
				(current.Record.Type == taskjournal.TaskDeploy || current.Record.Type == taskjournal.TaskRollback) {
				if _, err := service.repository.AbortPendingTask(ctx, taskID, service.now().UTC()); err != nil {
					return idempotencyrecord.IdempotencyResponse{}, err
				}
			}
			return taskAbortResponse(taskID)
		case taskjournal.TaskStatusCompleted, taskjournal.TaskStatusFailed, taskjournal.TaskStatusTimedOut:
			return idempotencyrecord.IdempotencyResponse{}, errs.Newf(
				errs.KindTaskNotAbortable,
				"task %s has status %s",
				taskID,
				current.Record.Status,
			)
		case taskjournal.TaskStatusPending:
			if _, err := service.repository.AbortPendingTask(ctx, taskID, service.now().UTC()); err != nil {
				if isTaskAbortStateRace(err) {
					continue
				}
				return idempotencyrecord.IdempotencyResponse{}, err
			}
		case taskjournal.TaskStatusRunning:
			assignment, err := service.repository.GetTaskAssignment(ctx, taskID)
			if err != nil {
				if isTaskAbortStateRace(err) {
					continue
				}
				return idempotencyrecord.IdempotencyResponse{}, err
			}
			switch assignment.Assignment.Record.Executor {
			case taskjournal.TaskExecutorAgent:
				err = service.abortAgentTask(ctx, assignment)
			case taskjournal.TaskExecutorController:
				err = service.controller.AbortTask(ctx, taskID)
			default:
				err = errs.New(errs.KindInternal, "Task abort executor is invalid")
			}
			if err != nil {
				if isTaskAbortStateRace(err) {
					continue
				}
				return idempotencyrecord.IdempotencyResponse{}, err
			}
		default:
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Task abort status is invalid")
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Task changed repeatedly during abort")
}

func (service *taskAbortService) abortAgentTask(ctx context.Context, assignment etcd.TaskAssignment) error {
	record := assignment.Assignment.Record
	subscriptionContext, cancel := context.WithCancel(ctx)
	defer cancel()
	terminal, err := service.agents.TaskTerminal(
		subscriptionContext,
		record.AgentID,
		record.AgentGeneration,
		record.TaskID,
		record.AssignmentID,
	)
	if err != nil {
		return err
	}
	if terminal == nil {
		return errs.New(errs.KindInternal, "Agent Task abort terminal subscription is nil")
	}
	current, err := service.repository.GetTaskAssignment(ctx, record.TaskID)
	if err != nil {
		return err
	}
	if current.Assignment.Revision != assignment.Assignment.Revision {
		return errs.New(errs.KindStateConflict, "Agent Task assignment changed before abort delivery")
	}
	if err := service.agents.AbortTask(
		ctx,
		record.AgentID,
		record.AgentGeneration,
		record.TaskID,
		record.AssignmentID,
		taskAbortReasonOperatorRequested,
	); err != nil {
		return err
	}
	select {
	case terminalErr, open := <-terminal:
		if !open {
			return errs.New(errs.KindInternal, "Agent Task terminal subscription closed without a result")
		}
		return terminalErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func taskAbortResponse(taskID string) (idempotencyrecord.IdempotencyResponse, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
	}, nil
}

func isTaskAbortStateRace(err error) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStateConflict
}
