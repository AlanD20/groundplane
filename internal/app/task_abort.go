package app

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const taskAbortReasonOperatorRequested = "operator_requested"

type taskAbortRepository interface {
	GetTask(context.Context, string) (etcd.Versioned[etcd.TaskRecord], error)
	GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
	AbortPendingTask(context.Context, string, time.Time) (etcd.Versioned[etcd.TaskRecord], error)
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

func newTaskAbortService(
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
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Task abort context is required")
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Task id is invalid")
	}
	for attempt := 0; attempt < 8; attempt++ {
		current, err := service.repository.GetTask(ctx, taskID)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		switch current.Record.Status {
		case etcd.TaskStatusAborted:
			return taskAbortResponse(taskID)
		case etcd.TaskStatusCompleted, etcd.TaskStatusFailed, etcd.TaskStatusTimedOut:
			return etcd.IdempotencyResponse{}, errs.Newf(
				errs.KindTaskNotAbortable,
				"task %s has status %s",
				taskID,
				current.Record.Status,
			)
		case etcd.TaskStatusPending:
			if _, err := service.repository.AbortPendingTask(ctx, taskID, service.now().UTC()); err != nil {
				if isTaskAbortStateRace(err) {
					continue
				}
				return etcd.IdempotencyResponse{}, err
			}
		case etcd.TaskStatusRunning:
			assignment, err := service.repository.GetTaskAssignment(ctx, taskID)
			if err != nil {
				if isTaskAbortStateRace(err) {
					continue
				}
				return etcd.IdempotencyResponse{}, err
			}
			switch assignment.Assignment.Record.Executor {
			case etcd.TaskExecutorAgent:
				err = service.abortAgentTask(ctx, assignment)
			case etcd.TaskExecutorController:
				err = service.controller.AbortTask(ctx, taskID)
			default:
				err = errs.New(errs.KindInternal, "Task abort executor is invalid")
			}
			if err != nil {
				if isTaskAbortStateRace(err) {
					continue
				}
				return etcd.IdempotencyResponse{}, err
			}
		default:
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Task abort status is invalid")
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Task changed repeatedly during abort")
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
	if current.Assignment.Record != record {
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

func taskAbortResponse(taskID string) (etcd.IdempotencyResponse, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
	}, nil
}

func isTaskAbortStateRace(err error) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStateConflict
}
