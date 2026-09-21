package localagent

import (
	"context"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const updateRecoveryReserve = 130 * time.Second

type UpdateLifecycle interface {
	RecoverUpdate(context.Context, UpdateRequest, UpdateGoal) (UpdateResult, error)
	Health(context.Context, string) (Health, error)
}

type UpdateAdmission interface {
	PauseAssignments(context.Context, string, uint64) (func(), error)
}

// TaskUpdates owns the lifetime of the durable standalone Agent update claim.
// Its all-generation hold outlives individual failed recovery attempts. The
// next Controller restores it from that claim before accepting Agent sessions.
type TaskUpdates struct {
	agents   UpdateLifecycle
	sessions UpdateAdmission
	logger   *slog.Logger
	now      func() time.Time
	mu       sync.Mutex
	active   *agentUpdateOperation
}

type agentUpdateOperation struct {
	taskID   string
	request  UpdateRequest
	deadline time.Time
	resume   func()
	cancel   context.CancelFunc
	aborted  bool
	restore  bool
	settled  taskjournal.TaskStatus
}

func NewTaskUpdates(agents UpdateLifecycle, sessions UpdateAdmission, logger *slog.Logger) (*TaskUpdates, error) {
	if agents == nil || sessions == nil || logger == nil {
		return nil, errs.New(errs.KindInternal, "agent update Task recovery is not configured")
	}
	return &TaskUpdates{agents: agents, sessions: sessions, logger: logger, now: time.Now}, nil
}

func (updates *TaskUpdates) Restore(ctx context.Context, claim etcd.TaskAssignment) error {
	request, err := DecodeUpdateTask(claim.Task.Record)
	if err != nil {
		return err
	}
	assignment := claim.Assignment.Record
	if assignment.Executor != taskjournal.TaskExecutorController || assignment.TaskID != claim.Task.Record.ID ||
		assignment.Deadline.IsZero() {
		return errs.New(errs.KindValidationFailed, "agent update Task claim is invalid")
	}
	updates.mu.Lock()
	defer updates.mu.Unlock()
	if previous := updates.active; previous != nil {
		if previous.taskID == claim.Task.Record.ID {
			if previous.request != request || !previous.deadline.Equal(assignment.Deadline) {
				return errs.New(errs.KindStateConflict, "agent update Task recovery identity changed")
			}
			return nil
		}
		if previous.settled == "" {
			return errs.New(errs.KindResourceInUse, "another agent update Task still owns recovery")
		}
	}
	drainContext, cancel := context.WithTimeout(ctx, updateRecoveryReserve)
	defer cancel()
	resume, err := updates.sessions.PauseAssignments(drainContext, request.AgentID, math.MaxUint64)
	if err != nil {
		return err
	}
	updates.active = &agentUpdateOperation{
		taskID: claim.Task.Record.ID, request: request, deadline: assignment.Deadline, resume: resume,
	}
	return nil
}

func (updates *TaskUpdates) Execute(
	ctx context.Context, task etcd.TaskRecord, deadline time.Time,
) (taskjournal.TaskStatus, error) {
	updates.mu.Lock()
	operation := updates.active
	if operation == nil || operation.taskID != task.ID || !operation.deadline.Equal(deadline) {
		updates.mu.Unlock()
		return "", errs.New(errs.KindStateConflict, "agent update Task admission is not restored")
	}
	if operation.settled != "" {
		status := operation.settled
		updates.mu.Unlock()
		return status, nil
	}
	restore := operation.restore || operation.aborted || !updates.now().Before(deadline.Add(-updateRecoveryReserve))
	updates.mu.Unlock()
	// A committed Ready candidate is a replayable successful effect. Do not
	// roll it back merely because its final Task acknowledgement was lost.
	if restore && !updates.wasAborted(operation) {
		health, err := updates.agents.Health(ctx, operation.request.AgentID)
		if err != nil {
			return "", err
		}
		if health.Agent.Phase == PhaseReady && health.Agent.Generation == operation.request.StartingGeneration+1 &&
			health.Agent.Image == operation.request.DesiredImage {
			return updates.recoverReadyCandidate(ctx, operation)
		}
	}
	if !restore {
		candidateContext, cancel := context.WithTimeout(ctx, deadline.Add(-updateRecoveryReserve).Sub(updates.now()))
		updates.mu.Lock()
		operation.cancel = cancel
		if operation.aborted {
			cancel()
		}
		updates.mu.Unlock()
		result, err := updates.agents.RecoverUpdate(candidateContext, operation.request, UpdateFinish)
		cancel()
		updates.mu.Lock()
		operation.cancel = nil
		aborted := operation.aborted
		updates.mu.Unlock()
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if err == nil && !aborted {
			if result == UpdateReadyDesired {
				return updates.settle(operation, taskjournal.TaskStatusCompleted)
			}
			if result == UpdateReadyPrevious {
				return updates.settle(operation, taskjournal.TaskStatusFailed)
			}
			return "", errs.New(errs.KindInternal, "agent update returned an invalid recovery result")
		}
		if err != nil {
			updates.logger.Error("agent update requires predecessor recovery", "task_id", task.ID, "error", err)
		}
	}
	updates.mu.Lock()
	operation.restore = true
	updates.mu.Unlock()
	// Even an expired Task gets a bounded recovery pass; failure retains its
	// claim and admission hold for the next pass or Controller process.
	recoveryContext, cancel := context.WithTimeout(ctx, updateRecoveryReserve)
	defer cancel()
	result, err := updates.agents.RecoverUpdate(recoveryContext, operation.request, UpdateRestore)
	if err != nil {
		return "", err
	}
	if result != UpdateReadyPrevious {
		return "", errs.New(errs.KindInternal, "agent predecessor recovery returned another image")
	}
	status := taskjournal.TaskStatusFailed
	if updates.wasAborted(operation) {
		status = taskjournal.TaskStatusAborted
	} else if !updates.now().Before(deadline) {
		status = taskjournal.TaskStatusTimedOut
	}
	return updates.settle(operation, status)
}

func (updates *TaskUpdates) recoverReadyCandidate(
	ctx context.Context, operation *agentUpdateOperation,
) (taskjournal.TaskStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, updateRecoveryReserve)
	defer cancel()
	result, err := updates.agents.RecoverUpdate(ctx, operation.request, UpdateFinish)
	if err != nil {
		return "", err
	}
	if result != UpdateReadyDesired {
		return "", errs.New(errs.KindStateConflict, "agent ready update replay changed")
	}
	return updates.settle(operation, taskjournal.TaskStatusCompleted)
}

func (updates *TaskUpdates) Abort(ctx context.Context, taskID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	updates.mu.Lock()
	defer updates.mu.Unlock()
	operation := updates.active
	if operation == nil || operation.taskID != taskID || operation.settled != "" {
		return errs.New(errs.KindStateConflict, "agent update Task is not recovering")
	}
	operation.aborted = true
	if operation.cancel != nil {
		operation.cancel()
	}
	return nil
}

func (updates *TaskUpdates) wasAborted(operation *agentUpdateOperation) bool {
	updates.mu.Lock()
	defer updates.mu.Unlock()
	return operation.aborted
}

func (updates *TaskUpdates) settle(
	operation *agentUpdateOperation, status taskjournal.TaskStatus,
) (taskjournal.TaskStatus, error) {
	updates.mu.Lock()
	defer updates.mu.Unlock()
	if operation.aborted {
		if status == taskjournal.TaskStatusCompleted {
			return "", errs.New(errs.KindStateConflict, "agent update abort won before qualification")
		}
		status = taskjournal.TaskStatusAborted
	}
	operation.settled = status
	operation.resume()
	return status, nil
}

// DecodeUpdateTask parses the frozen native Task, never current mutable
// configuration. This same grammar owns execution and cold-start recovery.
func DecodeUpdateTask(task etcd.TaskRecord) (UpdateRequest, error) {
	if task.Executor != taskjournal.TaskExecutorController || task.Type != taskjournal.TaskUpdate ||
		ids.Validate(ids.KindTask, task.ID) != nil || len(task.Params) != 4 ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceAgent {
		return UpdateRequest{}, invalidUpdateTask()
	}
	previous, desired := task.Params["previous_image"], task.Params["image"]
	text := task.Params["starting_generation"]
	generation, err := strconv.ParseUint(text, 10, 64)
	if err != nil || generation == 0 || generation > math.MaxUint64-2 ||
		strconv.FormatUint(generation, 10) != text || ids.Validate(ids.KindAgent, task.Target) != nil ||
		!imageref.IsDigestPinned(previous) || !imageref.IsDigestPinned(desired) || previous == desired {
		return UpdateRequest{}, invalidUpdateTask()
	}
	return UpdateRequest{AgentID: task.Target, PreviousImage: previous, DesiredImage: desired,
		StartingGeneration: generation}, nil
}

func invalidUpdateTask() error {
	return errs.New(errs.KindValidationFailed, "agent update Task parameters are invalid")
}
