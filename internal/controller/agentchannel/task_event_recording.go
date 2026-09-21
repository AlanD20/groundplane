package agentchannel

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) recordTaskEvent(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	event *agentpb.TaskEvent,
) error {
	if event == nil || ids.Validate(ids.KindAssignment, event.AssignmentId) != nil ||
		len(event.PlanHash) != 32 || event.ExecutionEpoch == 0 || event.Ordinal == 0 {
		return errs.New(errs.KindValidationFailed, "Agent Task event is invalid")
	}
	if len(event.Chunk) != 0 {
		return status.Error(
			codes.Unimplemented,
			"ephemeral Task output streaming is not implemented",
		)
	}
	task, err := s.tasks.GetTask(ctx, event.TaskId)
	if err != nil {
		return err
	}
	if assignments, ok := s.tasks.(interface {
		GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
	}); ok {
		assignment, assignmentErr := assignments.GetTaskAssignment(ctx, event.GetTaskId())
		if assignmentErr != nil || assignment.Assignment.Record.AssignmentID != event.GetAssignmentId() ||
			assignment.Assignment.Record.ExecutionEpoch != event.GetExecutionEpoch() {
			return errs.New(errs.KindStateConflict, "Agent Task event execution epoch does not match")
		}
	}
	planHash, err := hex.DecodeString(task.Record.PlanHash)
	if err != nil || !bytes.Equal(planHash, event.PlanHash) {
		return errs.New(errs.KindStateConflict, "Agent Task event plan hash does not match")
	}
	if task.Record.Status != taskjournal.TaskStatusRunning {
		return errs.New(
			errs.KindStateConflict,
			"Agent Task event does not belong to a running Task",
		)
	}
	var state taskjournal.TaskEventState
	switch event.State {
	case agentpb.TaskState_TASK_STATE_PENDING:
		state = taskjournal.TaskEventStatePending
	case agentpb.TaskState_TASK_STATE_RUNNING:
		state = taskjournal.TaskEventStateRunning
	case agentpb.TaskState_TASK_STATE_COMPLETED:
		state = taskjournal.TaskEventStateCompleted
	case agentpb.TaskState_TASK_STATE_FAILED:
		state = taskjournal.TaskEventStateFailed
	case agentpb.TaskState_TASK_STATE_ABORTED:
		state = taskjournal.TaskEventStateAborted
	case agentpb.TaskState_TASK_STATE_TIMED_OUT:
		state = taskjournal.TaskEventStateTimedOut
	default:
		return errs.New(errs.KindValidationFailed, "Agent Task event state is invalid")
	}
	_, err = s.tasks.AppendTaskEvent(ctx, etcd.TaskEventInput{
		Identity: etcd.TaskEventIdentity{
			AssignmentID: event.AssignmentId, AgentID: agentID, AgentGeneration: agentGeneration,
			TaskID: event.TaskId, StepID: event.StepId,
			Attempt: event.ExecutionEpoch, Ordinal: event.Ordinal,
		},
		State:   state,
		Payload: json.RawMessage(`{}`),
	}, s.now().UTC())
	return err
}
