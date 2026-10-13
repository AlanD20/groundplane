package agentchannel

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) recordTaskEvent(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	event *agentpb.TaskEvent,
) (*agentpb.TaskEventAck, error) {
	if event == nil || ids.Validate(ids.KindAssignment, event.AssignmentId) != nil ||
		len(event.PlanHash) != 32 || event.ExecutionEpoch == 0 || event.Ordinal == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Agent Task event is invalid")
	}
	if len(event.Chunk) != 0 {
		return nil, status.Error(
			codes.Unimplemented,
			"ephemeral Task output streaming is not implemented",
		)
	}
	task, err := s.tasks.GetTask(ctx, event.TaskId)
	if err != nil {
		return nil, err
	}
	if assignments, ok := s.tasks.(interface {
		GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
	}); ok {
		assignment, assignmentErr := assignments.GetTaskAssignment(ctx, event.GetTaskId())
		if assignmentErr != nil || assignment.Assignment.Record.AssignmentID != event.GetAssignmentId() ||
			assignment.Assignment.Record.ExecutionEpoch != event.GetExecutionEpoch() {
			return nil, errs.New(errs.KindStateConflict, "Agent Task event execution epoch does not match")
		}
	}
	planHash, err := hex.DecodeString(task.Record.PlanHash)
	if err != nil || !bytes.Equal(planHash, event.PlanHash) {
		return nil, errs.New(errs.KindStateConflict, "Agent Task event plan hash does not match")
	}
	if task.Record.Status != taskjournal.TaskStatusRunning {
		return nil, errs.New(
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
		return nil, errs.New(errs.KindValidationFailed, "Agent Task event state is invalid")
	}
	health, err := taskprogress.FromAgent(event.GetHealthWait())
	if err != nil {
		return nil, err
	}
	if health != nil && (state != taskjournal.TaskEventStateRunning || event.GetOrdinal() < 2) {
		return nil, errs.New(errs.KindValidationFailed, "health wait observation must belong to a running step")
	}
	failure, err := taskprogress.FailureFromAgent(event.GetFailure())
	if err != nil {
		return nil, err
	}
	if failure != nil &&
		(state != taskjournal.TaskEventStateFailed && state != taskjournal.TaskEventStateTimedOut && state != taskjournal.TaskEventStateAborted || health != nil) {
		return nil, errs.New(errs.KindValidationFailed, "failure diagnostic must belong to an unsuccessful step")
	}
	payload, err := json.Marshal(taskprogress.Payload{HealthWait: health, Failure: failure})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	appended, err := s.tasks.AppendTaskEvent(ctx, taskjournal.TaskEventInput{
		Identity: taskjournal.TaskEventIdentity{
			AssignmentID: event.AssignmentId, AgentID: agentID, AgentGeneration: agentGeneration,
			TaskID: event.TaskId, StepID: event.StepId,
			Attempt: event.ExecutionEpoch, Ordinal: event.Ordinal,
		},
		State:   state,
		Payload: payload,
	}, s.now().UTC())
	if err != nil {
		return nil, err
	}
	ack := &agentpb.TaskEventAck{
		TaskId: event.GetTaskId(), AssignmentId: event.GetAssignmentId(),
		PlanHash: append([]byte(nil), event.GetPlanHash()...), StepId: event.GetStepId(),
		ExecutionEpoch: event.GetExecutionEpoch(), Ordinal: event.GetOrdinal(), State: event.GetState(),
		EventOrdinalBase: appended.EventOrdinalBase,
	}
	switch appended.StartDecision {
	case taskjournal.StepStartAllowed:
		ack.StartDecision = agentpb.StepStartDecision_STEP_START_DECISION_ALLOWED
	case taskjournal.StepStartWait:
		ack.StartDecision = agentpb.StepStartDecision_STEP_START_DECISION_WAIT
	case taskjournal.StepStartStop:
		ack.StartDecision = agentpb.StepStartDecision_STEP_START_DECISION_STOP
	case taskjournal.StepStartNotRequested:
		if event.GetState() == agentpb.TaskState_TASK_STATE_RUNNING && event.GetOrdinal() == 1 {
			return nil, errs.New(errs.KindInternal, "Task start request has no execution decision")
		}
	default:
		return nil, errs.New(errs.KindInternal, "Task start execution decision is invalid")
	}
	if budget := appended.RollingBudget; budget != nil {
		ack.RollingBudget = &agentpb.RollingStepBudget{Deadline: timestamppb.New(budget.Deadline)}
		if !budget.DrainUntil.IsZero() {
			ack.RollingBudget.DrainUntil = timestamppb.New(budget.DrainUntil)
		}
	}
	return ack, nil
}
