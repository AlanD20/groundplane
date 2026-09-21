package etcd

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func decodeAcknowledgementAssignment(
	task TaskRecord, assignmentValue, assignmentIndexValue *etcdstore.KeyValue,
	executor taskjournal.TaskExecutor,
	agentID string, agentGeneration uint64, assignmentID string,
) (taskassignments.TaskAssignmentRecord, error) {
	assignment, err := taskassignments.DecodeTaskAssignment(assignmentValue.Value)
	if err != nil {
		return taskassignments.TaskAssignmentRecord{}, err
	}
	if assignmentIndexValue == nil || assignmentIndexValue.ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(assignmentIndexValue.Value, assignmentValue.Value) {
		return taskassignments.TaskAssignmentRecord{}, errs.New(
			errs.KindInternal,
			"task assignment index does not match assignment",
		)
	}
	if assignment.TaskID != task.ID || assignment.Executor != executor ||
		(executor == taskjournal.TaskExecutorAgent && assignment.AssignmentID != assignmentID) ||
		assignment.AgentID != agentID ||
		assignment.AgentGeneration != agentGeneration ||
		assignment.ClaimedTaskRevision >= assignmentValue.ModRevision || task.StartedAt == nil ||
		!assignment.AssignedAt.Equal(*task.StartedAt) {
		return taskassignments.TaskAssignmentRecord{}, errs.New(
			errs.KindStateConflict,
			"task assignment does not match the Agent generation",
		)
	}
	return assignment, nil
}

func validateTaskAcknowledgement(
	ctx context.Context, executor taskjournal.TaskExecutor,
	agentID string, agentGeneration uint64, taskID, assignmentID string,
	terminalStatus taskjournal.TaskStatus, result *taskjournal.TaskResultRecord,
	terminalAt time.Time, environmentID string,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if !taskjournal.ValidExecutor(executor) || recordcodec.ValidateID(ids.KindTask, taskID) != nil ||
		!taskjournal.IsTerminalTaskStatus(terminalStatus) ||
		(executor == taskjournal.TaskExecutorAgent && (recordcodec.ValidateID(ids.KindAgent, agentID) != nil || agentGeneration == 0 ||
			recordcodec.ValidateID(ids.KindAssignment, assignmentID) != nil || result == nil)) ||
		(executor == taskjournal.TaskExecutorController &&
			(agentID != "" || agentGeneration != 0 || assignmentID != "" || result != nil)) {
		return errs.New(errs.KindValidationFailed, "task acknowledgement is invalid")
	}
	if err := recordcodec.ValidateTimestamp("task terminal_at", terminalAt); err != nil {
		return err
	}
	if environmentID != "" && recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil {
		return errs.New(
			errs.KindValidationFailed,
			"environment creation acknowledgement is invalid",
		)
	}

	return nil
}
