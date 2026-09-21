package etcd

import (
	"context"
	"errors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// TimeoutAgentAssignments terminalizes the complete configured assignment set
// for one exact Agent generation. Stale-Agent detection owns when this method
// is called; the repository owns the normal terminal transaction and treats a
// concurrent terminal acknowledgement as an already-resolved assignment.
func (repository *TaskRepository) TimeoutAgentAssignments(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	maximum int32,
	terminalAt time.Time,
) (int, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return 0, err
	}
	if err := recordcodec.ValidateTimestamp("stale Agent task terminal_at", terminalAt); err != nil {
		return 0, err
	}
	assignments, err := repository.ListAgentAssignments(ctx, agentID, agentGeneration, maximum)
	if err != nil {
		return 0, err
	}
	timedOut := 0
	for _, assignment := range assignments {
		record := assignment.Assignment.Record
		deadline := record.Deadline
		if record.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly {
			deadline = record.RecoveryDeadline
		}
		if terminalAt.Before(deadline) {
			continue
		}
		if record.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly {
			if assignment.RecoveryProofRequired {
				continue
			}
			processed, markErr := repository.markReleaseRecoveryProofRequired(
				ctx, record, assignment.Task.ReadRevision, terminalAt,
			)
			if markErr != nil {
				if errors.Is(markErr, errs.New(errs.KindStateConflict, "")) {
					continue
				}
				return timedOut, markErr
			}
			if processed {
				timedOut++
			}
			continue
		}
		result := taskjournal.TaskResultRecord{
			Kind: taskjournal.TaskResultCompose, Diagnostic: taskjournal.TaskResultDiagnosticNone,
			ReconciliationRequired: true, ExecutionEpoch: assignment.Assignment.Record.ExecutionEpoch,
			ReleaseRecoveryRecordSHA256: assignment.Assignment.Record.ReleaseRecoveryRecordSHA256,
		}
		if classified, candidate, classifyErr := repository.preparedTaskTimeoutResult(ctx, assignment); candidate {
			if classifyErr != nil {
				return timedOut, classifyErr
			}
			result = classified
		}
		_, err := repository.AcknowledgeTask(
			ctx,
			agentID,
			agentGeneration,
			assignment.Task.Record.ID,
			assignment.Assignment.Record.AssignmentID,
			taskjournal.TaskStatusTimedOut,
			result,
			terminalAt,
		)
		if err != nil {
			if errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				continue
			}
			return timedOut, err
		}
		timedOut++
	}
	return timedOut, nil
}

// ExpireTimedOutTasks terminalizes at most 24 overdue assignments per pass.
// Deadline ordering prevents healthy future work from starving older timeouts.
func (repository *TaskRepository) ExpireTimedOutTasks(ctx context.Context, now time.Time) (int, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return 0, err
	}
	if err := recordcodec.ValidateTimestamp("task timeout collector", now); err != nil {
		return 0, err
	}
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: taskjournal.TaskTimeoutIndexPrefix, Limit: 24})
	if err != nil {
		return 0, err
	}
	expired := 0
	for _, value := range page.Values {
		taskID, deadline, err := taskjournal.ParseTaskTimeoutIndexKey(value.Key)
		if err != nil {
			return expired, err
		}
		assignment, err := taskassignments.DecodeTaskAssignment(value.Value)
		assignmentDeadline := assignment.Deadline
		if assignment.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly {
			assignmentDeadline = assignment.RecoveryDeadline
		}
		if err != nil || assignment.TaskID != taskID || !assignmentDeadline.Equal(deadline) {
			return expired, errs.New(errs.KindInternal, "task timeout index does not match assignment")
		}
		if deadline.After(now) {
			break
		}
		if assignment.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly {
			processed, markErr := repository.markReleaseRecoveryProofRequired(ctx, assignment, page.ReadRevision, now)
			if markErr != nil {
				if errors.Is(markErr, errs.New(errs.KindStateConflict, "")) {
					continue
				}
				return expired, markErr
			}
			if processed {
				expired++
			}
			continue
		}
		if assignment.Executor == taskjournal.TaskExecutorController {
			_, err = repository.AcknowledgeControllerTask(ctx, taskID, taskjournal.TaskStatusTimedOut, now)
		} else {
			result := taskjournal.TaskResultRecord{
				Kind: taskjournal.TaskResultCompose, Diagnostic: taskjournal.TaskResultDiagnosticNone,
				ReconciliationRequired: true, ExecutionEpoch: assignment.ExecutionEpoch,
				ReleaseRecoveryRecordSHA256: assignment.ReleaseRecoveryRecordSHA256,
			}
			if current, assignmentErr := repository.GetTaskAssignment(ctx, taskID); assignmentErr == nil {
				if classified, candidate, classifyErr := repository.preparedTaskTimeoutResult(ctx, current); candidate {
					if classifyErr != nil {
						return expired, classifyErr
					}
					result = classified
				}
			}
			_, err = repository.AcknowledgeTask(
				ctx,
				assignment.AgentID,
				assignment.AgentGeneration,
				taskID,
				assignment.AssignmentID,
				taskjournal.TaskStatusTimedOut,
				result,
				now,
			)
		}
		if err != nil {
			if errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				continue
			}
			return expired, err
		}
		expired++
	}
	return expired, nil
}
