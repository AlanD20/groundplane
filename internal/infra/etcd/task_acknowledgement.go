package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"
)

// AcknowledgeTask atomically records the Agent's terminal acknowledgement,
// removes assignment and active-operation state, terminalizes replay evidence,
// and advances any owned Attach lifecycle. Replaying the same terminal
// acknowledgement is idempotent.
func (repository *TaskRepository) AcknowledgeTask(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	return repository.acknowledgeTask(
		ctx, taskjournal.TaskExecutorAgent, agentID, agentGeneration, taskID, assignmentID,
		terminalStatus, &result, terminalAt, "",
	)
}

// AcknowledgeEnvironmentCreation atomically terminalizes one Agent Task and
// moves its owned Environment from provisioning to ready or failed.
func (repository *TaskRepository) AcknowledgeEnvironmentCreation(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
	environmentID string,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	return repository.acknowledgeTask(
		ctx,
		taskjournal.TaskExecutorAgent,
		agentID,
		agentGeneration,
		taskID,
		assignmentID,
		terminalStatus,
		&result,
		terminalAt,
		environmentID,
	)
}

// AcknowledgeControllerTask terminalizes one Controller claim. Native Tasks
// retain native failure diagnostics in the same atomic terminal transaction.
func (repository *TaskRepository) AcknowledgeControllerTask(
	ctx context.Context,
	taskID string,
	terminalStatus taskjournal.TaskStatus,
	failure *taskprogress.Failure,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	result := taskjournal.ControllerResult(failure)
	if err := taskjournal.ValidateTaskResult(result, nil, terminalStatus); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	return repository.acknowledgeTask(
		ctx, taskjournal.TaskExecutorController, "", 0, taskID, "", terminalStatus, &result, terminalAt, "",
	)
}
