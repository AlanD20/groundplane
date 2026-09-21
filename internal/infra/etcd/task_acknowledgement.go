package etcd

import (
	"context"
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
// have no Compose result; their durable event journal carries execution detail.
func (repository *TaskRepository) AcknowledgeControllerTask(
	ctx context.Context,
	taskID string,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	return repository.acknowledgeTask(
		ctx, taskjournal.TaskExecutorController, "", 0, taskID, "", terminalStatus, nil, terminalAt, "",
	)
}
