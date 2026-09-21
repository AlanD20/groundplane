package etcd

import (
	"context"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// preparedTaskTimeoutResult classifies only Tasks with durable effect authority.
// A manual Script cannot have effects before start authorization. The eventual
// acknowledgement re-reads and CAS-fences that execution; this read alone never
// grants terminalization or retry if a start checkpoint wins the race.
func (repository *TaskRepository) preparedTaskTimeoutResult(
	ctx context.Context,
	assignment TaskAssignment,
) (taskjournal.TaskResultRecord, bool, error) {
	if assignment.Task.Record.Type == taskjournal.TaskRemove &&
		assignment.Task.Record.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceVolume {
		return taskjournal.TaskResultRecord{Kind: taskjournal.TaskResultEnvironmentDirectory, Diagnostic: taskjournal.TaskResultDiagnosticNone}, true, nil
	}
	if assignment.Task.Record.Type != taskjournal.TaskScript {
		return repository.candidateReleaseTimeoutResult(ctx, assignment)
	}
	scripts := &ScriptRepository{store: repository.store}
	execution, _, err := scripts.manualScriptExecutionAtRevision(
		ctx, assignment.Task.Record, assignment.Task.ReadRevision,
	)
	if err != nil {
		return taskjournal.TaskResultRecord{}, true, err
	}
	return taskjournal.TaskResultRecord{
		Kind: taskjournal.TaskResultCompose, Diagnostic: taskjournal.TaskResultDiagnosticNone,
		ExecutionEpoch: assignment.Assignment.Record.ExecutionEpoch,
		ReconciliationRequired: execution.State != ScriptExecutionNotStarted ||
			execution.StartAuthorized || execution.ReconciliationRequired,
	}, true, nil
}
