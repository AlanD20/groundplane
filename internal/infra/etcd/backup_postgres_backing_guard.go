package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type postgresBackingGuardCleanupTask struct {
	task                    etcdstore.Versioned[TaskRecord]
	plan                    *agentpb.ExecutionPlan
	nativeKey               string
	owner                   environmentfence.Owner
	consumerEnvironmentID   string
	expectedStepEnvironment map[string]string
	coveredSteps            map[string]struct{}
}

// preparePostgresBackingGuardStagingCleanup accepts only the first durable ACK
// for exact DiscardRecovered dispositions whose PostgreSQL helper inventory was
// already proved clean by the Agent. It independently binds every guard to the
// sealed Task plan and terminal native record before releasing any backing
// Environment lock in the same transaction as that ACK. Terminal Tasks cannot
// be pruned while their exact guard remains, so the delivery and guard CASes
// are the compact commit fence after those immutable records are validated.
func (repository *TaskRepository) preparePostgresBackingGuardStagingCleanup(
	ctx context.Context,
	delivery etcdstore.Versioned[backupruntime.BackupStagingDeliveryRecord],
) (postgresbackingguard.Plan, error) {
	result := postgresbackingguard.Plan{}
	if delivery.Record.Plan == nil || delivery.Record.Inventory == nil ||
		len(delivery.Record.Plan.Dispositions) != len(delivery.Record.Inventory.Entries) {
		return result, backupStagingConflict()
	}
	tasks := make(map[string]*postgresBackingGuardCleanupTask)
	for index, disposition := range delivery.Record.Plan.Dispositions {
		guard := disposition.GetDatabaseGuard()
		if guard == nil || disposition.GetDiscardRecovered() == nil {
			continue
		}
		if guard.GetPostgresRecoveryApply() != nil || guard.GetMysqlRecoveryApply() != nil {
			return postgresbackingguard.Plan{}, backupStagingConflict()
		}
		source, err := repository.ReadBackupStagingSource(
			ctx,
			delivery.Record.AgentID,
			delivery.Record.AgentGeneration,
			delivery.Record.Inventory.Entries[index].RecoveryKeySha256,
		)
		if err != nil || source.Plan == nil || source.Step == nil ||
			guard.GetTaskId() != source.Task.Record.ID ||
			guard.TerminalCleanup != taskjournal.IsTerminalTaskStatus(source.Task.Record.Status) ||
			guard.CompletedTask != (source.Task.Record.Status == taskjournal.TaskStatusCompleted) ||
			!proto.Equal(guard.GetStep(), source.Step) ||
			!postgresBackingGuardPlanSelectionMatches(source, guard) {
			return postgresbackingguard.Plan{}, backupStagingConflict()
		}
		if !taskjournal.IsTerminalTaskStatus(source.Task.Record.Status) ||
			source.Task.Record.Status == taskjournal.TaskStatusCompleted {
			continue
		}
		cleanup, found := tasks[source.Task.Record.ID]
		if !found {
			cleanup, err = repository.loadPostgresBackingGuardCleanupTask(
				ctx,
				source,
				delivery.ReadRevision,
			)
			if err != nil {
				return postgresbackingguard.Plan{}, err
			}
			tasks[source.Task.Record.ID] = cleanup
		} else if cleanup.task.Revision != source.Task.Revision ||
			!proto.Equal(cleanup.plan, source.Plan) {
			return postgresbackingguard.Plan{}, backupStagingConflict()
		}
		stepID := guard.GetStep().GetStepId()
		if cleanup.expectedStepEnvironment[stepID] == "" {
			return postgresbackingguard.Plan{}, backupStagingConflict()
		}
		if _, duplicate := cleanup.coveredSteps[stepID]; duplicate {
			return postgresbackingguard.Plan{}, backupStagingConflict()
		}
		cleanup.coveredSteps[stepID] = struct{}{}
	}

	taskIDs := make([]string, 0, len(tasks))
	for taskID := range tasks {
		taskIDs = append(taskIDs, taskID)
	}
	slices.Sort(taskIDs)
	for _, taskID := range taskIDs {
		cleanup := tasks[taskID]
		if len(cleanup.coveredSteps) != len(cleanup.expectedStepEnvironment) {
			result.Clear()
			return postgresbackingguard.Plan{}, backupStagingConflict()
		}
		environmentSet := make(map[string]struct{})
		for stepID, environmentID := range cleanup.expectedStepEnvironment {
			if _, found := cleanup.coveredSteps[stepID]; !found {
				result.Clear()
				return postgresbackingguard.Plan{}, backupStagingConflict()
			}
			environmentSet[environmentID] = struct{}{}
		}
		environmentIDs := make([]string, 0, len(environmentSet))
		for environmentID := range environmentSet {
			environmentIDs = append(environmentIDs, environmentID)
		}
		slices.Sort(environmentIDs)
		guardPlan, err := postgresbackingguard.PrepareOwnership(
			ctx,
			repository.store,
			environmentIDs,
			cleanup.consumerEnvironmentID,
			cleanup.owner,
			delivery.ReadRevision,
			true,
		)
		if err != nil {
			result.Clear()
			return postgresbackingguard.Plan{}, err
		}
		result.Conditions = append(result.Conditions, guardPlan.Conditions...)
		result.Mutations = append(result.Mutations, guardPlan.Mutations...)
		guardPlan.Conditions = nil
		guardPlan.Mutations = nil
	}
	return result, nil
}

func (repository *TaskRepository) loadPostgresBackingGuardCleanupTask(
	ctx context.Context,
	source BackupStagingSource,
	readRevision int64,
) (*postgresBackingGuardCleanupTask, error) {
	if source.Task.Revision <= 0 || source.Task.Record.TerminalAssignment == nil ||
		!taskjournal.IsTerminalTaskStatus(source.Task.Record.Status) ||
		source.Task.Record.Status == taskjournal.TaskStatusCompleted {
		return nil, backupStagingConflict()
	}
	cleanup := &postgresBackingGuardCleanupTask{
		task:                    source.Task,
		plan:                    source.Plan,
		expectedStepEnvironment: make(map[string]string),
		coveredSteps:            make(map[string]struct{}),
	}
	switch source.Task.Record.Type {
	case taskjournal.TaskBackup:
		cleanup.nativeKey = backupruntime.BackupRunKey(source.Task.Record.ID)
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{cleanup.nativeKey}, Revision: readRevision,
		})
		if err != nil {
			return nil, err
		}
		if read == nil || read.ReadRevision != readRevision || len(read.Values) != 1 ||
			read.Values[0] == nil || read.Values[0].ModRevision != source.Task.Revision {
			return nil, backupStagingConflict()
		}
		defer etcdstore.ClearValues(read.Values)
		run, err := backupruntime.DecodeBackupRunRecord(read.Values[0].Value)
		wantState, stateErr := backupRunStateForTaskStatus(source.Task.Record.Status)
		if err != nil || ValidateBackupRunTaskBinding(source.Task.Record, run) != nil ||
			backupplanning.ValidateBackupRunExecutionPlan(run, source.Plan) != nil ||
			stateErr != nil || run.State != wantState ||
			!backupruntime.TerminalBackupRunState(run.State) || run.State == backupruntime.BackupRunCompleted {
			return nil, backupStagingConflict()
		}
		cleanup.owner = postgresbackingguard.Owner(
			backupruntime.BackupOperationBackup,
			run.OperationID,
			run.TaskID,
		)
		cleanup.consumerEnvironmentID = run.EnvironmentID
		for index, attempt := range run.Sources {
			if attempt.Kind != backupruntime.BackupRuntimeSourceAttach ||
				attempt.State == backupruntime.BackupSourceAttemptUnstarted {
				continue
			}
			step := source.Plan.Steps[index].GetBackupStep()
			if step == nil || (step.GetCapture().GetPostgres() == nil && step.GetCapture().GetMysql() == nil) ||
				cleanup.expectedStepEnvironment[step.GetStepId()] != "" {
				return nil, backupStagingConflict()
			}
			environmentID, ok := backupAttemptBackingEnvironmentID(attempt)
			if !ok {
				return nil, backupStagingConflict()
			}
			cleanup.expectedStepEnvironment[step.GetStepId()] = environmentID
		}
	case taskjournal.TaskRestore:
		cleanup.nativeKey = backupruntime.BackupRestoreKey(source.Task.Record.ID)
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{cleanup.nativeKey}, Revision: readRevision,
		})
		if err != nil {
			return nil, err
		}
		if read == nil || read.ReadRevision != readRevision || len(read.Values) != 1 ||
			read.Values[0] == nil || read.Values[0].ModRevision != source.Task.Revision {
			return nil, backupStagingConflict()
		}
		defer etcdstore.ClearValues(read.Values)
		restored, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
		if err != nil || restored.TaskID != source.Task.Record.ID ||
			restored.OperationID != source.Task.Record.OperationID ||
			restored.EnvironmentID != source.Task.Record.Owner.EnvironmentID ||
			restored.State != backupruntime.BackupRestoreFailedSafe ||
			backupruntime.ValidateDatabaseRestoreExecutionPlan(restored, source.Plan) != nil {
			return nil, backupStagingConflict()
		}
		backingEnvironmentID, found, err := backupruntime.DatabaseRestoreBackingEnvironmentID(restored)
		if err != nil || !found || len(source.Plan.Steps) != 1 {
			return nil, backupStagingConflict()
		}
		step := source.Plan.Steps[0].GetBackupStep()
		if step == nil || (step.GetRestore().GetPostgres() == nil && step.GetRestore().GetMysql() == nil) {
			return nil, backupStagingConflict()
		}
		cleanup.owner = postgresbackingguard.Owner(
			backupruntime.BackupOperationRestore,
			restored.OperationID,
			restored.TaskID,
		)
		cleanup.consumerEnvironmentID = restored.EnvironmentID
		cleanup.expectedStepEnvironment[step.GetStepId()] = backingEnvironmentID
	default:
		return nil, backupStagingConflict()
	}
	if len(cleanup.expectedStepEnvironment) == 0 {
		return nil, backupStagingConflict()
	}
	return cleanup, nil
}

func postgresBackingGuardPlanSelectionMatches(
	source BackupStagingSource,
	guard *agentpb.BackupDatabaseStagingGuard,
) bool {
	serviceID := source.Step.GetCapture().GetPostgres().GetDatabaseServiceId()
	if mysql := source.Step.GetCapture().GetMysql(); mysql != nil {
		serviceID = mysql.DatabaseServiceId
	}
	if restore := source.Step.GetRestore().GetPostgres(); restore != nil {
		serviceID = restore.GetDatabaseServiceId()
	}
	if mysql := source.Step.GetRestore().GetMysql(); mysql != nil {
		serviceID = mysql.GetDatabaseServiceId()
	}
	if serviceID == "" || source.Plan.GetBackupScope() == nil {
		return false
	}
	factMatches := 0
	for _, fact := range source.Plan.GetBackupScope().GetServices() {
		if fact.GetServiceId() == serviceID {
			factMatches++
			if !proto.Equal(fact, guard.GetDatabaseService()) {
				return false
			}
		}
	}
	artifactMatches := 0
	for _, artifact := range source.Plan.GetArtifacts() {
		for _, service := range artifact.GetServices() {
			if service.GetServiceId() == serviceID &&
				service.GetComposeName() == guard.GetDatabaseService().GetCurrentName() {
				artifactMatches++
				if !proto.Equal(artifact, guard.GetDatabaseArtifact()) {
					return false
				}
			}
		}
	}
	return factMatches == 1 && artifactMatches == 1
}

func backupAttemptBackingEnvironmentID(attempt backupruntime.BackupRunSourceAttemptRecord) (string, bool) {
	if attempt.Snapshot.Postgres != nil && attempt.Snapshot.MySQL == nil {
		return attempt.Snapshot.Postgres.BackingEnvironmentID, true
	}
	if attempt.Snapshot.MySQL != nil && attempt.Snapshot.Postgres == nil {
		return attempt.Snapshot.MySQL.BackingEnvironmentID, true
	}
	return "", false
}
