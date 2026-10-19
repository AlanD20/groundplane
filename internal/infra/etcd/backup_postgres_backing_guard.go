package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type postgresBackingGuardCleanupWork struct {
	source     backupwork.StagingSource
	owner      environmentfence.Owner
	expected   map[uint32]string
	covered    map[uint32]bool
	conditions []etcdstore.Condition
}

// The first staging ACK can release database locks only after every started
// source has an exact discard acknowledgement. Captured work and resource state,
// not the retired Task plan, authorize this transaction.
func (repository *TaskRepository) preparePostgresBackingGuardStagingCleanup(ctx context.Context,
	delivery etcdstore.Versioned[backupruntime.BackupStagingDeliveryRecord],
) (postgresbackingguard.Plan, error) {
	result := postgresbackingguard.Plan{}
	if delivery.Record.Plan == nil || delivery.Record.Inventory == nil ||
		len(delivery.Record.Plan.Dispositions) != len(delivery.Record.Inventory.Entries) {
		return result, backupStagingConflict()
	}
	inputs, err := backupwork.NewInputRepository(repository.store)
	if err != nil {
		return result, err
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return result, err
	}
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: delivery.Record.HostID,
		AgentID: delivery.Record.AgentID, Generation: delivery.Record.AgentGeneration}
	works := make(map[string]*postgresBackingGuardCleanupWork)
	for index, disposition := range delivery.Record.Plan.Dispositions {
		guard := disposition.GetDatabaseGuard()
		if guard == nil || disposition.GetDiscardRecovered() == nil {
			continue
		}
		if guard.GetPostgresRecoveryApply() != nil || guard.GetMysqlRecoveryApply() != nil {
			return result, backupStagingConflict()
		}
		source, err := inputs.ReadStagingSource(
			ctx,
			queue,
			identity,
			delivery.Record.Inventory.Entries[index].RecoveryKeySha256,
		)
		if err != nil {
			return result, err
		}
		item := source.View.Item.Item
		terminal := item.Phase == workexecution.Settled &&
			(item.Cleanup == workexecution.CleanupNone || item.Cleanup == workexecution.CleanupComplete)
		if guard.TaskId != source.View.Task.Task.ID || guard.TerminalCleanup != terminal ||
			guard.CompletedTask != (terminal && item.Outcome == workexecution.Succeeded) ||
			!postgresBackingGuardPlanSelectionMatches(source, guard) {
			return result, backupStagingConflict()
		}
		result.Conditions = append(result.Conditions, source.Conditions...)
		if !terminal || item.Outcome == workexecution.Succeeded {
			continue
		}
		cleanup := works[item.ID]
		if cleanup == nil {
			cleanup, err = loadPostgresBackingGuardCleanupWork(ctx, inputs, source)
			if err != nil {
				return result, err
			}
			works[item.ID] = cleanup
		} else if cleanup.source.View.Item.Revision != source.View.Item.Revision ||
			!proto.Equal(cleanup.source.Input, source.Input) {
			return result, backupStagingConflict()
		}
		if cleanup.expected[source.Ordinal] == "" || cleanup.covered[source.Ordinal] {
			return result, backupStagingConflict()
		}
		cleanup.covered[source.Ordinal] = true
	}
	workIDs := make([]string, 0, len(works))
	for id := range works {
		workIDs = append(workIDs, id)
	}
	slices.Sort(workIDs)
	for _, id := range workIDs {
		cleanup := works[id]
		if len(cleanup.expected) != len(cleanup.covered) {
			result.Clear()
			return postgresbackingguard.Plan{}, backupStagingConflict()
		}
		environments := make([]string, 0, len(cleanup.expected))
		for _, environment := range cleanup.expected {
			environments = append(environments, environment)
		}
		slices.Sort(environments)
		environments = slices.Compact(environments)
		plan, err := postgresbackingguard.PrepareOwnership(ctx, repository.store, environments,
			cleanup.source.Input.EnvironmentId, cleanup.owner, cleanup.source.View.ReadRevision, true)
		if err != nil {
			result.Clear()
			return postgresbackingguard.Plan{}, err
		}
		result.Conditions = append(result.Conditions, cleanup.conditions...)
		result.Conditions = append(result.Conditions, plan.Conditions...)
		result.Mutations = append(result.Mutations, plan.Mutations...)
	}
	return result, nil
}

func loadPostgresBackingGuardCleanupWork(ctx context.Context, inputs *backupwork.InputRepository,
	source backupwork.StagingSource,
) (*postgresBackingGuardCleanupWork, error) {
	cleanup := &postgresBackingGuardCleanupWork{
		source:   source,
		expected: make(map[uint32]string),
		covered:  make(map[uint32]bool),
	}
	switch source.Input.Operation {
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE:
		run, guard, err := inputs.LoadStagingCapture(ctx, source)
		if err != nil {
			return nil, err
		}
		if !backupruntime.TerminalBackupRunState(run.State) || run.State == backupruntime.BackupRunCompleted {
			return nil, backupStagingConflict()
		}
		cleanup.conditions = append(cleanup.conditions, guard)
		cleanup.owner = postgresbackingguard.Owner(backupruntime.BackupOperationBackup, run.OperationID, run.TaskID)
		for index, attempt := range run.Sources {
			if attempt.Kind != backupruntime.BackupRuntimeSourceAttach ||
				attempt.State == backupruntime.BackupSourceAttemptUnstarted {
				continue
			}
			step := source.Input.Steps[index]
			environment := ""
			if step.GetCapture().GetPostgres() != nil && attempt.Snapshot.Postgres != nil &&
				attempt.Snapshot.MySQL == nil {
				environment = attempt.Snapshot.Postgres.BackingEnvironmentID
			} else if step.GetCapture().GetMysql() != nil && attempt.Snapshot.MySQL != nil && attempt.Snapshot.Postgres == nil {
				environment = attempt.Snapshot.MySQL.BackingEnvironmentID
			}
			if environment == "" {
				return nil, backupStagingConflict()
			}
			cleanup.expected[uint32(index)] = environment
		}
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE:
		restored, guard, err := inputs.LoadStagingRestore(ctx, source)
		if err != nil {
			return nil, err
		}
		if restored.State != backupruntime.BackupRestoreFailedSafe || len(source.Input.Steps) != 1 {
			return nil, backupStagingConflict()
		}
		environment, found, err := backupruntime.DatabaseRestoreBackingEnvironmentID(restored)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, backupStagingConflict()
		}
		cleanup.conditions = append(cleanup.conditions, guard)
		cleanup.owner = postgresbackingguard.Owner(
			backupruntime.BackupOperationRestore,
			restored.OperationID,
			restored.TaskID,
		)
		cleanup.expected[0] = environment
	default:
		return nil, backupStagingConflict()
	}
	if len(cleanup.expected) == 0 {
		return nil, backupStagingConflict()
	}
	return cleanup, nil
}

func postgresBackingGuardPlanSelectionMatches(
	source backupwork.StagingSource,
	guard *agentpb.BackupDatabaseStagingGuard,
) bool {
	step := source.Input.Steps[source.Ordinal]
	if !proto.Equal(step, guard.Step) {
		return false
	}
	serviceID := step.GetCapture().GetPostgres().GetDatabaseServiceId()
	if mysql := step.GetCapture().GetMysql(); mysql != nil {
		serviceID = mysql.DatabaseServiceId
	}
	if restore := step.GetRestore().GetPostgres(); restore != nil {
		serviceID = restore.GetDatabaseServiceId()
	}
	if mysql := step.GetRestore().GetMysql(); mysql != nil {
		serviceID = mysql.GetDatabaseServiceId()
	}
	if serviceID == "" {
		return false
	}
	factMatches := 0
	for _, fact := range source.Input.Services {
		if fact.GetServiceId() == serviceID {
			factMatches++
			if !proto.Equal(fact, guard.GetDatabaseService()) {
				return false
			}
		}
	}
	artifactMatches := 0
	for _, artifact := range source.Input.Artifacts {
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
