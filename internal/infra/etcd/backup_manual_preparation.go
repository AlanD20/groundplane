package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareManualBackupRun derives all durable evidence from one fixed revision.
func (repository *BackupRuntimeRepository) PrepareManualBackupRun(
	ctx context.Context,
	input ManualBackupRunInput,
	resolvePostgres BackupPostgresIdentityResolver,
) (PreparedManualBackupRun, error) {
	if repository == nil || repository.store == nil {
		return PreparedManualBackupRun{}, errs.New(
			errs.KindInternal,
			"backup runtime repository is not configured",
		)
	}
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return PreparedManualBackupRun{}, err
	}
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(
			ids.KindTask,
			input.TaskID,
		) != nil || ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		ids.Validate(
			ids.KindPlan,
			input.PlanID,
		) != nil || !backupruntime.ValidBackupRuntimeInstant(input.CreatedAt) ||
		input.FixedRevision < 0 || resolvePostgres == nil {
		return PreparedManualBackupRun{}, errs.New(
			errs.KindValidationFailed,
			"manual backup run input is invalid",
		)
	}
	anchorKeys := []string{
		hierarchyrecord.EnvironmentKey(input.EnvironmentID),
		backuppolicy.BackupPolicyKey(input.EnvironmentID),
	}
	var anchor *etcdstore.GetManyResult
	var err error
	if input.FixedRevision == 0 {
		anchor, err = repository.readCurrentKeys(ctx, anchorKeys)
	} else {
		anchor, err = repository.readFixedKeys(ctx, anchorKeys, input.FixedRevision)
	}
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] == nil || anchor.Values[1] == nil {
		return PreparedManualBackupRun{}, errs.New(
			errs.KindStateConflict,
			"backup Environment or policy is unavailable",
		)
	}
	fixedRevision := anchor.ReadRevision
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(anchor.Values[0].Value)
	policy, policyErr := backuppolicy.DecodeBackupPolicyRecord(anchor.Values[1].Value)
	if environmentErr != nil || policyErr != nil || environment.ID != input.EnvironmentID ||
		policy.EnvironmentID != input.EnvironmentID {
		return PreparedManualBackupRun{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	if !policy.Enabled || len(policy.SourceIDs) == 0 ||
		len(policy.SourceIDs) > backuppolicy.MaximumBackupPolicySources {
		return PreparedManualBackupRun{}, errs.New(
			errs.KindStateConflict,
			"backup policy is disabled or unconfigured",
		)
	}
	owner, err := repository.manualBackupOwner(ctx, environment, fixedRevision)
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	connector, connectorRevision, credentialsRevision, hasDirect, err := repository.manualBackupConnector(
		ctx,
		policy.ConnectorID,
		input.EnvironmentID,
		fixedRevision,
	)
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	initiator := input.Initiator
	if initiator == "" {
		initiator = backupruntime.BackupRunInitiatorOperator
	}
	if initiator != backupruntime.BackupRunInitiatorOperator && initiator != backupruntime.BackupRunInitiatorSchedule {
		return PreparedManualBackupRun{}, errs.New(errs.KindValidationFailed, "backup run initiator is invalid")
	}
	if initiator == backupruntime.BackupRunInitiatorOperator && input.ScheduledAt != nil {
		return PreparedManualBackupRun{}, errs.New(
			errs.KindValidationFailed,
			"operator backup run cannot have a schedule",
		)
	}
	if initiator == backupruntime.BackupRunInitiatorSchedule &&
		(input.ScheduledAt == nil || !backupruntime.ValidBackupRuntimeInstant(input.ScheduledAt.UTC()) || input.ScheduledAt.After(input.CreatedAt)) {
		return PreparedManualBackupRun{}, errs.New(errs.KindValidationFailed, "scheduled backup run time is invalid")
	}
	run := backupruntime.BackupRunRecord{
		TaskID:                        input.TaskID,
		OperationID:                   input.OperationID,
		EnvironmentID:                 input.EnvironmentID,
		PolicyRevision:                anchor.Values[1].ModRevision,
		RetentionKeep:                 int64(policy.Keep),
		Initiator:                     initiator,
		ConnectorID:                   policy.ConnectorID,
		ConnectorRevision:             connectorRevision,
		ConnectorEndpoint:             connector.Connector.Endpoint,
		ConnectorBucket:               connector.Connector.Bucket,
		ConnectorPrefix:               connector.Connector.Prefix,
		ConnectorRegion:               connector.Connector.Region,
		ConnectorPathStyle:            connector.Connector.PathStyle,
		ConnectorHasDirectCredentials: hasDirect,
		ConnectorCredentialsRevision:  credentialsRevision,
		Encryption:                    backupruntime.BackupRuntimeEncryption(policy.Encryption),
		State:                         backupruntime.BackupRunQueued,
		CreatedAt:                     input.CreatedAt.UTC(),
		UpdatedAt:                     input.CreatedAt.UTC(),
	}
	if input.ScheduledAt != nil {
		scheduledAt := input.ScheduledAt.UTC()
		run.ScheduledAt = &scheduledAt
	}
	if err := repository.manualBackupKey(ctx, &run, fixedRevision); err != nil {
		return PreparedManualBackupRun{}, err
	}
	sourceKeys := make([]string, len(policy.SourceIDs))
	for index, sourceID := range policy.SourceIDs {
		sourceKeys[index] = backuppolicy.BackupSourceKey(sourceID)
	}
	sourceRead, err := repository.readFixedKeys(ctx, sourceKeys, fixedRevision)
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	defer etcdstore.ClearValues(sourceRead.Values)
	run.Sources = make([]backupruntime.BackupRunSourceAttemptRecord, len(sourceRead.Values))
	for index, value := range sourceRead.Values {
		if value == nil {
			return PreparedManualBackupRun{}, errs.New(
				errs.KindStateConflict,
				"backup source is unavailable",
			)
		}
		source, decodeErr := backuppolicy.DecodeBackupSourceRecord(value.Value)
		if decodeErr != nil || source.ID != policy.SourceIDs[index] ||
			source.EnvironmentID != input.EnvironmentID {
			return PreparedManualBackupRun{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		attempt, prepareErr := repository.prepareManualBackupSource(
			ctx, run, source, value.ModRevision, uint32(index), fixedRevision, resolvePostgres,
		)
		if prepareErr != nil {
			return PreparedManualBackupRun{}, prepareErr
		}
		run.Sources[index] = attempt
	}
	lock := backupruntime.BackupOperationLockRecord{
		EnvironmentID: input.EnvironmentID, OperationID: input.OperationID, TaskID: input.TaskID,
		Kind: backupruntime.BackupOperationBackup, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
	}
	plan, err := repository.prepareBackupRunPublication(ctx, run, lock, fixedRevision)
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	publication := &PreparedBackupRunPublication{
		state: &preparedBackupRunState{repository: repository, plan: plan},
	}
	return PreparedManualBackupRun{
		Run: backupruntime.CloneBackupRunPublicationRecord(run), Owner: owner, Publication: publication,
	}, nil
}
