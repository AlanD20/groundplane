package backupplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareManualRunSources derives all durable evidence from one fixed revision.
func (repository *Planner) PrepareManualRunSources(
	ctx context.Context,
	input ManualBackupRunInput,
	resolvePostgres BackupPostgresIdentityResolver,
) (ManualRunSources, error) {
	if repository == nil || repository.store == nil {
		return ManualRunSources{}, errs.New(
			errs.KindInternal,
			"backup runtime repository is not configured",
		)
	}
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return ManualRunSources{}, err
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
		input.FixedRevision < 0 {
		return ManualRunSources{}, errs.New(
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
		anchor, err = repository.reader.ReadCurrentKeys(ctx, anchorKeys)
	} else {
		anchor, err = repository.reader.ReadFixedKeys(ctx, anchorKeys, input.FixedRevision)
	}
	if err != nil {
		return ManualRunSources{}, err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] == nil || anchor.Values[1] == nil {
		return ManualRunSources{}, errs.New(
			errs.KindStateConflict,
			"backup Environment or policy is unavailable",
		)
	}
	fixedRevision := anchor.ReadRevision
	// Existing source/projection owners also read immutable keys. Constrain all
	// of their reads to this snapshot without changing the shared Planner.
	repository = NewPlanner(fixedSnapshotStore{store: repository.store, revision: fixedRevision})
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(anchor.Values[0].Value)
	policy, policyErr := backuppolicy.DecodeBackupPolicyRecord(anchor.Values[1].Value)
	if environmentErr != nil || policyErr != nil || environment.ID != input.EnvironmentID ||
		policy.EnvironmentID != input.EnvironmentID {
		return ManualRunSources{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	if !policy.Enabled || len(policy.SourceIDs) == 0 ||
		len(policy.SourceIDs) > backuppolicy.MaximumBackupPolicySources {
		return ManualRunSources{}, errs.New(
			errs.KindStateConflict,
			"backup policy is disabled or unconfigured",
		)
	}
	owner, err := repository.manualBackupOwner(ctx, environment, fixedRevision)
	if err != nil {
		return ManualRunSources{}, err
	}
	scope, err := repository.manualBackupPlanScope(ctx, anchor.Values[0], owner.ProjectID, fixedRevision)
	if err != nil {
		return ManualRunSources{}, err
	}
	connector, connectorRevision, credentialsRevision, hasDirect, err := repository.manualBackupConnector(
		ctx,
		policy.ConnectorID,
		input.EnvironmentID,
		fixedRevision,
	)
	if err != nil {
		return ManualRunSources{}, err
	}
	initiator := input.Initiator
	if initiator == "" {
		initiator = backupruntime.BackupRunInitiatorOperator
	}
	if initiator != backupruntime.BackupRunInitiatorOperator && initiator != backupruntime.BackupRunInitiatorSchedule {
		return ManualRunSources{}, errs.New(errs.KindValidationFailed, "backup run initiator is invalid")
	}
	if initiator == backupruntime.BackupRunInitiatorOperator && input.ScheduledAt != nil {
		return ManualRunSources{}, errs.New(
			errs.KindValidationFailed,
			"operator backup run cannot have a schedule",
		)
	}
	if initiator == backupruntime.BackupRunInitiatorSchedule &&
		(input.ScheduledAt == nil || !backupruntime.ValidBackupRuntimeInstant(input.ScheduledAt.UTC()) || input.ScheduledAt.After(input.CreatedAt)) {
		return ManualRunSources{}, errs.New(errs.KindValidationFailed, "scheduled backup run time is invalid")
	}
	policyDigest := sha256.Sum256(anchor.Values[1].Value)
	run := backupruntime.BackupRunRecord{
		TaskID:                        input.TaskID,
		OperationID:                   input.OperationID,
		EnvironmentID:                 input.EnvironmentID,
		PolicyRevision:                anchor.Values[1].ModRevision,
		PolicySHA256:                  hex.EncodeToString(policyDigest[:]),
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
		return ManualRunSources{}, err
	}
	connectorAuthority, err := repository.manualBackupConnectorAuthority(ctx, run, owner.ProjectID, fixedRevision)
	if err != nil {
		return ManualRunSources{}, err
	}
	encryptionAuthority, err := repository.manualBackupEncryptionAuthority(ctx, run, fixedRevision)
	if err != nil {
		return ManualRunSources{}, err
	}
	sourceKeys := make([]string, len(policy.SourceIDs))
	for index, sourceID := range policy.SourceIDs {
		sourceKeys[index] = backuppolicy.BackupSourceKey(sourceID)
	}
	sourceRead, err := repository.reader.ReadFixedKeys(ctx, sourceKeys, fixedRevision)
	if err != nil {
		return ManualRunSources{}, err
	}
	defer etcdstore.ClearValues(sourceRead.Values)
	run.Sources = make([]backupruntime.BackupRunSourceAttemptRecord, len(sourceRead.Values))
	authorities := make([]*agentpb.BackupStepAuthority, len(sourceRead.Values))
	artifacts := make([]*agentpb.ComposeArtifact, 0)
	for index, value := range sourceRead.Values {
		if value == nil {
			return ManualRunSources{}, errs.New(
				errs.KindStateConflict,
				"backup source is unavailable",
			)
		}
		source, decodeErr := backuppolicy.DecodeBackupSourceRecord(value.Value)
		if decodeErr != nil || source.ID != policy.SourceIDs[index] ||
			source.EnvironmentID != input.EnvironmentID {
			return ManualRunSources{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		if backupruntime.BackupRuntimeSourceKind(source.Kind) == backupruntime.BackupRuntimeSourceAttach &&
			resolvePostgres == nil {
			return ManualRunSources{}, errs.New(
				errs.KindStrategyNotImplemented,
				"backup PostgreSQL requires its fixed-revision encrypted identity resolver",
			)
		}
		attempt, prepareErr := repository.prepareManualBackupSource(
			ctx, run, source, value.ModRevision, uint32(index), fixedRevision, resolvePostgres,
		)
		if prepareErr != nil {
			return ManualRunSources{}, prepareErr
		}
		authority, prepareErr := repository.manualBackupSourceAuthority(ctx, input, run, &attempt, scope,
			connectorAuthority, encryptionAuthority, fixedRevision, &artifacts)
		if prepareErr != nil {
			return ManualRunSources{}, prepareErr
		}
		run.Sources[index], authorities[index] = attempt, authority
	}
	lock := backupruntime.BackupOperationLockRecord{
		EnvironmentID: input.EnvironmentID, OperationID: input.OperationID, TaskID: input.TaskID,
		Kind: backupruntime.BackupOperationBackup, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
	}
	return ManualRunSources{
		Run:          run,
		Owner:        owner,
		Lock:         lock,
		ReadRevision: fixedRevision,
		Scope:        scope,
		Authority:    authorities,
		Artifacts:    artifacts,
	}, nil
}
