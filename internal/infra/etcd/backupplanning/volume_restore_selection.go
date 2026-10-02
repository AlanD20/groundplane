package backupplanning

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type VolumeRestoreSelectionInput struct {
	EnvironmentID      string
	SourceID           string
	RecoveryPointID    string
	TaskID             string
	OperationID        string
	CreatedAt          time.Time
	FixedRevision      int64
	UsesOldIdentity    bool
	ResolveServiceFact BackupServiceFactResolver
}

type VolumeRestoreSelection struct {
	Restore      backupruntime.BackupRestoreRecord
	Scope        *agentpb.BackupPlanScope
	Owner        taskjournal.TaskOwner
	Connector    *agentpb.BackupConnectorAuthority
	Encryption   *agentpb.BackupEncryptionAuthority
	Projection   *agentpb.BackupVolumeProjectionAuthority
	Artifact     *agentpb.ComposeArtifact
	ReadRevision int64
	Conditions   []etcdstore.Condition
}

// PrepareVolumeRestoreSelection freezes the surviving destination and the
// selected immutable Point at one MVCC view. No current-state fallback is
// permitted when a Point names a missing archive ledger or projection.
func (repository *Planner) PrepareVolumeRestoreSelection(ctx context.Context,
	input VolumeRestoreSelectionInput,
) (VolumeRestoreSelection, error) {
	var zero VolumeRestoreSelection
	if repository == nil || repository.store == nil || input.ResolveServiceFact == nil {
		return zero, errs.New(errs.KindInternal, "Volume Restore admission is not configured")
	}
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return zero, err
	}
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(ids.KindBackupSource, input.SourceID) != nil ||
		ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		input.RecoveryPointID != "" && ids.Validate(ids.KindRecoveryPoint, input.RecoveryPointID) != nil ||
		input.FixedRevision < 0 || !backupruntime.ValidBackupRuntimeInstant(input.CreatedAt) {
		return zero, errs.New(errs.KindValidationFailed, "Volume Restore selection is invalid")
	}
	keys := []string{hierarchy.EnvironmentKey(input.EnvironmentID), backuppolicy.BackupSourceKey(input.SourceID)}
	anchor, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: input.FixedRevision})
	if err != nil {
		return zero, err
	}
	if anchor == nil || anchor.ReadRevision <= 0 || len(anchor.Values) != len(keys) ||
		input.FixedRevision > 0 && anchor.ReadRevision != input.FixedRevision {
		return zero, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] == nil || anchor.Values[1] == nil {
		return zero, errs.New(errs.KindStateConflict, "restore Environment or source is unavailable")
	}
	environment, environmentErr := hierarchy.DecodeEnvironment(anchor.Values[0].Value)
	source, sourceErr := backuppolicy.DecodeBackupSourceRecord(anchor.Values[1].Value)
	if environmentErr != nil || sourceErr != nil || environment.ID != input.EnvironmentID ||
		source.ID != input.SourceID {
		return zero, backupruntime.CorruptBackupRuntimeRecord()
	}
	if source.EnvironmentID != input.EnvironmentID ||
		backupruntime.BackupRuntimeSourceKind(source.Kind) != backupruntime.BackupRuntimeSourceVolume ||
		ids.Validate(ids.KindVolume, source.TargetID) != nil {
		return zero, errs.New(errs.KindStateConflict, "restore source is not a Volume in this Environment")
	}
	selected := NewPlanner(fixedSnapshotStore{store: repository.store, revision: anchor.ReadRevision})
	point, err := selected.selectRestorePoint(ctx, input.SourceID, input.RecoveryPointID, anchor.ReadRevision)
	if err != nil {
		return zero, err
	}
	if point.Record.EnvironmentID != input.EnvironmentID || point.Record.SourceID != source.ID ||
		point.Record.SourceKind != backupruntime.BackupRuntimeSourceVolume || point.Record.TargetID != source.TargetID {
		return zero, errs.New(errs.KindStateConflict, "Recovery Point does not belong to this Volume")
	}
	snapshot := &restoreSnapshot{
		fixedSnapshotStore: fixedSnapshotStore{store: repository.store, revision: anchor.ReadRevision},
		conditions: map[string]int64{
			keys[0]: anchor.Values[0].ModRevision,
			keys[1]: anchor.Values[1].ModRevision,
		},
	}
	selected = NewPlanner(snapshot)
	point, err = selected.reader.GetBackupRecoveryPoint(ctx, point.Record.ID)
	if err != nil {
		return zero, err
	}
	owner, err := selected.manualBackupOwner(ctx, environment, anchor.ReadRevision)
	if err != nil {
		return zero, err
	}
	scope, err := selected.manualBackupPlanScope(ctx, anchor.Values[0], owner.ProjectID, anchor.ReadRevision)
	if err != nil {
		return zero, err
	}
	attempt, err := selected.prepareManualVolumeSource(ctx,
		backupruntime.BackupRunSourceAttemptRecord{TargetID: source.TargetID}, input.EnvironmentID, anchor.ReadRevision)
	if err != nil {
		return zero, err
	}
	current := attempt.Snapshot.Volume
	projection, artifact, err := selected.manualBackupVolumeArtifact(ctx, current, anchor.ReadRevision)
	if err != nil {
		return zero, err
	}
	for _, consumer := range current.Services {
		if _, err := addBackupServiceFact(ctx, input.ResolveServiceFact, scope,
			consumer.ServiceID, input.EnvironmentID, anchor.ReadRevision); err != nil {
			return zero, err
		}
	}
	connector, connectorRevision, credentialRevision, hasDirect, err := selected.manualBackupConnector(
		ctx, point.Record.ConnectorID, input.EnvironmentID, anchor.ReadRevision)
	if err != nil {
		return zero, err
	}
	if connector.Connector.Endpoint != point.Record.ConnectorEndpoint ||
		connector.Connector.Bucket != point.Record.ConnectorBucket ||
		connector.Connector.Prefix != point.Record.ConnectorPrefix ||
		connector.Connector.Region != point.Record.ConnectorRegion ||
		connector.Connector.PathStyle != point.Record.ConnectorPathStyle {
		return zero, errs.New(errs.KindStateConflict, "Recovery Point Connector location changed")
	}
	run := backupruntime.BackupRunRecord{EnvironmentID: input.EnvironmentID,
		ConnectorID: point.Record.ConnectorID, ConnectorRevision: connectorRevision,
		ConnectorCredentialsRevision: credentialRevision, ConnectorHasDirectCredentials: hasDirect,
		ConnectorEndpoint: point.Record.ConnectorEndpoint, ConnectorBucket: point.Record.ConnectorBucket,
		ConnectorPrefix: point.Record.ConnectorPrefix, ConnectorRegion: point.Record.ConnectorRegion,
		ConnectorPathStyle: point.Record.ConnectorPathStyle, Encryption: point.Record.Encryption}
	connectorAuthority, err := selected.manualBackupConnectorAuthority(ctx, run, owner.ProjectID, anchor.ReadRevision)
	if err != nil {
		return zero, err
	}
	var encryption *agentpb.BackupEncryptionAuthority
	if point.Record.Encryption == backupruntime.BackupRuntimeEncryptionNone {
		encryption = &agentpb.BackupEncryptionAuthority{Kind: agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE}
	} else {
		encryption, err = selected.configRestoreEncryption(ctx, &run, point.Record, input.UsesOldIdentity, anchor.ReadRevision)
		if err != nil {
			return zero, err
		}
	}
	restore := backupruntime.BackupRestoreRecord{TaskID: input.TaskID, OperationID: input.OperationID,
		EnvironmentID: input.EnvironmentID, RestoreGenerationID: ids.NewULID(),
		RecoveryPointRevision: point.Revision, Point: point.Record.BackupRecoveryPointSnapshot,
		SourceRevision: anchor.Values[1].ModRevision, CurrentTarget: backupruntime.BackupRestoreTargetSnapshot{Volume: current},
		ConnectorRevision: connectorRevision, ConnectorHasDirectCredentials: hasDirect,
		ConnectorCredentialsRevision: credentialRevision, ExpectedKeyRecordRevision: run.BackupKeyRecordRevision,
		ExpectedKeyValueRevision: run.BackupKeyValueRevision, UsesOldIdentity: input.UsesOldIdentity,
		State: backupruntime.BackupRestoreQueued, ServiceCount: uint32(len(current.Services)),
		Verification: backupruntime.BackupVerificationPending,
		CreatedAt:    input.CreatedAt.UTC(), UpdatedAt: input.CreatedAt.UTC()}
	if err := backupruntime.ValidateBackupRestoreRecord(restore); err != nil {
		return zero, err
	}
	return VolumeRestoreSelection{Restore: restore, Scope: scope, Owner: owner, Connector: connectorAuthority,
		Encryption: encryption, Projection: projection, Artifact: artifact, ReadRevision: anchor.ReadRevision,
		Conditions: snapshot.compares()}, nil
}
