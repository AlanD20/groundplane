package backupplanning

import (
	"context"
	"crypto/sha256"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfigmaterialization"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ConfigRestoreSelectionInput selects an original surviving target at one view.
// ResolveFiles is the Controller's ordinary managed-file planning seam; it
// receives only the selected immutable predecessor, not current Entry values.
type ConfigRestoreSelectionInput struct {
	EnvironmentID   string
	SourceID        string
	RecoveryPointID string
	TaskID          string
	OperationID     string
	CreatedAt       time.Time
	FixedRevision   int64
	UsesOldIdentity bool
	ResolveFiles    func(context.Context, environmentprojection.EnvironmentComposeProjection) (*agentpb.BackupConfigFileContext, error)
}

type ConfigRestoreSelection struct {
	Restore      backupruntime.BackupRestoreRecord
	Scope        *agentpb.BackupPlanScope
	Owner        taskjournal.TaskOwner
	Connector    *agentpb.BackupConnectorAuthority
	Encryption   *agentpb.BackupEncryptionAuthority
	Files        *agentpb.BackupConfigFileContext
	ReadRevision int64
	Conditions   []etcdstore.Condition
}

func (repository *Planner) PrepareConfigRestoreSelection(ctx context.Context,
	input ConfigRestoreSelectionInput,
) (ConfigRestoreSelection, error) {
	var zero ConfigRestoreSelection
	if repository == nil || repository.store == nil || input.ResolveFiles == nil {
		return zero, errs.New(errs.KindInternal, "config restore admission is not configured")
	}
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return zero, err
	}
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(ids.KindBackupSource, input.SourceID) != nil ||
		ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		input.RecoveryPointID != "" && ids.Validate(ids.KindRecoveryPoint, input.RecoveryPointID) != nil ||
		input.FixedRevision < 0 || !backupruntime.ValidBackupRuntimeInstant(input.CreatedAt) {
		return zero, errs.New(errs.KindValidationFailed, "config restore selection is invalid")
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
	if source.EnvironmentID != input.EnvironmentID {
		return zero, errs.New(errs.KindStateConflict, "restore source belongs to another Environment")
	}
	if backupruntime.BackupRuntimeSourceKind(source.Kind) != backupruntime.BackupRuntimeSourceConfig {
		return zero, errs.New(errs.KindStrategyNotImplemented, "this restore planner accepts Config sources only")
	}
	if source.TargetID != input.EnvironmentID {
		return zero, backupruntime.CorruptBackupRuntimeRecord()
	}
	// All subsequent selection, including latest-Point paging, uses this view.
	selected := NewPlanner(fixedSnapshotStore{store: repository.store, revision: anchor.ReadRevision})
	point, err := selected.selectRestorePoint(ctx, input.SourceID, input.RecoveryPointID, anchor.ReadRevision)
	if err != nil {
		return zero, err
	}
	if point.Record.EnvironmentID != input.EnvironmentID || point.Record.SourceID != source.ID ||
		point.Record.SourceKind != backupruntime.BackupRuntimeSourceConfig || point.Record.TargetID != source.TargetID {
		return zero, errs.New(errs.KindStateConflict, "Recovery Point does not belong to this restore target")
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
	baseline, found, err := blueprints.ReadProjectionAtRevision(
		ctx,
		selected.store,
		input.EnvironmentID,
		anchor.ReadRevision,
	)
	if err != nil {
		return zero, err
	}
	if !found || baseline.Revision <= 0 || baseline.Record.RenderGeneration == math.MaxUint64 {
		return zero, errs.New(errs.KindStateConflict, "restore requires a complete surviving configuration")
	}
	files, err := input.ResolveFiles(ctx, baseline.Record)
	if err != nil {
		return zero, err
	}
	filesSHA, err := backupconfigmaterialization.ContextSHA256(input.EnvironmentID, files)
	if err != nil {
		return zero, err
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
	encryption, err := selected.configRestoreEncryption(
		ctx,
		&run,
		point.Record,
		input.UsesOldIdentity,
		anchor.ReadRevision,
	)
	if err != nil {
		return zero, err
	}
	restore := backupruntime.BackupRestoreRecord{TaskID: input.TaskID, OperationID: input.OperationID,
		EnvironmentID: input.EnvironmentID, RestoreGenerationID: ids.New(ids.KindConfig),
		RecoveryPointRevision: point.Revision, Point: point.Record.BackupRecoveryPointSnapshot,
		SourceRevision: anchor.Values[1].ModRevision, ConnectorRevision: connectorRevision,
		ConnectorHasDirectCredentials: hasDirect, ConnectorCredentialsRevision: credentialRevision,
		ExpectedKeyRecordRevision: run.BackupKeyRecordRevision, ExpectedKeyValueRevision: run.BackupKeyValueRevision,
		UsesOldIdentity: input.UsesOldIdentity,
		CurrentTarget: backupruntime.BackupRestoreTargetSnapshot{Config: &backupruntime.BackupRestoreConfigTarget{
			EnvironmentID: input.EnvironmentID, EnvironmentRevision: anchor.Values[0].ModRevision,
			BaselineRevisionID: baseline.Record.RevisionID, BaselineHeadRevision: baseline.Revision,
			RenderGeneration: baseline.Record.RenderGeneration + 1, FileContextSHA256: filesSHA}},
		State: backupruntime.BackupRestoreQueued, ConfigProgress: &backupruntime.BackupRestoreConfigProgress{},
		Verification: backupruntime.BackupVerificationPending, CreatedAt: input.CreatedAt.UTC(), UpdatedAt: input.CreatedAt.UTC()}
	if err := backupruntime.ValidateBackupRestoreRecord(restore); err != nil {
		return zero, err
	}
	return ConfigRestoreSelection{Restore: restore, Scope: scope, Owner: owner, Connector: connectorAuthority,
		Encryption: encryption, Files: files, ReadRevision: anchor.ReadRevision, Conditions: snapshot.compares()}, nil
}

func (repository *Planner) configRestoreEncryption(ctx context.Context, run *backupruntime.BackupRunRecord,
	point backupruntime.BackupRecoveryPointRecord, usesOldIdentity bool, revision int64,
) (*agentpb.BackupEncryptionAuthority, error) {
	if usesOldIdentity {
		recipient := sha256.Sum256([]byte(point.Recipient))
		era := uint64(point.KeyEra)
		return &agentpb.BackupEncryptionAuthority{Kind: agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
			SecretSlotId: backupsecret.OperatorOldAgeIdentitySlotID, RecipientSha256: recipient[:], KeyEra: &era}, nil
	}
	if err := repository.manualBackupKey(ctx, run, revision); err != nil {
		return nil, err
	}
	if run.KeyEra != point.KeyEra || run.Recipient != point.Recipient {
		return nil, errs.New(errs.KindValidationFailed, "this Recovery Point requires its matching age identity")
	}
	return repository.manualBackupEncryptionAuthority(ctx, *run, revision)
}

func (repository *Planner) selectRestorePoint(ctx context.Context, sourceID, pointID string,
	revision int64,
) (etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord], error) {
	if pointID != "" {
		return repository.reader.GetBackupRecoveryPoint(ctx, pointID)
	}
	request := backupruntime.BackupRuntimeListRequest{Limit: 96, Revision: revision}
	for {
		page, err := repository.reader.ListBackupRecoveryPointsBySource(ctx, sourceID, request)
		if err != nil {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
		}
		if len(page.Items) > 0 {
			return repository.reader.GetBackupRecoveryPoint(ctx, page.Items[0].Record.ID)
		}
		if page.Next == "" {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
				errs.KindRecoveryPointNotFound, "no verified Recovery Point is available for this source")
		}
		if page.Next <= request.StartExclusive || page.Revision != revision {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		request.StartExclusive = page.Next
	}
}
