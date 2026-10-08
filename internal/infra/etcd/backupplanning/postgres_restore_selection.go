package backupplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/databaseversion"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicefactauthority"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type DatabaseRestoreSelectionInput struct {
	ResolveVersions              func(context.Context, DatabaseRestoreSelection) (databaseversion.Target, error)
	Preview                      bool
	VersionReviewSHA256          string
	AcknowledgeVersionDifference bool
	EnvironmentID                string
	SourceID                     string
	RecoveryPointID              string
	TaskID                       string
	OperationID                  string
	CreatedAt                    time.Time
	FixedRevision                int64
	UsesOldIdentity              bool
	ResolveDatabase              BackupDatabaseIdentityResolver
	ResolveServiceFact           BackupServiceFactResolver
}

type DatabaseRestoreSelection struct {
	Restore      backupruntime.BackupRestoreRecord
	Scope        *agentpb.BackupPlanScope
	Owner        taskjournal.TaskOwner
	Connector    *agentpb.BackupConnectorAuthority
	Encryption   *agentpb.BackupEncryptionAuthority
	Artifacts    []*agentpb.ComposeArtifact
	ReadRevision int64
	Conditions   []etcdstore.Condition
}

// PrepareDatabaseRestoreSelection freezes both the captured database identity
// and the surviving Attach, backed by one fixed MVCC read. Current credentials
// may authorize that exact surviving target, never a different database/role.
func (repository *Planner) PrepareDatabaseRestoreSelection(ctx context.Context,
	input DatabaseRestoreSelectionInput,
) (DatabaseRestoreSelection, error) {
	var zero DatabaseRestoreSelection
	if repository == nil || repository.store == nil || input.ResolveDatabase == nil ||
		input.ResolveServiceFact == nil {
		return zero, errs.New(errs.KindInternal, "PostgreSQL Restore admission is not configured")
	}
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return zero, err
	}
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(ids.KindBackupSource, input.SourceID) != nil ||
		ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		input.RecoveryPointID != "" && ids.Validate(ids.KindRecoveryPoint, input.RecoveryPointID) != nil ||
		input.FixedRevision < 0 || !backupruntime.ValidBackupRuntimeInstant(input.CreatedAt) {
		return zero, errs.New(errs.KindValidationFailed, "PostgreSQL Restore selection is invalid")
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
		return zero, errs.New(errs.KindStateConflict, "Restore Environment or source is unavailable")
	}
	environment, environmentErr := hierarchy.DecodeEnvironment(anchor.Values[0].Value)
	source, sourceErr := backuppolicy.DecodeBackupSourceRecord(anchor.Values[1].Value)
	if environmentErr != nil || sourceErr != nil || environment.ID != input.EnvironmentID ||
		source.ID != input.SourceID {
		return zero, backupruntime.CorruptBackupRuntimeRecord()
	}
	if source.EnvironmentID != input.EnvironmentID ||
		backupruntime.BackupRuntimeSourceKind(source.Kind) != backupruntime.BackupRuntimeSourceAttach ||
		ids.Validate(ids.KindAttach, source.TargetID) != nil {
		return zero, errs.New(errs.KindStateConflict, "Restore source is not a database Attach in this Environment")
	}
	selected := NewPlanner(fixedSnapshotStore{store: repository.store, revision: anchor.ReadRevision})
	point, err := selected.selectRestorePoint(ctx, input.SourceID, input.RecoveryPointID, anchor.ReadRevision)
	if err != nil {
		return zero, err
	}
	if point.Record.EnvironmentID != input.EnvironmentID || point.Record.SourceID != source.ID ||
		point.Record.SourceKind != backupruntime.BackupRuntimeSourceAttach ||
		point.Record.TargetID != source.TargetID ||
		(point.Record.SourceFormat != backupruntime.BackupRuntimeFormatPostgres &&
			point.Record.SourceFormat != backupruntime.BackupRuntimeFormatMySQL) {
		return zero, errs.New(errs.KindStateConflict, "Recovery Point does not belong to this database Attach")
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
	attempt, err := selected.prepareManualDatabaseSource(ctx,
		backupruntime.BackupRunSourceAttemptRecord{TargetID: source.TargetID}, input.EnvironmentID,
		anchor.ReadRevision, input.ResolveDatabase)
	if err != nil {
		return zero, err
	}
	var attachID string
	var attachRevision int64
	var databaseKind servicefactauthority.Kind
	var backingEnvironmentID, backingServiceID, consumerEnvironmentID, consumerServiceID string
	switch point.Record.SourceFormat {
	case backupruntime.BackupRuntimeFormatPostgres:
		databaseKind = servicefactauthority.BackingRuntime
		current, identity := attempt.Snapshot.Postgres, point.Record.Postgres
		if current == nil || attempt.Snapshot.MySQL != nil || current.Database != identity.Database ||
			current.Role != identity.Role || current.BackingEnvironmentID != identity.BackingEnvironmentID ||
			current.BackingServiceID != identity.BackingServiceID ||
			current.ConsumerServiceID != identity.ConsumerServiceID {
			return zero, errs.New(
				errs.KindStateConflict,
				"PostgreSQL Attach no longer names the Point's database and role",
			)
		}
		attachID, attachRevision = current.AttachID, current.AttachRevision
		backingEnvironmentID, backingServiceID = current.BackingEnvironmentID, current.BackingServiceID
		consumerEnvironmentID, consumerServiceID = current.ConsumerEnvironmentID, current.ConsumerServiceID
	case backupruntime.BackupRuntimeFormatMySQL:
		databaseKind = servicefactauthority.MySQLRuntime
		current, identity := attempt.Snapshot.MySQL, point.Record.MySQL
		if current == nil || attempt.Snapshot.Postgres != nil || current.Database != identity.Database ||
			current.Role != identity.Role || current.BackingEnvironmentID != identity.BackingEnvironmentID ||
			current.BackingServiceID != identity.BackingServiceID ||
			current.ConsumerServiceID != identity.ConsumerServiceID ||
			point.Record.MySQLArchive.Validate() != nil {
			return zero, errs.New(errs.KindStateConflict, "MySQL Attach no longer names the Point's database and role")
		}
		attachID, attachRevision = current.AttachID, current.AttachRevision
		backingEnvironmentID, backingServiceID = current.BackingEnvironmentID, current.BackingServiceID
		consumerEnvironmentID, consumerServiceID = current.ConsumerEnvironmentID, current.ConsumerServiceID
	default:
		return zero, errs.New(errs.KindStrategyNotImplemented, "database Restore format is not implemented")
	}
	attachRead, err := snapshot.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{attachments.AttachKey(attachID)}, Revision: anchor.ReadRevision})
	if err != nil {
		return zero, err
	}
	if len(attachRead.Values) != 1 || attachRead.Values[0] == nil ||
		attachRead.Values[0].ModRevision != attachRevision {
		etcdstore.ClearValues(attachRead.Values)
		return zero, errs.New(errs.KindStateConflict, "database Restore Attach revision changed")
	}
	attachSHA := sha256.Sum256(attachRead.Values[0].Value)
	etcdstore.ClearValues(attachRead.Values)
	artifacts := make([]*agentpb.ComposeArtifact, 0, 2)
	databaseArtifact, err := addBackupServiceFact(ctx, input.ResolveServiceFact, scope,
		backingServiceID, backingEnvironmentID, anchor.ReadRevision)
	if err != nil {
		return zero, err
	}
	if err := appendBackupArtifact(&artifacts, databaseArtifact); err != nil {
		return zero, err
	}
	consumerIdentities, dependentIndexes, err := selectDatabaseRestoreConsumers(
		ctx,
		snapshot,
		databaseRestoreConsumerSource{AttachID: attachID, BackingServiceID: backingServiceID,
			ConsumerEnvironmentID: consumerEnvironmentID, ConsumerServiceID: consumerServiceID},
		anchor.ReadRevision,
	)
	if err != nil {
		return zero, err
	}
	databaseService, err := snapshotDatabaseRestoreService(
		ctx,
		snapshot,
		scope,
		backingServiceID,
		backingEnvironmentID,
		databaseArtifact,
		anchor.ReadRevision,
		databaseKind,
	)
	if err != nil {
		return zero, err
	}
	consumers := make([]backupruntime.BackupRestoreDatabaseServiceSnapshot, 0, len(consumerIdentities))
	for _, identity := range consumerIdentities {
		artifact, err := addBackupServiceFact(ctx, input.ResolveServiceFact, scope,
			identity.ServiceID, identity.EnvironmentID, anchor.ReadRevision)
		if err != nil {
			return zero, err
		}
		if err := appendBackupArtifact(&artifacts, artifact); err != nil {
			return zero, err
		}
		service, err := snapshotDatabaseRestoreService(
			ctx,
			snapshot,
			scope,
			identity.ServiceID,
			identity.EnvironmentID,
			artifact,
			anchor.ReadRevision,
			servicefactauthority.ReleaseRuntime,
		)
		if err != nil {
			return zero, err
		}
		consumers = append(consumers, service)
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
	target := backupruntime.BackupRestoreTargetSnapshot{}
	if current := attempt.Snapshot.Postgres; current != nil {
		target.Postgres = &backupruntime.BackupRestorePostgresTarget{
			Source: *current, ConsumerEnvironmentRevision: anchor.Values[0].ModRevision,
			AttachSHA256: hex.EncodeToString(attachSHA[:]), DatabaseService: databaseService,
			Consumers: consumers, DependentIndexes: dependentIndexes,
		}
	} else if current := attempt.Snapshot.MySQL; current != nil {
		target.MySQL = &backupruntime.BackupRestoreMySQLTarget{
			Source: *current, ConsumerEnvironmentRevision: anchor.Values[0].ModRevision,
			AttachSHA256: hex.EncodeToString(attachSHA[:]), DatabaseService: databaseService,
			Consumers: consumers, DependentIndexes: dependentIndexes,
		}
	} else {
		return zero, errs.New(errs.KindStateConflict, "database Restore target is unavailable")
	}
	restore := backupruntime.BackupRestoreRecord{TaskID: input.TaskID, OperationID: input.OperationID,
		EnvironmentID: input.EnvironmentID, RestoreGenerationID: ids.NewULID(),
		RecoveryPointRevision: point.Revision, Point: point.Record.BackupRecoveryPointSnapshot,
		SourceRevision:    anchor.Values[1].ModRevision,
		CurrentTarget:     target,
		ConnectorRevision: connectorRevision, ConnectorHasDirectCredentials: hasDirect,
		ConnectorCredentialsRevision: credentialRevision, ExpectedKeyRecordRevision: run.BackupKeyRecordRevision,
		ExpectedKeyValueRevision: run.BackupKeyValueRevision, UsesOldIdentity: input.UsesOldIdentity,
		State: backupruntime.BackupRestoreQueued, ServiceCount: uint32(len(consumers)),
		DatabaseProgress: &backupruntime.BackupRestoreDatabaseProgress{},
		Verification:     backupruntime.BackupVerificationPending,
		CreatedAt:        input.CreatedAt.UTC(), UpdatedAt: input.CreatedAt.UTC()}
	if err := backupruntime.ValidateBackupRestoreSelection(restore); err != nil {
		return zero, err
	}
	selection := DatabaseRestoreSelection{Restore: restore, Scope: scope, Owner: owner, Connector: connectorAuthority,
		Encryption: encryption, Artifacts: artifacts, ReadRevision: anchor.ReadRevision,
		Conditions: snapshot.compares()}
	if input.ResolveVersions == nil {
		return zero, errs.New(errs.KindInternal, "database version observer is required")
	}
	versions, err := input.ResolveVersions(ctx, selection)
	if err != nil {
		return zero, err
	}
	selection.Restore.TargetVersions = &versions
	digest, err := backupruntime.RestoreVersionReviewDigest(selection.Restore)
	if err != nil {
		return zero, err
	}
	selection.Restore.VersionReviewSHA256 = digest
	selection.Restore.VersionDifferenceAcknowledged = input.AcknowledgeVersionDifference
	if !input.Preview {
		if input.VersionReviewSHA256 != digest {
			return zero, errs.New(errs.KindStateConflict, "Restore preview changed; review the current target again")
		}
		if err := backupruntime.ValidateRestoreVersionReview(selection.Restore); err != nil {
			return zero, err
		}
	}
	return selection, nil
}

func snapshotDatabaseRestoreService(ctx context.Context, snapshot *restoreSnapshot,
	scope *agentpb.BackupPlanScope, serviceID, environmentID string, artifact *agentpb.ComposeArtifact,
	revision int64, kind servicefactauthority.Kind,
) (backupruntime.BackupRestoreDatabaseServiceSnapshot, error) {
	var zero backupruntime.BackupRestoreDatabaseServiceSnapshot
	fact := backupScopeService(scope, serviceID)
	if fact == nil || fact.Service == nil || fact.Compose == nil || fact.PriorRuntimeIntent == nil ||
		artifact == nil || artifact.OwnerId != environmentID {
		return zero, errs.New(errs.KindStateConflict, "database Restore Service fact is unavailable")
	}
	keys := []string{blueprints.EnvironmentBlueprintHeadKey(environmentID),
		services.ServiceRuntimeKey(serviceID), servicefactauthority.Key(kind, environmentID, serviceID)}
	read, err := snapshot.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return zero, err
	}
	defer etcdstore.ClearValues(read.Values)
	if len(read.Values) != len(keys) || read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil ||
		!serviceDigestMatches(read.Values[0], fact.Service) ||
		!serviceDigestMatches(read.Values[2], fact.Compose) ||
		fact.PriorRuntimeIntent == nil {
		return zero, errs.New(errs.KindStateConflict, "database Restore Service fact changed at the selected view")
	}
	applied, err := servicefactauthority.ReadApplied(read.Values[2], kind, environmentID, serviceID)
	if err != nil || !proto.Equal(applied.Artifact, artifact) {
		return zero, errs.New(errs.KindStateConflict, "database Restore applied Service artifact changed")
	}
	prior := backupruntime.BackupServiceIntentAbsent
	switch fact.PriorRuntimeIntent.Kind {
	case agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING:
		prior = backupruntime.BackupServiceIntentRunning
	case agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_STOPPED:
		prior = backupruntime.BackupServiceIntentStopped
	case agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT:
	default:
		return zero, errs.New(errs.KindStateConflict, "database Restore prior Service intent is invalid")
	}
	if prior != backupruntime.BackupServiceIntentAbsent &&
		!serviceDigestMatches(read.Values[1], fact.PriorRuntimeIntent.Intent) {
		return zero, errs.New(errs.KindStateConflict, "database Restore Service intent changed at the selected view")
	}
	factBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(fact)
	if err != nil {
		return zero, err
	}
	defer clear(factBytes)
	artifactBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
	if err != nil {
		return zero, err
	}
	defer clear(artifactBytes)
	factSHA, artifactSHA := sha256.Sum256(factBytes), sha256.Sum256(artifactBytes)
	intentSHA := sha256.Sum256(read.Values[1].Value)
	result := backupruntime.BackupRestoreDatabaseServiceSnapshot{
		ServiceID: serviceID, EnvironmentID: environmentID,
		ServiceRevision: fact.Service.ModRevision, PriorIntent: prior,
		ServiceSHA256:   hex.EncodeToString(fact.Service.Sha256),
		IntentRevision:  read.Values[1].ModRevision,
		IntentSHA256:    hex.EncodeToString(intentSHA[:]),
		ComposeRevision: fact.Compose.ModRevision,
		ComposeSHA256:   hex.EncodeToString(fact.Compose.Sha256),
		FactSHA256:      hex.EncodeToString(factSHA[:]), ArtifactID: artifact.ArtifactId,
		ArtifactSHA256: hex.EncodeToString(artifactSHA[:])}
	return result, nil
}

func serviceDigestMatches(value *etcdstore.KeyValue, digest *agentpb.RevisionDigest) bool {
	if value == nil || digest == nil || value.ModRevision != digest.ModRevision {
		return false
	}
	actual := sha256.Sum256(value.Value)
	return bytes.Equal(actual[:], digest.Sha256)
}
