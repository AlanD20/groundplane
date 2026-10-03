package backupplanning

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func ValidateBackupRunExecutionPlan(run backupruntime.BackupRunRecord, plan *agentpb.ExecutionPlan) error {
	if plan == nil || plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP ||
		plan.TargetId != run.EnvironmentID || plan.GetBackupScope().GetEnvironmentId() != run.EnvironmentID ||
		len(plan.Steps) != len(run.Sources) {
		return errs.New(errs.KindValidationFailed, "backup run execution plan is invalid")
	}
	for index, source := range run.Sources {
		step := plan.Steps[index]
		if step == nil || step.GetBackupStep().GetStepId() != step.GetStepId() || source.Ordinal != uint32(index) {
			return errs.New(errs.KindValidationFailed, "backup run source plan evidence is invalid")
		}
		if err := ValidateBackupRunStepAuthority(run, source, step.GetBackupStep(), plan.GetBackupScope()); err != nil {
			return err
		}
	}
	return nil
}

// ValidateBackupRunStepAuthority compares a schema-one step against its frozen
// run inputs. Missing record digests, secret slots and source evidence must be
// supplied by the versioned publication snapshot, never manufactured here.
func ValidateBackupRunStepAuthority(
	run backupruntime.BackupRunRecord,
	source backupruntime.BackupRunSourceAttemptRecord,
	step *agentpb.BackupStepAuthority,
	scope *agentpb.BackupPlanScope,
) error {
	switch source.Kind {
	case backupruntime.BackupRuntimeSourceAttach,
		backupruntime.BackupRuntimeSourceConfig,
		backupruntime.BackupRuntimeSourceVolume:
	default:
		return errs.New(errs.KindStrategyNotImplemented, "backup source strategy is not implemented")
	}
	if _, err := executionplan.ValidateBackupStepAuthority(step); err != nil {
		return err
	}
	capture := step.GetCapture()
	if capture == nil || scope == nil || scope.GetEnvironmentId() != run.EnvironmentID ||
		source.SourceRevision <= 0 || source.TargetRevision <= 0 ||
		capture.GetResource().GetResourceId() != source.TargetID ||
		capture.GetResource().GetResource().GetModRevision() != source.TargetRevision ||
		capture.GetPointId() != source.RecoveryPointID ||
		capture.GetTarget().GetObjectKey() != source.ObjectKey ||
		source.ObjectKey != run.ConnectorPrefix+run.EnvironmentID+"/"+source.SourceID+"/"+source.RecoveryPointID+"/artifact.bin" ||
		!backupRunConnectorMatches(run, capture.GetTarget()) ||
		capture.GetEncryption().GetKind() != backupPlanEncryption(run.Encryption) ||
		step.GetStepDeadlineUnixNano() != uint64(run.CreatedAt.Add(6*time.Hour).UnixNano()) {
		return errs.New(errs.KindValidationFailed, "backup run source plan evidence is invalid")
	}
	if run.Encryption == backupruntime.BackupRuntimeEncryptionAge {
		recipient := sha256.Sum256([]byte(run.Recipient))
		if run.KeyEra <= 0 || run.BackupKeyRecordRevision <= 0 ||
			capture.GetEncryption().GetKeyEra() != uint64(run.KeyEra) ||
			capture.GetEncryption().GetSecretSlot().GetModRevision() != run.BackupKeyValueRevision ||
			!bytes.Equal(capture.GetEncryption().GetRecipientSha256(), recipient[:]) {
			return errs.New(errs.KindValidationFailed, "backup run encryption snapshot changed")
		}
	}
	if !backupRunSourcePlanSnapshotEqual(source, capture, step.GetConsumerServiceIds(), scope) {
		return errs.New(errs.KindValidationFailed, "backup run source snapshot is incomplete or changed")
	}
	return nil
}

func backupRunConnectorMatches(run backupruntime.BackupRunRecord, target *agentpb.BackupObjectTarget) bool {
	connector := target.GetConnector()
	return connector.GetConnectorId() == run.ConnectorID &&
		connector.GetConnector().GetModRevision() == run.ConnectorRevision &&
		connector.GetCanonicalEndpointUrl() == run.ConnectorEndpoint &&
		connector.GetRegion() == run.ConnectorRegion &&
		connector.PathStyle != nil &&
		connector.GetPathStyle() == run.ConnectorPathStyle &&
		connector.GetPrefix() == run.ConnectorPrefix &&
		target.GetBucket() == run.ConnectorBucket
}

func backupRunSourcePlanSnapshotEqual(
	source backupruntime.BackupRunSourceAttemptRecord,
	capture *agentpb.BackupCaptureAuthority,
	consumers []string,
	scope *agentpb.BackupPlanScope,
) bool {
	switch source.Kind {
	case backupruntime.BackupRuntimeSourceAttach:
		snapshot := source.Snapshot.Postgres
		postgres := capture.GetPostgres()
		service := backupScopeService(scope, snapshotServiceID(snapshot))
		return snapshot != nil && postgres != nil && service != nil &&
			source.Format == backupruntime.BackupRuntimeFormatPostgres &&
			capture.Resource.Kind == agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH &&
			postgres.DatabaseServiceId == snapshot.BackingServiceID &&
			service.GetService().GetModRevision() == snapshot.BackingServiceRevision &&
			postgres.DatabaseName == snapshot.Database &&
			postgres.RoleName == snapshot.Role && string(postgres.ManagedReleaseIndex) == snapshot.ManagedReleaseIndex
	case backupruntime.BackupRuntimeSourceConfig:
		snapshot := source.Snapshot.Config
		config := capture.GetConfig()
		return snapshot != nil && config != nil && source.Format == backupruntime.BackupRuntimeFormatConfig &&
			capture.Resource.Kind == agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT &&
			config.EnvironmentId == source.TargetID && config.MetadataSnapshotRevision == snapshot.ReadRevision && len(consumers) == 0
	case backupruntime.BackupRuntimeSourceVolume:
		snapshot := source.Snapshot.Volume
		volume := capture.GetVolume()
		if snapshot == nil || volume == nil {
			return false
		}
		headSHA, headErr := hex.DecodeString(snapshot.HeadSHA256)
		if source.Format != backupruntime.BackupRuntimeFormatVolume ||
			headErr != nil || !bytes.Equal(capture.Resource.Resource.Sha256, headSHA) ||
			capture.Resource.Kind != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_VOLUME ||
			snapshot.EnvironmentID != scope.GetEnvironmentId() || snapshot.VolumeID != source.TargetID ||
			!backupVolumeProjectionMatches(
				snapshot,
				volume.GetProjection(),
			) || len(consumers) != len(snapshot.Services) ||
			scope.GetEnvironment().GetModRevision() != snapshot.EnvironmentRevision {
			return false
		}
		for index, service := range snapshot.Services {
			planned := backupScopeService(scope, service.ServiceID)
			if consumers[index] != service.ServiceID || planned == nil ||
				planned.GetPriorRuntimeIntent().GetIntent().GetModRevision() != service.ServiceRevision ||
				planned.GetPriorRuntimeIntent().GetKind() != backupPlanServiceIntent(service.PriorIntent) ||
				service.ComposeKey == "" || len(service.MountPaths) == 0 {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func snapshotServiceID(snapshot *backupruntime.BackupPostgresSourceSnapshot) string {
	if snapshot == nil {
		return ""
	}
	return snapshot.BackingServiceID
}

func backupScopeService(scope *agentpb.BackupPlanScope, serviceID string) *agentpb.BackupServiceFact {
	for _, service := range scope.GetServices() {
		if service.GetServiceId() == serviceID {
			return service
		}
	}
	return nil
}

func backupVolumeProjectionMatches(
	snapshot *backupruntime.BackupVolumeSourceSnapshot,
	projection *agentpb.BackupVolumeProjectionAuthority,
) bool {
	digest, err := hex.DecodeString(snapshot.ArtifactDigest)
	return err == nil && projection != nil && projection.ArtifactId == snapshot.ArtifactID &&
		bytes.Equal(projection.ArtifactSha256, digest) && projection.ArtifactRevision == snapshot.ArtifactRevision &&
		projection.ProjectionRoot == snapshot.ProjectionRoot && projection.RenderGeneration == snapshot.RenderGeneration &&
		projection.ComposeVolumeKey == snapshot.ComposeVolumeKey && projection.DockerVolumeName == snapshot.DockerVolumeName &&
		projection.AuthorizedVolumeDir == snapshot.AuthorizedVolumeDir
}

func backupPlanEncryption(value backupruntime.BackupRuntimeEncryption) agentpb.BackupEncryption {
	switch value {
	case backupruntime.BackupRuntimeEncryptionNone:
		return agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE
	case backupruntime.BackupRuntimeEncryptionAge:
		return agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE
	default:
		return agentpb.BackupEncryption_BACKUP_ENCRYPTION_UNSPECIFIED
	}
}

func backupPlanServiceIntent(value backupruntime.BackupServiceRuntimeIntent) agentpb.BackupServiceRuntimeIntent {
	switch value {
	case backupruntime.BackupServiceIntentRunning:
		return agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING
	case backupruntime.BackupServiceIntentStopped:
		return agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_STOPPED
	case backupruntime.BackupServiceIntentAbsent:
		return agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT
	default:
		return agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_UNSPECIFIED
	}
}

type PruneExecutionEvidence struct {
	Prune               backupruntime.BackupRecoveryPointPruneRecord
	PruneRevision       int64
	PointRevision       int64
	SourceRevision      int64
	EnvironmentRevision int64
	ConnectorRevision   int64
	ConnectorEndpoint   string
	ConnectorBucket     string
	ConnectorPrefix     string
	ConnectorRegion     string
	ConnectorPathStyle  bool
	RetentionPolicy     *agentpb.RevisionDigest
	PointSHA256         []byte
	ConnectorAuthority  *agentpb.BackupConnectorAuthority
}

func ValidateBackupPruneExecutionPlan(
	dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
	evidence []PruneExecutionEvidence,
	plan *agentpb.ExecutionPlan,
) error {
	if plan == nil || plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE ||
		plan.TargetId != dispatch.EnvironmentID || plan.GetBackupScope().GetEnvironmentId() != dispatch.EnvironmentID ||
		len(plan.Steps) != len(evidence) || len(dispatch.RecoveryPointIDs) != len(evidence) {
		return errs.New(errs.KindValidationFailed, "backup prune execution plan is invalid")
	}
	for index, item := range evidence {
		if err := ValidateBackupPruneExecutionStep(dispatch, item, plan, index); err != nil {
			return err
		}
	}
	return nil
}

// ValidateBackupPruneExecutionStep checks one still-owned object against its
// immutable dispatch and plan. Earlier verified-absent points are already retired.
func ValidateBackupPruneExecutionStep(
	dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
	item PruneExecutionEvidence,
	plan *agentpb.ExecutionPlan,
	index int,
) error {
	if plan == nil || plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE ||
		plan.TargetId != dispatch.EnvironmentID || plan.GetBackupScope().GetEnvironmentId() != dispatch.EnvironmentID ||
		len(plan.Steps) != len(dispatch.RecoveryPointIDs) || index < 0 || index >= len(plan.Steps) {
		return errs.New(errs.KindValidationFailed, "backup prune execution plan is invalid")
	}
	step := plan.Steps[index]
	if step == nil || step.GetBackupStep().GetStepId() != step.GetStepId() {
		return errs.New(errs.KindValidationFailed, "backup prune step binding is invalid")
	}
	authority, err := executionplan.ValidateBackupStepAuthority(step.GetBackupStep())
	if err != nil {
		return err
	}
	prune := authority.GetPrune()
	point := item.Prune.Point
	if prune == nil || len(prune.Objects) != 1 || !proto.Equal(prune.RetentionPolicy, item.RetentionPolicy) ||
		item.Prune.OperationID != dispatch.OperationID || item.PruneRevision <= 0 || item.SourceRevision <= 0 ||
		point.EnvironmentID != dispatch.EnvironmentID || dispatch.RecoveryPointIDs[index] != point.ID ||
		plan.GetBackupScope().GetEnvironment().GetModRevision() != item.EnvironmentRevision ||
		authority.StepDeadlineUnixNano != uint64(dispatch.CreatedAt.Add(30*time.Minute).UnixNano()) {
		return errs.New(errs.KindValidationFailed, "backup prune snapshot authority is invalid")
	}
	object := prune.Objects[0]
	connector := object.GetObject().GetConnector()
	metadataCount, metadataDigest, err := backupPointMetadataEvidence(point)
	if err != nil || object.Ordinal != uint32(index+1) || object.PointId != point.ID ||
		object.GetPoint().
			GetModRevision() !=
			item.PointRevision || !bytes.Equal(object.GetPoint().GetSha256(), item.PointSHA256) ||
		!backupPointArtifactMatches(point.Evidence, object.Evidence) ||
		!backupPointObjectMatches(point.Object, object.Object) ||
		!proto.Equal(connector, item.ConnectorAuthority) ||
		connector.GetConnector().GetModRevision() != item.ConnectorRevision ||
		connector.GetCanonicalEndpointUrl() != item.ConnectorEndpoint || object.Object.Bucket != item.ConnectorBucket ||
		connector.GetPrefix() != item.ConnectorPrefix || connector.GetRegion() != item.ConnectorRegion ||
		connector.GetPathStyle() != item.ConnectorPathStyle ||
		object.MetadataCount != metadataCount || !bytes.Equal(object.MetadataSha256, metadataDigest[:]) {
		return errs.New(errs.KindValidationFailed, "backup prune point plan evidence is invalid")
	}
	return nil
}

func backupPointArtifactMatches(
	native backupruntime.BackupArtifactEvidence,
	wire *agentpb.BackupArtifactEvidence,
) bool {
	return wire != nil && wire.SourceSizeBytes == native.SourceSizeBytes &&
		wire.StoredSizeBytes == native.StoredSizeBytes &&
		hex.EncodeToString(wire.SourceSha256) == native.SourceSHA256 &&
		hex.EncodeToString(wire.StoredSha256) == native.StoredSHA256
}

func backupPointObjectMatches(native backupruntime.BackupObjectIdentity, wire *agentpb.BackupObjectIdentity) bool {
	if wire == nil || wire.GetConnector().GetConnectorId() != native.Target.ConnectorID ||
		wire.GetConnector().GetCanonicalEndpointUrl() != native.Target.ConnectorEndpoint ||
		wire.GetConnector().GetRegion() != native.Target.ConnectorRegion ||
		wire.GetConnector().GetPrefix() != native.Target.ConnectorPrefix ||
		wire.GetConnector().GetPathStyle() != native.Target.ConnectorPathStyle ||
		wire.Bucket != native.Target.ConnectorBucket || wire.ObjectKey != native.Target.ObjectKey {
		return false
	}
	switch selected := wire.Discriminator.(type) {
	case *agentpb.BackupObjectIdentity_VersionId:
		return selected != nil && selected.VersionId != nil && native.Discriminator.Kind == backupobject.DiscriminatorVersionID &&
			native.Discriminator.Value == selected.VersionId.Value
	case *agentpb.BackupObjectIdentity_Etag:
		return selected != nil && selected.Etag != nil && native.Discriminator.Kind == backupobject.DiscriminatorETag &&
			native.Discriminator.Value == selected.Etag.Value
	default:
		return false
	}
}

func backupPointMetadataEvidence(point backupruntime.BackupRecoveryPointSnapshot) (uint32, [sha256.Size]byte, error) {
	if err := backupruntime.ValidateBackupRecoveryPointSnapshot(point); err != nil {
		return 0, [sha256.Size]byte{}, err
	}
	artifact := backupobject.Artifact{
		Key: point.ObjectKey, EnvironmentID: point.EnvironmentID, SourceID: point.SourceID, RecoveryPointID: point.ID,
		SourceFormat: backupobject.SourceFormat(
			point.SourceFormat,
		), Encryption: backupobject.Encryption(point.Encryption),
		Evidence: backupobject.Evidence{
			SourceSizeBytes: point.Evidence.SourceSizeBytes,
			StoredSizeBytes: point.Evidence.StoredSizeBytes,
		},
	}
	sourceDigest, err := hex.DecodeString(point.Evidence.SourceSHA256)
	if err != nil {
		return 0, [sha256.Size]byte{}, err
	}
	storedDigest, err := hex.DecodeString(point.Evidence.StoredSHA256)
	if err != nil {
		return 0, [sha256.Size]byte{}, err
	}
	copy(artifact.Evidence.SourceSHA256[:], sourceDigest)
	copy(artifact.Evidence.StoredSHA256[:], storedDigest)
	if point.Encryption == backupruntime.BackupRuntimeEncryptionAge {
		era := uint64(point.KeyEra)
		artifact.KeyEra = &era
	}
	count, digest := artifact.MetadataEvidence()
	return count, digest, nil
}
