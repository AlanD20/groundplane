package backupplanning

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func BuildPostgresRestoreAuthority(selected PostgresRestoreSelection) (*agentpb.BackupStepAuthority, error) {
	record := selected.Restore
	if backupruntime.ValidateBackupRestoreRecord(record) != nil ||
		record.Point.SourceKind != backupruntime.BackupRuntimeSourceAttach ||
		record.State != backupruntime.BackupRestoreQueued || selected.Scope == nil ||
		selected.Connector == nil || selected.Encryption == nil || len(selected.Artifacts) == 0 ||
		selected.Scope.EnvironmentId != record.EnvironmentID {
		return nil, errs.New(errs.KindValidationFailed, "PostgreSQL Restore selected authority is invalid")
	}
	target := record.CurrentTarget.Postgres
	attachSHA, err := hex.DecodeString(target.AttachSHA256)
	if err != nil {
		return nil, err
	}
	sourceSHA, err := hex.DecodeString(record.Point.Evidence.SourceSHA256)
	if err != nil {
		return nil, err
	}
	storedSHA, err := hex.DecodeString(record.Point.Evidence.StoredSHA256)
	if err != nil {
		return nil, err
	}
	object := &agentpb.BackupObjectIdentity{Connector: proto.CloneOf(selected.Connector),
		Bucket: record.Point.ConnectorBucket, ObjectKey: record.Point.ObjectKey}
	switch record.Point.Object.Discriminator.Kind {
	case backupobject.DiscriminatorVersionID:
		object.Discriminator = &agentpb.BackupObjectIdentity_VersionId{
			VersionId: &agentpb.BackupS3VersionId{Value: record.Point.Object.Discriminator.Value}}
	case backupobject.DiscriminatorETag:
		object.Discriminator = &agentpb.BackupObjectIdentity_Etag{
			Etag: &agentpb.BackupS3ETag{Value: record.Point.Object.Discriminator.Value}}
	default:
		return nil, errs.New(errs.KindValidationFailed, "PostgreSQL Restore selected object is invalid")
	}
	destination := &agentpb.BackupResourceIdentity{Kind: agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH,
		ResourceId: record.Point.TargetID,
		Resource:   &agentpb.RevisionDigest{ModRevision: target.Source.AttachRevision, Sha256: attachSHA}}
	restore := &agentpb.BackupRestoreAuthority{PointId: record.Point.ID, Destination: destination,
		SourceObject: object, Encryption: proto.CloneOf(selected.Encryption),
		ExpectedEvidence: &agentpb.BackupArtifactEvidence{SourceSizeBytes: record.Point.Evidence.SourceSizeBytes,
			SourceSha256: sourceSHA, StoredSizeBytes: record.Point.Evidence.StoredSizeBytes, StoredSha256: storedSHA},
		Source: &agentpb.BackupRestoreAuthority_Postgres{Postgres: &agentpb.BackupPostgresRestoreAuthority{
			AdapterContractVersion: postgres16protocol.AdapterContractVersion,
			DatabaseServiceId:      target.Source.BackingServiceID, DatabaseName: target.Source.Database,
			RoleName: target.Source.Role, ManagedReleaseIndex: []byte(target.Source.ManagedReleaseIndex)}},
		Identity: &agentpb.BackupRestoreAuthority_OriginalIdentity{OriginalIdentity: &agentpb.BackupOriginalIdentity{
			OriginalExecutionId: record.RestoreGenerationID, Destination: proto.CloneOf(destination),
			OriginalAssignmentId: ids.New(ids.KindAssignment)}}}
	authority := &agentpb.BackupStepAuthority{StepId: ids.New(ids.KindStep),
		ExecutionId:          record.RestoreGenerationID,
		StepDeadlineUnixNano: uint64(record.CreatedAt.Add(6 * time.Hour).UnixNano()),
		Operation:            &agentpb.BackupStepAuthority_Restore{Restore: restore}}
	for _, consumer := range target.Consumers {
		authority.ConsumerServiceIds = append(authority.ConsumerServiceIds, consumer.ServiceID)
	}
	return executionplan.SealBackupStepAuthority(authority)
}
