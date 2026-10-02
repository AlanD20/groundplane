package backupplanning

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BuildConfigRestoreAuthority allocates the first assignment before sealing.
// Claim consumes that same identity; reconnect cannot replace the execution or
// assignment named by the original Restore procedure.
func BuildConfigRestoreAuthority(selected ConfigRestoreSelection) (*agentpb.BackupStepAuthority, error) {
	record := selected.Restore
	if backupruntime.ValidateBackupRestoreRecord(record) != nil ||
		record.Point.SourceKind != backupruntime.BackupRuntimeSourceConfig ||
		record.State != backupruntime.BackupRestoreQueued || selected.Scope == nil ||
		selected.Connector == nil || selected.Encryption == nil || selected.Files == nil ||
		selected.Scope.EnvironmentId != record.EnvironmentID {
		return nil, errs.New(errs.KindValidationFailed, "config restore selected authority is invalid")
	}
	archive, err := record.Point.ConfigArchive.Wire()
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
		return nil, errs.New(errs.KindValidationFailed, "config restore selected object is invalid")
	}
	destination := &agentpb.BackupResourceIdentity{Kind: agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT,
		ResourceId: record.EnvironmentID, Resource: proto.CloneOf(selected.Scope.Environment)}
	executionID := ids.NewULID()
	restore := &agentpb.BackupRestoreAuthority{PointId: record.Point.ID, Destination: destination,
		SourceObject: object, Encryption: proto.CloneOf(selected.Encryption),
		ExpectedEvidence: &agentpb.BackupArtifactEvidence{SourceSizeBytes: record.Point.Evidence.SourceSizeBytes,
			SourceSha256: sourceSHA, StoredSizeBytes: record.Point.Evidence.StoredSizeBytes, StoredSha256: storedSHA},
		Source: &agentpb.BackupRestoreAuthority_Config{Config: &agentpb.BackupConfigRestoreAuthority{
			DestinationEnvironmentId: record.EnvironmentID, ExpectedArchive: archive,
			RestoreGenerationId: record.RestoreGenerationID, RenderGeneration: record.CurrentTarget.Config.RenderGeneration,
			BaselineRevisionId:   record.CurrentTarget.Config.BaselineRevisionID,
			BaselineHeadRevision: record.CurrentTarget.Config.BaselineHeadRevision, Files: proto.CloneOf(selected.Files)}},
		Identity: &agentpb.BackupRestoreAuthority_OriginalIdentity{OriginalIdentity: &agentpb.BackupOriginalIdentity{
			OriginalExecutionId: executionID, Destination: proto.CloneOf(destination),
			OriginalAssignmentId: ids.New(ids.KindAssignment)}}}
	return executionplan.SealBackupStepAuthority(&agentpb.BackupStepAuthority{StepId: ids.New(ids.KindStep),
		ExecutionId: executionID, StepDeadlineUnixNano: uint64(record.CreatedAt.Add(6 * time.Hour).UnixNano()),
		Operation: &agentpb.BackupStepAuthority_Restore{Restore: restore}})
}
