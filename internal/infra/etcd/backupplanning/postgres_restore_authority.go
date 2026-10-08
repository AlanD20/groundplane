package backupplanning

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func BuildDatabaseRestoreAuthority(selected DatabaseRestoreSelection) (*agentpb.BackupStepAuthority, error) {
	if err := backupruntime.ValidateRestoreVersionReview(selected.Restore); err != nil {
		return nil, err
	}
	authority, err := buildDatabaseRestoreAuthority(selected)
	if err != nil {
		return nil, err
	}
	return executionplan.SealBackupStepAuthority(authority)
}

// A preflight carries the selected identity but no executable Step digest.
func BuildDatabaseVersionProbe(selected DatabaseRestoreSelection) (*agentpb.DatabaseVersionProbe, error) {
	authority, err := buildDatabaseRestoreAuthority(selected)
	if err != nil {
		return nil, err
	}
	return &agentpb.DatabaseVersionProbe{
		Authority: authority,
		Services:  selected.Scope.Services,
		Artifacts: selected.Artifacts,
	}, nil
}

func buildDatabaseRestoreAuthority(selected DatabaseRestoreSelection) (*agentpb.BackupStepAuthority, error) {
	record := selected.Restore
	if backupruntime.ValidateBackupRestoreSelection(record) != nil ||
		record.Point.SourceKind != backupruntime.BackupRuntimeSourceAttach ||
		record.State != backupruntime.BackupRestoreQueued || selected.Scope == nil ||
		selected.Connector == nil || selected.Encryption == nil || len(selected.Artifacts) == 0 ||
		selected.Scope.EnvironmentId != record.EnvironmentID {
		return nil, errs.New(errs.KindValidationFailed, "database Restore selected authority is invalid")
	}
	var attachSHA256 string
	var attachRevision int64
	var consumers []backupruntime.BackupRestoreDatabaseServiceSnapshot
	if target := record.CurrentTarget.Postgres; target != nil {
		attachSHA256, attachRevision, consumers = target.AttachSHA256, target.Source.AttachRevision, target.Consumers
	} else if target := record.CurrentTarget.MySQL; target != nil {
		attachSHA256, attachRevision, consumers = target.AttachSHA256, target.Source.AttachRevision, target.Consumers
	} else {
		return nil, errs.New(errs.KindValidationFailed, "database Restore target is unavailable")
	}
	attachSHA, err := hex.DecodeString(attachSHA256)
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
		return nil, errs.New(errs.KindValidationFailed, "database Restore selected object is invalid")
	}
	destination := &agentpb.BackupResourceIdentity{Kind: agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH,
		ResourceId: record.Point.TargetID,
		Resource:   &agentpb.RevisionDigest{ModRevision: attachRevision, Sha256: attachSHA}}
	restore := &agentpb.BackupRestoreAuthority{PointId: record.Point.ID, Destination: destination,
		SourceObject: object, Encryption: proto.CloneOf(selected.Encryption),
		ExpectedEvidence: &agentpb.BackupArtifactEvidence{SourceSizeBytes: record.Point.Evidence.SourceSizeBytes,
			SourceSha256: sourceSHA, StoredSizeBytes: record.Point.Evidence.StoredSizeBytes, StoredSha256: storedSHA},
		Identity: &agentpb.BackupRestoreAuthority_OriginalIdentity{OriginalIdentity: &agentpb.BackupOriginalIdentity{
			OriginalExecutionId: record.RestoreGenerationID, Destination: proto.CloneOf(destination),
			OriginalAssignmentId: ids.New(ids.KindAssignment)}}}
	if target := record.CurrentTarget.Postgres; target != nil {
		archive, err := record.Point.PostgresArchive.Wire()
		if err != nil {
			return nil, err
		}
		restore.Source = &agentpb.BackupRestoreAuthority_Postgres{Postgres: &agentpb.BackupPostgresRestoreAuthority{
			AdapterContractVersion: postgres16protocol.AdapterContractVersion,
			DatabaseServiceId:      target.Source.BackingServiceID, DatabaseName: target.Source.Database,
			RoleName: target.Source.Role, ManagedReleaseIndex: []byte(target.Source.ManagedReleaseIndex), ExpectedArchive: archive}}
		if record.TargetVersions != nil {
			restore.GetPostgres().ExpectedTargetVersions = record.TargetVersions.Wire()
		}
	} else {
		target := record.CurrentTarget.MySQL
		archive, err := record.Point.MySQLArchive.Wire()
		if err != nil {
			return nil, err
		}
		fact := backupScopeService(selected.Scope, target.Source.BackingServiceID)
		if fact == nil {
			return nil, errs.New(errs.KindStateConflict, "MySQL Restore Service fact is unavailable")
		}
		var imageReference string
		for _, artifact := range selected.Artifacts {
			for _, service := range artifact.Services {
				if service.ServiceId == target.Source.BackingServiceID &&
					service.ComposeName == fact.CurrentName {
					if imageReference != "" {
						return nil, errs.New(errs.KindStateConflict, "MySQL Restore workload is ambiguous")
					}
					imageReference = service.ImageReference
				}
			}
		}
		if imageReference == "" {
			return nil, errs.New(errs.KindStateConflict, "MySQL Restore workload is unavailable")
		}
		imageSHA := sha256.Sum256([]byte(imageReference))
		restore.Source = &agentpb.BackupRestoreAuthority_Mysql{Mysql: &agentpb.BackupMySQLRestoreAuthority{
			AdapterContractVersion: mysql84protocol.AdapterContractVersion,
			DatabaseServiceId:      target.Source.BackingServiceID, DatabaseName: target.Source.Database,
			RoleName: target.Source.Role, RequiredServerMajor: mysql84protocol.ServerMajor,
			RequiredServerMinor: mysql84protocol.ServerMinor, ExpectedArchive: archive,
			DatabaseImageReferenceSha256: imageSHA[:],
		}}
		if record.TargetVersions != nil {
			restore.GetMysql().ExpectedTargetVersions = record.TargetVersions.Wire()
		}
	}
	authority := &agentpb.BackupStepAuthority{StepId: ids.New(ids.KindStep),
		ExecutionId:          record.RestoreGenerationID,
		StepDeadlineUnixNano: uint64(record.CreatedAt.Add(6 * time.Hour).UnixNano()),
		Operation:            &agentpb.BackupStepAuthority_Restore{Restore: restore}}
	for _, consumer := range consumers {
		authority.ConsumerServiceIds = append(authority.ConsumerServiceIds, consumer.ServiceID)
	}
	return authority, nil
}
