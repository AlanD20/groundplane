package backupplanning

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *Planner) manualBackupSourceAuthority(
	ctx context.Context,
	input ManualBackupRunInput,
	run backupruntime.BackupRunRecord,
	attempt *backupruntime.BackupRunSourceAttemptRecord,
	scope *agentpb.BackupPlanScope,
	connector *agentpb.BackupConnectorAuthority,
	encryption *agentpb.BackupEncryptionAuthority,
	fixedRevision int64,
	artifacts *[]*agentpb.ComposeArtifact,
) (*agentpb.BackupStepAuthority, error) {
	resourceKey := ""
	kind := agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_UNSPECIFIED
	switch attempt.Kind {
	case backupruntime.BackupRuntimeSourceAttach:
		resourceKey, kind = attachrecord.AttachKey(
			attempt.TargetID,
		), agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH
	case backupruntime.BackupRuntimeSourceConfig:
		resourceKey, kind = hierarchyrecord.EnvironmentKey(
			attempt.TargetID,
		), agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT
	case backupruntime.BackupRuntimeSourceVolume:
		// Volume desired identity is selected by this durable head. The full
		// immutable effective projection is independently pinned below.
		resourceKey, kind = blueprints.EnvironmentBlueprintHeadKey(
			run.EnvironmentID,
		), agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_VOLUME
	default:
		return nil, errs.New(errs.KindStrategyNotImplemented, "backup source strategy is not implemented")
	}
	read, err := repository.reader.ReadFixedKeys(ctx, []string{resourceKey}, fixedRevision)
	if err != nil {
		return nil, err
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[0].ModRevision != attempt.TargetRevision {
		return nil, errs.New(errs.KindStateConflict, "backup source resource snapshot changed")
	}
	capture := &agentpb.BackupCaptureAuthority{PointId: attempt.RecoveryPointID,
		Resource: &agentpb.BackupResourceIdentity{
			Kind:       kind,
			ResourceId: attempt.TargetID,
			Resource:   snapshotRevisionDigest(read.Values[0]),
		},
		Target: &agentpb.BackupObjectTarget{
			Connector: proto.CloneOf(connector),
			Bucket:    run.ConnectorBucket,
			ObjectKey: attempt.ObjectKey,
		},
		Encryption: proto.CloneOf(encryption)}
	authority := &agentpb.BackupStepAuthority{StepId: ids.New(ids.KindStep), ExecutionId: ids.NewULID(),
		StepDeadlineUnixNano: uint64(run.CreatedAt.Add(6 * time.Hour).UnixNano()),
		Operation:            &agentpb.BackupStepAuthority_Capture{Capture: capture}}
	switch attempt.Kind {
	case backupruntime.BackupRuntimeSourceConfig:
		if input.ResolveConfig == nil {
			return nil, errs.New(
				errs.KindStrategyNotImplemented,
				"backup Config requires a sealed fixed-revision Entry and selected-value snapshot producer",
			)
		}
		config, err := input.ResolveConfig(
			ctx,
			BackupConfigSnapshotInput{TaskID: input.TaskID, SnapshotID: attempt.Snapshot.Config.ConfigSnapshotID,
				EnvironmentID: run.EnvironmentID, SourceID: attempt.SourceID, ReadRevision: fixedRevision},
		)
		if err != nil {
			return nil, err
		}
		if config == nil || config.EnvironmentId != run.EnvironmentID ||
			config.MetadataSnapshotRevision != fixedRevision {
			return nil, errs.New(errs.KindStateConflict, "backup Config snapshot authority changed")
		}
		capture.Source = &agentpb.BackupCaptureAuthority_Config{Config: proto.CloneOf(config)}
	case backupruntime.BackupRuntimeSourceAttach:
		postgres := attempt.Snapshot.Postgres
		artifact, err := addBackupServiceFact(
			ctx,
			input.ResolveServiceFact,
			scope,
			postgres.BackingServiceID,
			postgres.BackingEnvironmentID,
			fixedRevision,
		)
		if err != nil {
			return nil, err
		}
		if err := appendBackupArtifact(artifacts, artifact); err != nil {
			return nil, err
		}
		release, err := postgres16protocol.DecodeManagedReleaseIndex([]byte(postgres.ManagedReleaseIndex))
		if err != nil {
			return nil, err
		}
		fact := backupScopeService(scope, postgres.BackingServiceID)
		var boundTools bool
		for _, workload := range artifact.Services {
			if workload.ServiceId == fact.GetServiceId() && workload.PostgresToolsImage == release.Image {
				boundTools = true
			}
		}
		if !boundTools {
			return nil, errs.New(errs.KindStateConflict, "PostgreSQL workload is not the managed backup release")
		}
		maximum := backupformat.MaxStoredBytes
		if encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
			maximum = backupformat.MaxAgeSourceBytes
		}
		capture.Source = &agentpb.BackupCaptureAuthority_Postgres{Postgres: &agentpb.BackupPostgresCaptureAuthority{
			AdapterContractVersion: postgres16protocol.AdapterContractVersion, DatabaseServiceId: postgres.BackingServiceID,
			DatabaseName: postgres.Database, RoleName: postgres.Role, MaxPlaintextBytes: maximum,
			ManagedReleaseIndex: []byte(postgres.ManagedReleaseIndex)}}
	case backupruntime.BackupRuntimeSourceVolume:
		projection, artifact, err := repository.manualBackupVolumeArtifact(ctx, attempt.Snapshot.Volume, fixedRevision)
		if err != nil {
			return nil, err
		}
		found := false
		for _, existing := range *artifacts {
			if existing.ArtifactId == artifact.ArtifactId {
				if !proto.Equal(existing, artifact) {
					return nil, errs.New(errs.KindStateConflict, "Volume Compose artifact identity changed")
				}
				found = true
				break
			}
		}
		if !found {
			*artifacts = append(*artifacts, artifact)
		}
		maximum := backupformat.MaxStoredBytes
		if encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
			maximum = backupformat.MaxAgeSourceBytes
		}
		capture.Source = &agentpb.BackupCaptureAuthority_Volume{Volume: &agentpb.BackupVolumeCaptureAuthority{
			VolumeId: attempt.TargetID, Volume: proto.CloneOf(capture.Resource.Resource), SourceSizeUpperBound: maximum, Projection: projection}}
		for _, consumer := range attempt.Snapshot.Volume.Services {
			consumerArtifact, err := addBackupServiceFact(
				ctx,
				input.ResolveServiceFact,
				scope,
				consumer.ServiceID,
				run.EnvironmentID,
				fixedRevision,
			)
			if err != nil {
				return nil, err
			}
			if err := appendBackupArtifact(artifacts, consumerArtifact); err != nil {
				return nil, err
			}
			authority.ConsumerServiceIds = append(authority.ConsumerServiceIds, consumer.ServiceID)
		}
	}
	return executionplan.SealBackupStepAuthority(authority)
}

func addBackupServiceFact(ctx context.Context, resolve BackupServiceFactResolver, scope *agentpb.BackupPlanScope,
	serviceID, environmentID string, fixedRevision int64) (*agentpb.ComposeArtifact, error) {
	if resolve == nil {
		return nil, errs.New(
			errs.KindStrategyNotImplemented,
			"backup requires sealed Service desired, Compose, runtime, label and repository evidence",
		)
	}
	evidence, err := resolve(
		ctx,
		BackupServiceFactInput{ServiceID: serviceID, EnvironmentID: environmentID, ReadRevision: fixedRevision},
	)
	if err != nil {
		return nil, err
	}
	if evidence == nil || evidence.Fact == nil || evidence.Fact.ServiceId != serviceID ||
		evidence.Artifact == nil || evidence.Artifact.OwnerId != environmentID {
		return nil, errs.New(errs.KindStateConflict, "backup Service fact or artifact identity changed")
	}
	fact := evidence.Fact
	for _, existing := range scope.Services {
		if existing.ServiceId == serviceID {
			if !proto.Equal(existing, fact) {
				return nil, errs.New(errs.KindStateConflict, "backup Service facts differ at the selected view")
			}
			return proto.CloneOf(evidence.Artifact), nil
		}
	}
	scope.Services = append(scope.Services, proto.CloneOf(fact))
	slices.SortFunc(
		scope.Services,
		func(left, right *agentpb.BackupServiceFact) int { return cmp.Compare(left.ServiceId, right.ServiceId) },
	)
	return proto.CloneOf(evidence.Artifact), nil
}

func appendBackupArtifact(artifacts *[]*agentpb.ComposeArtifact, artifact *agentpb.ComposeArtifact) error {
	for _, existing := range *artifacts {
		if existing.ArtifactId == artifact.ArtifactId {
			if !proto.Equal(existing, artifact) {
				return errs.New(errs.KindStateConflict, "Backup Compose artifact identity changed")
			}
			return nil
		}
	}
	*artifacts = append(*artifacts, proto.CloneOf(artifact))
	return nil
}

func (repository *Planner) manualBackupVolumeArtifact(
	ctx context.Context,
	snapshot *backupruntime.BackupVolumeSourceSnapshot,
	fixedRevision int64,
) (*agentpb.BackupVolumeProjectionAuthority, *agentpb.ComposeArtifact, error) {
	read, err := repository.reader.ReadFixedKeys(
		ctx,
		[]string{
			blueprints.EnvironmentBlueprintEffectiveProjectionKey(snapshot.EnvironmentID, snapshot.DesiredRevisionID),
		},
		fixedRevision,
	)
	if err != nil {
		return nil, nil, err
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, nil, errs.New(errs.KindStateConflict, "backup Volume effective projection is unavailable")
	}
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[0].Value)
	if err != nil || projection.EnvironmentID != snapshot.EnvironmentID ||
		projection.RevisionID != snapshot.DesiredRevisionID ||
		projection.RenderGeneration != snapshot.RenderGeneration {
		return nil, nil, errs.New(errs.KindStateConflict, "backup Volume effective projection changed")
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		return nil, nil, errs.Wrap(errs.KindInternal, err)
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
	if err != nil || !bytes.Equal(canonical, projection.ComposeArtifact) {
		return nil, nil, errs.New(errs.KindStateConflict, "backup Volume effective artifact encoding changed")
	}
	digest := sha256.Sum256(projection.ComposeArtifact)
	snapshot.ArtifactID, snapshot.ArtifactDigest, snapshot.ArtifactRevision = artifact.ArtifactId, hex.EncodeToString(
		digest[:],
	), read.Values[0].ModRevision
	return &agentpb.BackupVolumeProjectionAuthority{
		ArtifactId:          artifact.ArtifactId,
		ArtifactSha256:      digest[:],
		ArtifactRevision:    read.Values[0].ModRevision,
		ProjectionRoot:      snapshot.ProjectionRoot,
		RenderGeneration:    snapshot.RenderGeneration,
		ComposeVolumeKey:    snapshot.ComposeVolumeKey,
		DockerVolumeName:    snapshot.DockerVolumeName,
		AuthorizedVolumeDir: snapshot.AuthorizedVolumeDir,
	}, artifact, nil
}
