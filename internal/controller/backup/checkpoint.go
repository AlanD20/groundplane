package backup

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type BackupCheckpointRepository interface {
	VolumeManifestRepository() *backupvolumemanifest.Repository
	configRestorePublicationStore
	SealConfigRestoreGeneration(context.Context, backupconfiguration.ConfigRestoreGenerationRecord) (int64, error)
	CommitConfigRestoreTransfer(
		context.Context,
		backupconfiguration.ConfigRestoreTransferBatch,
		*agentpb.BackupConfigCredit,
	) (int64, error)
	VisitConfigRestoreTransferRecords(
		context.Context,
		backupconfiguration.ConfigRestoreTransferOwner,
		etcdstore.Versioned[backupconfiguration.ConfigTransferCursor],
		uint64,
		uint64,
		func(backupconfiguration.ConfigRestoreTransferRecord) error,
	) error
	ReadConfigTransferCursor(
		context.Context,
		string,
		string,
		string,
		string,
		int64,
	) (etcdstore.Versioned[backupconfiguration.ConfigTransferCursor], bool, error)
	VisitConfigTransferCredits(
		context.Context,
		etcdstore.Versioned[backupconfiguration.ConfigTransferCursor],
		func(*agentpb.BackupConfigCredit) error,
	) error
	CommitConfigTransferCredit(
		context.Context,
		backupconfiguration.ConfigTransferOwner,
		*agentpb.BackupConfigCredit,
	) (int64, error)
	GetBackupExecutionPlan(context.Context, string) (etcdstore.Versioned[*agentpb.ExecutionPlan], error)
	GetBackupRun(context.Context, string) (etcdstore.Versioned[backupruntime.BackupRunRecord], error)
	GetBackupRestore(context.Context, string) (etcdstore.Versioned[backupruntime.BackupRestoreRecord], error)
	CheckpointConfigRestore(context.Context, backupruntime.BackupCheckpointInput,
		etcdstore.Versioned[backupruntime.BackupRestoreRecord], *etcd.ConfigRestoreCheckpointPublication, time.Time) (int64, error)
	CheckpointVolumeRestore(context.Context, backupruntime.BackupCheckpointInput,
		etcdstore.Versioned[backupruntime.BackupRestoreRecord], *backupruntime.BackupVolumeOldManifestProof, time.Time) (int64, error)
	CheckpointPostgresRestore(context.Context, backupruntime.BackupCheckpointInput,
		etcdstore.Versioned[backupruntime.BackupRestoreRecord], time.Time) (int64, error)
	PublishConfigRestore(context.Context, etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord],
		environmentprojection.EnvironmentComposeProjection, environmentprojection.EnvironmentOwnedIdentities) error
	GetBackupCheckpointAssignment(
		context.Context,
		string,
	) (etcdstore.Versioned[taskassignments.TaskAssignmentRecord], error)
	ReplayBackupCheckpoint(context.Context, backupruntime.BackupCheckpointInput) (int64, bool, error)
	FinalizeBackupCapture(context.Context, backupruntime.BackupCheckpointInput) error
	CheckpointBackupPrune(context.Context, backupruntime.BackupCheckpointInput, time.Time) (int64, error)
	VisitBackupStepCheckpoints(
		context.Context,
		*agentpb.BackupTaskAuthority,
		*agentpb.BackupStepAuthority,
		int64,
		func(backupruntime.BackupCommittedCheckpoint) error,
	) error
	CheckpointBackupRun(context.Context, backupruntime.BackupCheckpointInput,
		etcdstore.Versioned[backupruntime.BackupRunRecord], backupruntime.BackupRunRecord) (etcdstore.Versioned[backupruntime.BackupRunRecord], error)
	CheckpointBackupConfigCapture(context.Context, backupruntime.BackupCheckpointInput,
		etcdstore.Versioned[backupruntime.BackupRunRecord], backupruntime.BackupRunRecord,
		etcdstore.Versioned[backupconfiguration.ConfigTransferCursor]) (etcdstore.Versioned[backupruntime.BackupRunRecord], error)
	CheckpointBackupVolumeCapture(context.Context, backupruntime.BackupCheckpointInput,
		etcdstore.Versioned[backupruntime.BackupRunRecord], backupruntime.BackupRunRecord,
		etcdstore.Versioned[backupvolumemanifest.Cursor]) (etcdstore.Versioned[backupruntime.BackupRunRecord], error)
	GetBackupOrphan(context.Context, string) (etcdstore.Versioned[backupruntime.BackupOrphanRecord], bool, error)
	CommitBackupRecoveryPoint(context.Context, backupruntime.BackupCheckpointInput,
		etcdstore.Versioned[backupruntime.BackupRunRecord], backupruntime.BackupRunRecord, uint32,
		backupruntime.BackupRecoveryPointRecord, *etcdstore.Versioned[backupruntime.BackupOrphanRecord],
		backupruntime.BackupRetentionSweepRecord) (etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord], etcdstore.Versioned[backupruntime.BackupRunRecord], error)
}

type BackupCheckpointService struct {
	repository      BackupCheckpointRepository
	configSnapshots *ConfigSnapshotProducer
	configRestores  *ConfigRestoreProducer
	now             func() time.Time
}

func NewBackupCheckpointService(
	repository BackupCheckpointRepository,
	store etcdstore.Store, protector *secretvalue.Protector,
) (*BackupCheckpointService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "Backup checkpoint repository is required")
	}
	configSnapshots, err := NewConfigSnapshotProducer(store, protector)
	if err != nil {
		return nil, err
	}
	configRestores, err := NewConfigRestoreProducer(store, repository, protector)
	if err != nil {
		return nil, err
	}
	return &BackupCheckpointService{
		repository:      repository,
		configSnapshots: configSnapshots,
		configRestores:  configRestores,
		now:             time.Now,
	}, nil
}

func (service *BackupCheckpointService) CheckpointBackup(ctx context.Context, agentID string, agentGeneration uint64,
	request *agentpb.BackupCheckpointRequest) (*agentpb.BackupCheckpointAck, error) {
	if ctx == nil || service == nil || service.repository == nil || service.now == nil {
		return nil, errs.New(errs.KindInternal, "Backup checkpoint service is not configured")
	}
	owned, err := executionplan.ValidateBackupCheckpointRequest(request, request.GetCheckpointSequence())
	if err != nil {
		return nil, err
	}
	claim, err := service.repository.GetBackupCheckpointAssignment(ctx, owned.TaskId)
	if err != nil {
		return nil, err
	}
	assignment := claim.Record
	authority := assignment.BackupAuthorityFence
	if claim.Revision <= 0 || authority == nil || assignment.AssignmentID != owned.AssignmentId ||
		assignment.AgentID != agentID || assignment.AgentGeneration != agentGeneration ||
		!authority.MatchesCheckpoint(
			authority.AssignmentGeneration,
			owned.StepId,
			owned.ExecutionId,
			hex.EncodeToString(owned.AuthorityDigest),
		) {
		return nil, errs.New(errs.KindStateConflict, "Backup checkpoint assignment authority changed")
	}
	input := backupruntime.BackupCheckpointInput{
		TaskID: owned.TaskId, AssignmentID: owned.AssignmentId, AgentID: agentID, AgentGeneration: agentGeneration,
		StepID: owned.StepId, ExecutionID: owned.ExecutionId, Sequence: owned.CheckpointSequence,
		AssignmentGeneration: authority.AssignmentGeneration, AuthoritySHA256: hex.EncodeToString(owned.AuthorityDigest),
		PrecedingCheckpointRevision: owned.GetPrecedingCheckpoint().GetDedupeKeyModRevision(), Request: owned,
	}
	replayRevision, replay, err := service.repository.ReplayBackupCheckpoint(ctx, input)
	if err != nil {
		return nil, err
	}
	if replay {
		if owned.GetSourceCleanupCompleted() != nil {
			sealed, err := service.repository.GetBackupExecutionPlan(ctx, owned.TaskId)
			if err != nil {
				return nil, err
			}
			if sealed.Record.Operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP {
				if err := service.repository.FinalizeBackupCapture(ctx, input); err != nil {
					return nil, err
				}
			}
		}
		if owned.GetVolume() != nil || owned.GetRestoreArtifactValidated().GetVolume() != nil {
			sealed, err := service.repository.GetBackupExecutionPlan(ctx, owned.TaskId)
			if err != nil {
				return nil, err
			}
			for _, step := range sealed.Record.Steps {
				if step.StepId == owned.StepId && step.GetBackupStep().GetRestore().GetVolume() != nil {
					return service.checkpointVolumeRestore(ctx, input, step.GetBackupStep(), replayRevision)
				}
			}
		}
		if owned.GetConfig().GetTransferCompleted() != nil {
			sealed, err := service.repository.GetBackupExecutionPlan(ctx, owned.TaskId)
			if err != nil {
				return nil, err
			}
			if sealed.Record.Operation == agentpb.PlanOperation_PLAN_OPERATION_RESTORE {
				for _, step := range sealed.Record.Steps {
					if step.StepId == owned.StepId && step.GetBackupStep().ExecutionId == owned.ExecutionId {
						return service.checkpointConfigRestore(ctx, input, step.GetBackupStep(), replayRevision)
					}
				}
				return nil, configSnapshotGuardConflict()
			}
		}
		return backupCheckpointAcknowledgement(owned, replayRevision)
	}
	if owned.GetPruneObjectDeleted() != nil {
		revision, err := service.repository.CheckpointBackupPrune(ctx, input, service.now().UTC())
		if err != nil {
			return nil, err
		}
		return backupCheckpointAcknowledgement(owned, revision)
	}
	sealed, err := service.repository.GetBackupExecutionPlan(ctx, owned.TaskId)
	if err != nil {
		return nil, err
	}
	if sealed.Record.Operation == agentpb.PlanOperation_PLAN_OPERATION_RESTORE {
		for _, step := range sealed.Record.Steps {
			if step.StepId == owned.StepId && step.GetBackupStep().ExecutionId == owned.ExecutionId {
				if step.GetBackupStep().GetRestore().GetPostgres() != nil {
					return service.checkpointPostgresRestore(ctx, input, claim, sealed.Record)
				}
				if step.GetBackupStep().GetRestore().GetVolume() != nil {
					return service.checkpointVolumeRestore(ctx, input, step.GetBackupStep(), 0)
				}
				return service.checkpointConfigRestore(ctx, input, step.GetBackupStep(), 0)
			}
		}
		return nil, configSnapshotGuardConflict()
	}
	current, err := service.repository.GetBackupRun(ctx, owned.TaskId)
	if err != nil {
		return nil, err
	}
	ordinal := -1
	for index, step := range authority.Steps {
		if step.StepID == owned.StepId {
			ordinal = index
			break
		}
	}
	if ordinal < 0 || ordinal >= len(current.Record.Sources) {
		return nil, errs.New(errs.KindValidationFailed, "Backup checkpoint source binding is invalid")
	}
	if current.Record.Sources[ordinal].Kind == backupruntime.BackupRuntimeSourceAttach {
		if err := service.validatePostgresCheckpointAdvance(ctx, claim, sealed.Record,
			current.Record.OperationID, current.ReadRevision, owned); err != nil {
			return nil, err
		}
	}
	next := backupruntime.CloneBackupRunPublicationRecord(current.Record)
	var configCursor etcdstore.Versioned[backupconfiguration.ConfigTransferCursor]
	var volumeCursor etcdstore.Versioned[backupvolumemanifest.Cursor]
	if owned.GetConfig().GetTransferCompleted() != nil {
		configCursor, err = service.validateConfigCaptureCompletion(
			ctx,
			agentID,
			agentGeneration,
			claim,
			current,
			ordinal,
			owned,
		)
		if err != nil {
			return nil, err
		}
	}
	if owned.GetArtifactPrepared().GetVolume() != nil {
		volumeCursor, err = service.validateVolumeCaptureCompletion(ctx, input, current, ordinal)
		if err != nil {
			return nil, err
		}
	}
	if err := applyBackupCheckpointTransition(&next.Sources[ordinal], owned, current.Record); err != nil {
		return nil, err
	}
	if owned.GetArtifactPrepared().GetVolume() != nil {
		next.Sources[ordinal].VolumeArchive.Manifest = volumeManifestReference(volumeCursor)
	}
	next.UpdatedAt = service.now().UTC()
	if !next.UpdatedAt.After(current.Record.UpdatedAt) {
		next.UpdatedAt = current.Record.UpdatedAt.Add(time.Nanosecond)
	}
	if owned.GetSourceCleanupCompleted() != nil {
		return service.commitBackupPoint(ctx, input, owned, current, next, uint32(ordinal))
	}
	if owned.GetConfig().GetTransferCompleted() != nil {
		updated, err := service.repository.CheckpointBackupConfigCapture(ctx, input, current, next, configCursor)
		if err != nil {
			return nil, err
		}
		return backupCheckpointAcknowledgement(owned, updated.Revision)
	}
	if owned.GetArtifactPrepared().GetVolume() != nil {
		updated, err := service.repository.CheckpointBackupVolumeCapture(ctx, input, current, next, volumeCursor)
		if err != nil {
			return nil, err
		}
		return backupCheckpointAcknowledgement(owned, updated.Revision)
	}
	updated, err := service.repository.CheckpointBackupRun(ctx, input, current, next)
	if err != nil {
		return nil, err
	}
	return backupCheckpointAcknowledgement(owned, updated.Revision)
}

func backupCheckpointAcknowledgement(
	owned *agentpb.BackupCheckpointRequest,
	revision int64,
) (*agentpb.BackupCheckpointAck, error) {
	acknowledgement := &agentpb.BackupCheckpointAck{
		TaskId: owned.TaskId, AssignmentId: owned.AssignmentId, StepId: owned.StepId, ExecutionId: owned.ExecutionId,
		CheckpointSequence: owned.CheckpointSequence, Committed: &agentpb.CheckpointFence{
			AuthorityDigest: append([]byte(nil), owned.AuthorityDigest...), DedupeKeyModRevision: revision,
		},
	}
	return executionplan.ValidateBackupCheckpointAck(acknowledgement, owned)
}

func applyBackupCheckpointTransition(source *backupruntime.BackupRunSourceAttemptRecord,
	request *agentpb.BackupCheckpointRequest, run backupruntime.BackupRunRecord) error {
	if source == nil {
		return errs.New(errs.KindInternal, "Backup checkpoint source is missing")
	}
	if pointID := backupruntime.BackupCheckpointPointID(request); pointID != "" && pointID != source.RecoveryPointID {
		return errs.New(errs.KindValidationFailed, "Backup checkpoint point does not match its source")
	}
	switch checkpoint := request.Checkpoint.(type) {
	case *agentpb.BackupCheckpointRequest_PostgresContainerObserved:
		if source.Kind != backupruntime.BackupRuntimeSourceAttach || source.State != backupruntime.BackupSourceAttemptPending {
			return errs.New(errs.KindStateConflict, "PostgreSQL capture observation is out of order")
		}
		source.State = backupruntime.BackupSourceAttemptCapturing
	case *agentpb.BackupCheckpointRequest_PostgresDumpStart:
		if source.Kind != backupruntime.BackupRuntimeSourceAttach || source.State != backupruntime.BackupSourceAttemptCapturing {
			return errs.New(errs.KindStateConflict, "PostgreSQL dump start is out of order")
		}
		source.State, source.Phase = backupruntime.BackupSourceAttemptReady, backupruntime.BackupSourcePhaseStaging
	case *agentpb.BackupCheckpointRequest_ArtifactPrepared:
		if checkpoint.ArtifactPrepared.GetVolume() != nil {
			archive, err := backupruntime.VolumeArchiveEvidenceFromWire(checkpoint.ArtifactPrepared.GetVolume())
			if err != nil {
				return err
			}
			source.VolumeArchive = archive
		}
		source.Evidence = checkpointEvidence(checkpoint.ArtifactPrepared.Evidence)
		source.Upload = backupruntime.BackupUploadOutcome{Kind: backupruntime.BackupUploadPrepared,
			Target: backupruntime.BackupObjectTarget{ConnectorID: run.ConnectorID, ConnectorPrefix: run.ConnectorPrefix,
				ConnectorEndpoint: run.ConnectorEndpoint, ConnectorBucket: run.ConnectorBucket,
				ConnectorRegion: run.ConnectorRegion, ConnectorPathStyle: run.ConnectorPathStyle, ObjectKey: source.ObjectKey}}
		source.State, source.Phase = backupruntime.BackupSourceAttemptStaged, backupruntime.BackupSourcePhaseUpload
	case *agentpb.BackupCheckpointRequest_UploadCompleted:
		completed := checkpoint.UploadCompleted
		source.Upload.Target = checkpointTarget(completed.Target)
		switch outcome := completed.Outcome.(type) {
		case *agentpb.BackupUploadCompleted_ReturnedObject:
			source.Upload.Kind = backupruntime.BackupUploadReturned
			source.Upload.ReturnedObject = checkpointObject(outcome.ReturnedObject)
		case *agentpb.BackupUploadCompleted_Unknown:
			source.Upload.Kind = backupruntime.BackupUploadUnknown
			source.Upload.ReturnedObject = backupruntime.BackupObjectIdentity{}
		}
		source.Phase = backupruntime.BackupSourcePhaseHeadVerification
	case *agentpb.BackupCheckpointRequest_UploadVerified:
		source.Object = checkpointObject(checkpoint.UploadVerified.Object)
		source.Phase = backupruntime.BackupSourcePhasePointCommit
	case *agentpb.BackupCheckpointRequest_SourceCleanupCompleted:
		source.State, source.Phase = backupruntime.BackupSourceAttemptPointCommitted, backupruntime.BackupSourcePhaseRetention
	case *agentpb.BackupCheckpointRequest_Config:
		if checkpoint.Config.GetTransferCompleted() == nil {
			return errs.New(errs.KindNotImplemented, "Backup Config checkpoint requires its progress publisher")
		}
		archive, err := backupruntime.ConfigArchiveEvidenceFromCapture(checkpoint.Config.GetTransferCompleted())
		if err != nil {
			return err
		}
		source.ConfigArchive = archive
		source.State, source.Phase = backupruntime.BackupSourceAttemptReady, backupruntime.BackupSourcePhaseStaging
	default:
		return errs.New(errs.KindNotImplemented, "Backup checkpoint requires its source-specific progress publisher")
	}
	return nil
}

func checkpointEvidence(value *agentpb.BackupArtifactEvidence) backupruntime.BackupArtifactEvidence {
	return backupruntime.BackupArtifactEvidence{SourceSizeBytes: value.SourceSizeBytes,
		SourceSHA256: hex.EncodeToString(value.SourceSha256), StoredSizeBytes: value.StoredSizeBytes,
		StoredSHA256: hex.EncodeToString(value.StoredSha256)}
}

func checkpointTarget(value *agentpb.BackupObjectTarget) backupruntime.BackupObjectTarget {
	return backupruntime.BackupObjectTarget{
		ConnectorID:        value.Connector.ConnectorId,
		ConnectorPrefix:    value.Connector.Prefix,
		ConnectorEndpoint:  value.Connector.CanonicalEndpointUrl,
		ConnectorBucket:    value.Bucket,
		ConnectorRegion:    value.Connector.Region,
		ConnectorPathStyle: value.Connector.GetPathStyle(),
		ObjectKey:          value.ObjectKey,
	}
}

func checkpointObject(value *agentpb.BackupObjectIdentity) backupruntime.BackupObjectIdentity {
	result := backupruntime.BackupObjectIdentity{Target: checkpointTarget(&agentpb.BackupObjectTarget{
		Connector: value.Connector, Bucket: value.Bucket, ObjectKey: value.ObjectKey})}
	switch discriminator := value.Discriminator.(type) {
	case *agentpb.BackupObjectIdentity_VersionId:
		result.Discriminator = backupruntime.BackupObjectDiscriminator{Kind: backupobject.DiscriminatorVersionID, Value: discriminator.VersionId.Value}
	case *agentpb.BackupObjectIdentity_Etag:
		result.Discriminator = backupruntime.BackupObjectDiscriminator{Kind: backupobject.DiscriminatorETag, Value: discriminator.Etag.Value}
	}
	return result
}
