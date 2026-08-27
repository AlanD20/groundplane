package controller

import (
	"context"
	"encoding/hex"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type BackupCheckpointRepository interface {
	GetBackupRun(context.Context, string) (etcd.Versioned[etcd.BackupRunRecord], error)
	CheckpointBackupRun(
		context.Context,
		etcd.BackupCheckpointInput,
		etcd.Versioned[etcd.BackupRunRecord],
		etcd.BackupRunRecord,
	) (etcd.Versioned[etcd.BackupRunRecord], error)
}

type BackupCheckpointService struct {
	repository BackupCheckpointRepository
	now        func() time.Time
}

func NewBackupCheckpointService(repository BackupCheckpointRepository) (*BackupCheckpointService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "Backup checkpoint repository is required")
	}
	return &BackupCheckpointService{repository: repository, now: time.Now}, nil
}

func (service *BackupCheckpointService) CheckpointBackup(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	request *agentpb.BackupCheckpointRequest,
) (*agentpb.BackupCheckpointAck, error) {
	if ctx == nil || service == nil || service.repository == nil || service.now == nil {
		return nil, errs.New(errs.KindInternal, "Backup checkpoint service is not configured")
	}
	if err := executionplan.ValidateBackupCheckpointRequest(request, request.GetSequence()); err != nil {
		return nil, err
	}
	payload, err := backupCheckpointPayload(request)
	if err != nil {
		return nil, err
	}
	current, err := service.repository.GetBackupRun(ctx, request.GetTaskId())
	if err != nil {
		return nil, err
	}
	next := current.Record
	next.Sources = append([]etcd.BackupRunSourceAttemptRecord(nil), current.Record.Sources...)
	ordinal := -1
	for index := range next.Sources {
		if next.Sources[index].RecoveryPointID == payload.PointID {
			ordinal = index
			break
		}
	}
	if ordinal < 0 {
		return nil, errs.New(errs.KindValidationFailed, "Backup checkpoint recovery point is not in the run")
	}
	if err := applyBackupCheckpointTransition(&next.Sources[ordinal], payload); err != nil {
		return nil, err
	}
	next.UpdatedAt = service.now().UTC()
	if !next.UpdatedAt.After(current.Record.UpdatedAt) {
		next.UpdatedAt = current.Record.UpdatedAt.Add(time.Nanosecond)
	}
	_, err = service.repository.CheckpointBackupRun(ctx, etcd.BackupCheckpointInput{
		TaskID:          request.GetTaskId(),
		AssignmentID:    request.GetAssignmentId(),
		AgentID:         agentID,
		AgentGeneration: agentGeneration,
		StepID:          request.GetStepId(),
		Sequence:        request.GetSequence(),
		Payload:         payload,
	}, current, next)
	if err != nil {
		return nil, err
	}
	return &agentpb.BackupCheckpointAck{
		TaskId:       request.GetTaskId(),
		AssignmentId: request.GetAssignmentId(),
		StepId:       request.GetStepId(),
		Sequence:     request.GetSequence(),
	}, nil
}

func backupCheckpointPayload(request *agentpb.BackupCheckpointRequest) (etcd.BackupCheckpointPayload, error) {
	payload := request.GetPayload()
	result := etcd.BackupCheckpointPayload{
		PointID:         payload.GetPointId(),
		StoredSizeBytes: payload.GetStoredSizeBytes(),
		StoredSHA256:    hex.EncodeToString(payload.GetStoredSha256()),
	}
	switch request.GetKind() {
	case agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_ARTIFACT_PREPARED:
		result.Kind = etcd.BackupCheckpointArtifactPrepared
	case agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_COMPLETED:
		result.Kind = etcd.BackupCheckpointUploadCompleted
	case agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_VERIFIED:
		result.Kind = etcd.BackupCheckpointUploadVerified
	case agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_SOURCE_CLEANUP_COMPLETED:
		result.Kind = etcd.BackupCheckpointSourceCleanupCompleted
	default:
		return etcd.BackupCheckpointPayload{}, errs.New(
			errs.KindNotImplemented,
			"Backup checkpoint kind does not belong to a Backup run",
		)
	}
	return result, nil
}

func applyBackupCheckpointTransition(
	source *etcd.BackupRunSourceAttemptRecord,
	payload etcd.BackupCheckpointPayload,
) error {
	if source == nil {
		return errs.New(errs.KindInternal, "Backup checkpoint source is missing")
	}
	switch payload.Kind {
	case etcd.BackupCheckpointArtifactPrepared:
		if payload.StoredSizeBytes > math.MaxInt64 {
			return errs.New(errs.KindValidationFailed, "Backup artifact size exceeds the durable range")
		}
		source.State = etcd.BackupSourceAttemptStaged
		source.Phase = etcd.BackupSourcePhaseUpload
		source.SizeBytes = int64(payload.StoredSizeBytes)
		source.SHA256 = payload.StoredSHA256
	case etcd.BackupCheckpointUploadCompleted:
		source.Phase = etcd.BackupSourcePhaseHeadVerification
	case etcd.BackupCheckpointUploadVerified:
		source.Phase = etcd.BackupSourcePhasePointCommit
	case etcd.BackupCheckpointSourceCleanupCompleted:
		source.State = etcd.BackupSourceAttemptSucceeded
	default:
		return errs.New(errs.KindNotImplemented, "Backup checkpoint transition is not implemented")
	}
	return nil
}
