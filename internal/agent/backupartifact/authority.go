// Package backupartifact transfers exact sealed Backup objects. It does not
// publish Recovery Points, mutate restore targets or clean staging state.
package backupartifact

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Authority owns the exact task, step, capture, and Environment identity used
// to derive the provider-neutral object expectation. It is constructed only
// from sealed schema-one authority.
type Authority struct {
	task    *agentpb.BackupTaskAuthority
	step    *agentpb.BackupStepAuthority
	capture *agentpb.BackupCaptureAuthority
}

// NewAuthority binds a capture to its sealed task and step. The returned value
// owns copies, so callers cannot change upload authority after validation.
func NewAuthority(
	task *agentpb.BackupTaskAuthority,
	step *agentpb.BackupStepAuthority,
	capture *agentpb.BackupCaptureAuthority,
) (*Authority, error) {
	if task == nil || step == nil || capture == nil {
		return nil, invalidAuthority()
	}
	sealedTask, sealedStep, err := bindStep(task, step)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(sealedStep.GetCapture(), capture) {
		return nil, invalidAuthority()
	}
	return &Authority{
		task:    sealedTask,
		step:    sealedStep,
		capture: proto.Clone(capture).(*agentpb.BackupCaptureAuthority),
	}, nil
}

func bindStep(task *agentpb.BackupTaskAuthority, step *agentpb.BackupStepAuthority) (
	*agentpb.BackupTaskAuthority, *agentpb.BackupStepAuthority, error,
) {
	if task == nil || step == nil {
		return nil, nil, invalidAuthority()
	}
	if _, err := executionplan.BackupTaskAuthorityDigest(task); err != nil {
		return nil, nil, err
	}
	sealedStep, err := executionplan.ValidateBackupStepAuthority(step)
	if err != nil {
		return nil, nil, err
	}
	for _, candidate := range task.Steps {
		if proto.Equal(candidate, sealedStep) {
			return proto.Clone(task).(*agentpb.BackupTaskAuthority), sealedStep, nil
		}
	}
	return nil, nil, invalidAuthority()
}

func (authority *Authority) artifact(prepared *agentpb.BackupArtifactPrepared) (backupobject.Artifact, error) {
	if authority == nil || authority.task == nil || authority.step == nil || authority.capture == nil ||
		prepared == nil {
		return backupobject.Artifact{}, invalidAuthority()
	}
	validated, err := authority.validateCheckpoint(&agentpb.BackupCheckpointRequest{
		Checkpoint: &agentpb.BackupCheckpointRequest_ArtifactPrepared{ArtifactPrepared: prepared},
	})
	if err != nil {
		return backupobject.Artifact{}, err
	}
	prepared = validated.GetArtifactPrepared()
	if prepared.PointId != authority.capture.PointId || !preparedSourceMatches(prepared, authority.capture) {
		return backupobject.Artifact{}, invalidAuthority()
	}
	target := authority.capture.Target
	connector := target.Connector
	parts := strings.Split(strings.TrimPrefix(target.ObjectKey, connector.Prefix), "/")
	if len(parts) != 4 || parts[0] != authority.task.EnvironmentId {
		return backupobject.Artifact{}, invalidAuthority()
	}

	artifact := backupobject.Artifact{
		Key:             target.ObjectKey,
		EnvironmentID:   authority.task.EnvironmentId,
		SourceID:        parts[1],
		RecoveryPointID: authority.capture.PointId,
		Evidence: backupobject.Evidence{
			SourceSizeBytes: prepared.Evidence.SourceSizeBytes,
			StoredSizeBytes: prepared.Evidence.StoredSizeBytes,
		},
	}
	copy(artifact.Evidence.SourceSHA256[:], prepared.Evidence.SourceSha256)
	copy(artifact.Evidence.StoredSHA256[:], prepared.Evidence.StoredSha256)
	switch {
	case authority.capture.GetPostgres() != nil:
		artifact.SourceFormat = backupobject.SourceFormatPostgresCustom
	case authority.capture.GetConfig() != nil:
		artifact.SourceFormat = backupobject.SourceFormatEnvironmentConfig
	case authority.capture.GetVolume() != nil:
		artifact.SourceFormat = backupobject.SourceFormatVolumeTar
	default:
		return backupobject.Artifact{}, invalidAuthority()
	}
	switch authority.capture.Encryption.Kind {
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE:
		if authority.capture.Encryption.KeyEra != nil {
			return backupobject.Artifact{}, invalidAuthority()
		}
		artifact.Encryption = backupobject.EncryptionNone
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE:
		if authority.capture.Encryption.KeyEra == nil || authority.capture.Encryption.GetKeyEra() == 0 {
			return backupobject.Artifact{}, invalidAuthority()
		}
		era := authority.capture.Encryption.GetKeyEra()
		artifact.Encryption, artifact.KeyEra = backupobject.EncryptionAge, &era
	default:
		return backupobject.Artifact{}, invalidAuthority()
	}
	if artifact.ExpectedKey(connector.Prefix) != artifact.Key {
		return backupobject.Artifact{}, invalidAuthority()
	}
	if err := artifact.Validate(); err != nil {
		return backupobject.Artifact{}, err
	}
	return artifact, nil
}

func (authority *Authority) validateCheckpoint(
	request *agentpb.BackupCheckpointRequest,
) (*agentpb.BackupCheckpointRequest, error) {
	if request == nil {
		return nil, invalidAuthority()
	}
	request = proto.Clone(request).(*agentpb.BackupCheckpointRequest)
	request.TaskId, request.AssignmentId = authority.task.TaskId, authority.task.AssignmentId
	request.StepId, request.ExecutionId = authority.step.StepId, authority.step.ExecutionId
	request.CheckpointSequence = 1
	request.AuthorityDigest = append([]byte(nil), authority.step.StepDigest...)
	return executionplan.ValidateBackupCheckpointRequest(request, 1)
}

func preparedSourceMatches(prepared *agentpb.BackupArtifactPrepared, capture *agentpb.BackupCaptureAuthority) bool {
	return prepared.GetPostgres() != nil && capture.GetPostgres() != nil ||
		prepared.GetConfig() != nil && capture.GetConfig() != nil ||
		prepared.GetVolume() != nil && capture.GetVolume() != nil
}

func invalidAuthority() error {
	return errs.New(errs.KindStateConflict, "backup artifact upload differs from sealed capture authority")
}
