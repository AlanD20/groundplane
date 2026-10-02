package agent

import (
	"bytes"
	"context"
	"math"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (state *backupStagingState) volumeCaptureStage(ctx context.Context, taskID string,
	step *agentpb.BackupStepAuthority,
) (*backupstage.Stage, *backupstage.Artifact, backupstage.ArtifactEvidence, *backupstage.Artifact, error) {
	volume := step.GetCapture().GetVolume()
	if state == nil || !state.ready || state.stager == nil || volume == nil {
		return nil, nil, backupstage.ArtifactEvidence{}, nil, invalidAgentStaging()
	}
	ids := backupstage.IDs{Task: taskID, Step: step.StepId, Point: step.GetCapture().PointId}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.cleaned[ids] || state.active[ids] != nil {
		return nil, nil, backupstage.ArtifactEvidence{}, nil, invalidAgentStaging()
	}
	for _, prepared := range state.prepared {
		if prepared.IDs != ids {
			continue
		}
		var source, stored *backupstage.Artifact
		var sourceEvidence backupstage.ArtifactEvidence
		for _, file := range prepared.Files {
			switch backupRecoveredFileRole(file.Evidence.Name) {
			case agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT:
				source, sourceEvidence = file.Artifact, file.Evidence
			case agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT:
				stored = file.Artifact
			}
		}
		if source == nil {
			return nil, nil, backupstage.ArtifactEvidence{}, nil, invalidAgentStaging()
		}
		return prepared.Stage, source, sourceEvidence, stored, nil
	}
	upper := volume.SourceSizeUpperBound
	if upper == 0 || upper > backupformat.MaxStoredBytes {
		return nil, nil, backupstage.ArtifactEvidence{}, nil, invalidAgentStaging()
	}
	// The format limit is not a size estimate. Serialize unknown-size capture
	// and reserve each write before it reaches disk.
	stage, err := state.stager.Prepare(ctx, ids, backupstage.ExclusiveUnknown())
	if err != nil {
		return nil, nil, backupstage.ArtifactEvidence{}, nil, err
	}
	state.active[ids] = stage
	source, err := stage.CreateFile(ctx, executionplan.BackupSourceStagingFinal)
	return stage, source, backupstage.ArtifactEvidence{}, nil, err
}

func (state *backupStagingState) volumeRestoreStage(ctx context.Context, taskID string,
	step *agentpb.BackupStepAuthority,
) (*backupstage.Stage, *backupstage.Artifact, *backupstage.Artifact, error) {
	if state == nil || !state.ready || state.stager == nil || step.GetRestore().GetVolume() == nil {
		return nil, nil, nil, invalidAgentStaging()
	}
	ids := backupstage.IDs{Task: taskID, Step: step.StepId, Point: step.GetRestore().PointId}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.cleaned[ids] || state.active[ids] != nil {
		return nil, nil, nil, invalidAgentStaging()
	}
	for _, prepared := range state.prepared {
		if prepared.IDs != ids {
			continue
		}
		var source, stored *backupstage.Artifact
		for _, file := range prepared.Files {
			switch backupRecoveredFileRole(file.Evidence.Name) {
			case agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT:
				source = file.Artifact
			case agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT:
				stored = file.Artifact
			}
		}
		if source == nil ||
			stored == nil && step.GetRestore().Encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
			return nil, nil, nil, invalidAgentStaging()
		}
		if stored == nil {
			stored = source
		}
		return prepared.Stage, source, stored, nil
	}
	evidence := step.GetRestore().ExpectedEvidence
	if evidence == nil || evidence.SourceSizeBytes == 0 || evidence.StoredSizeBytes == 0 ||
		evidence.SourceSizeBytes > backupformat.MaxStoredBytes || evidence.StoredSizeBytes > backupformat.MaxStoredBytes ||
		evidence.SourceSizeBytes > math.MaxUint64-evidence.StoredSizeBytes {
		return nil, nil, nil, invalidAgentStaging()
	}
	bound := evidence.SourceSizeBytes + evidence.StoredSizeBytes
	if step.GetRestore().Encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE {
		bound = evidence.SourceSizeBytes
	}
	journalHeadroom := uint64(agentvolumemanifest.MaximumJournalBytes + agentvolumejournal.MaximumJournalBytes)
	if bound > math.MaxUint64-journalHeadroom {
		return nil, nil, nil, invalidAgentStaging()
	}
	bound += journalHeadroom
	stage, err := state.stager.Prepare(ctx, ids, backupstage.Bounded(bound))
	if err != nil {
		return nil, nil, nil, err
	}
	state.active[ids] = stage
	return stage, nil, nil, nil
}

func (state *backupStagingState) volumeStageAbsent(taskID string, step *agentpb.BackupStepAuthority) (bool, error) {
	if state == nil || !state.ready || state.plan == nil || state.inventory == nil || step == nil {
		return false, invalidAgentStaging()
	}
	pointID := step.GetCapture().GetPointId()
	if pointID == "" {
		pointID = step.GetRestore().GetPointId()
	}
	key, err := executionplan.BackupStagingRecoveryKey(taskID, step.StepId, pointID)
	if err != nil {
		return false, err
	}
	ids := backupstage.IDs{Task: taskID, Step: step.StepId, Point: pointID}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.cleaned[ids] {
		return true, nil
	}
	if state.active[ids] != nil {
		return false, nil
	}
	for _, prepared := range state.prepared {
		if prepared.IDs == ids {
			return false, nil
		}
	}
	for _, disposition := range state.plan.Dispositions {
		if bytes.Equal(disposition.RecoveryKeySha256, key) && disposition.GetDiscardRecovered() != nil {
			return true, nil
		}
	}
	for _, entry := range state.inventory.Entries {
		if bytes.Equal(entry.RecoveryKeySha256, key) {
			return false, nil
		}
	}
	return true, nil
}

func (state *backupStagingState) volumeMetadataAbsent(taskID string, step *agentpb.BackupStepAuthority) (bool, error) {
	if state == nil || !state.ready || state.plan == nil || step == nil || step.GetRestore().GetVolume() == nil {
		return false, invalidAgentStaging()
	}
	key, err := executionplan.BackupStagingRecoveryKey(taskID, step.StepId, step.GetRestore().PointId)
	if err != nil {
		return false, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if _, exists := state.volumes[string(key)]; exists {
		return false, nil
	}
	for _, disposition := range state.plan.VolumeDispositions {
		if bytes.Equal(disposition.RecoveryKeySha256, key) {
			return disposition.GetCleanup() != nil, nil
		}
	}
	return true, nil
}

func (state *backupStagingState) retireVolumeMetadata(taskID string, step *agentpb.BackupStepAuthority) error {
	if state == nil || step == nil || step.GetRestore().GetVolume() == nil {
		return invalidAgentStaging()
	}
	key, err := executionplan.BackupStagingRecoveryKey(taskID, step.StepId, step.GetRestore().PointId)
	if err != nil {
		return err
	}
	state.mu.Lock()
	delete(state.volumes, string(key))
	state.mu.Unlock()
	return nil
}

func (state *backupStagingState) retireVolumeStage(
	taskID string,
	step *agentpb.BackupStepAuthority,
	stage *backupstage.Stage,
) {
	pointID := step.GetCapture().GetPointId()
	if pointID == "" {
		pointID = step.GetRestore().GetPointId()
	}
	ids := backupstage.IDs{Task: taskID, Step: step.StepId, Point: pointID}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.cleaned[ids] = true
	if state.active[ids] == stage {
		delete(state.active, ids)
	}
	for index, prepared := range state.prepared {
		if prepared.IDs == ids && prepared.Stage == stage {
			copy(state.prepared[index:], state.prepared[index+1:])
			state.prepared[len(state.prepared)-1] = nil
			state.prepared = state.prepared[:len(state.prepared)-1]
			break
		}
	}
}
