package agent

import (
	"context"
	"math"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (state *backupStagingState) postgresCaptureStage(ctx context.Context, taskID string,
	step *agentpb.BackupStepAuthority, requireRetained bool,
) (*backupstage.Stage, *backupstage.Artifact, backupstage.ArtifactEvidence, *backupstage.Artifact, error) {
	postgres := step.GetCapture().GetPostgres()
	if state == nil || !state.ready || state.stager == nil || postgres == nil ||
		postgres.MaxPlaintextBytes == 0 || postgres.MaxPlaintextBytes > backupformat.MaxStoredBytes {
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
		var evidence backupstage.ArtifactEvidence
		for _, file := range prepared.Files {
			switch backupRecoveredFileRole(file.Evidence.Name) {
			case agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT:
				source, evidence = file.Artifact, file.Evidence
			case agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT:
				stored = file.Artifact
			}
		}
		if source == nil {
			return nil, nil, backupstage.ArtifactEvidence{}, nil, invalidAgentStaging()
		}
		return prepared.Stage, source, evidence, stored, nil
	}
	if requireRetained {
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

func (state *backupStagingState) postgresRestoreStage(ctx context.Context, taskID string,
	step *agentpb.BackupStepAuthority,
) (*backupstage.Stage, *backupstage.Artifact, *backupstage.Artifact, error) {
	if state == nil || !state.ready || state.stager == nil || step.GetRestore().GetPostgres() == nil {
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
		if source == nil || stored == nil && step.GetRestore().Encryption.Kind ==
			agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
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
	stage, err := state.stager.Prepare(ctx, ids, backupstage.Bounded(bound))
	if err != nil {
		return nil, nil, nil, err
	}
	state.active[ids] = stage
	return stage, nil, nil, nil
}

func (state *backupStagingState) retirePostgresStage(taskID string, step *agentpb.BackupStepAuthority,
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
