package agent

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Prepared leases have already been matched to this assignment's exact resume
// before Submit. New stages are owned here until cleanup or pool shutdown.
func (state *backupStagingState) configCaptureStage(
	ctx context.Context, taskID string, step *agentpb.BackupStepAuthority,
) (*backupstage.Stage, *backupstage.Artifact, backupstage.ArtifactEvidence, *backupstage.Artifact, error) {
	if state == nil || !state.ready || state.stager == nil || step.GetCapture().GetConfig() == nil {
		return nil, nil, backupstage.ArtifactEvidence{}, nil, invalidAgentStaging()
	}
	ids := backupstage.IDs{Task: taskID, Step: step.StepId, Point: step.GetCapture().PointId}
	state.mu.Lock()
	defer state.mu.Unlock()
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
	if state.active[ids] != nil {
		return nil, nil, backupstage.ArtifactEvidence{}, nil, invalidAgentStaging()
	}
	content := step.GetCapture().GetConfig().Content
	journalGrowth, err := backupconfigtransfer.JournalGrowthUpperBound(content)
	if err != nil {
		return nil, nil, backupstage.ArtifactEvidence{}, nil, err
	}
	storedSize, err := backupconfig.AgeStoredSize(content.SourceSizeBytes)
	if err != nil {
		return nil, nil, backupstage.ArtifactEvidence{}, nil, err
	}
	stage, err := state.stager.Prepare(ctx, ids, backupstage.Bounded(content.SourceSizeBytes+storedSize+journalGrowth))
	if err != nil {
		return nil, nil, backupstage.ArtifactEvidence{}, nil, err
	}
	state.active[ids] = stage
	source, err := stage.CreateFile(ctx, executionplan.BackupSourceStagingFinal)
	if err != nil {
		return stage, nil, backupstage.ArtifactEvidence{}, nil, err
	}
	evidence, err := source.SyncPartial(ctx)
	return stage, source, evidence, nil, err
}

// A sealed verified upload can outlive its physical cleanup Ack. Only the
// completed startup inventory may prove that there is no stage to clean;
// absence never authorizes recapture or another Put.
func (state *backupStagingState) configStageAbsent(
	taskID string,
	step *agentpb.BackupStepAuthority,
) (bool, error) {
	if state == nil || !state.ready || state.plan == nil || state.inventory == nil ||
		configStagePointID(step) == "" {
		return false, invalidAgentStaging()
	}
	key, err := executionplan.BackupStagingRecoveryKey(taskID, step.StepId, configStagePointID(step))
	if err != nil {
		return false, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	ids := backupstage.IDs{Task: taskID, Step: step.StepId, Point: configStagePointID(step)}
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
			return true, nil // Ready follows the durable acknowledgement of this exact discard.
		}
	}
	for _, entry := range state.inventory.Entries {
		if bytes.Equal(entry.RecoveryKeySha256, key) {
			return false, nil
		}
	}
	return true, nil
}

// Successful physical cleanup releases the lease bookkeeping too. Otherwise
// every completed capture would retain its closed Stage until reconnect.
func (state *backupStagingState) retireConfigStage(
	taskID string,
	step *agentpb.BackupStepAuthority,
	stage *backupstage.Stage,
) {
	state.mu.Lock()
	defer state.mu.Unlock()
	ids := backupstage.IDs{Task: taskID, Step: step.StepId, Point: configStagePointID(step)}
	state.cleaned[ids] = true
	if state.active[ids] == stage {
		delete(state.active, ids)
	}
	for index, prepared := range state.prepared {
		if prepared.IDs == ids && prepared.Stage == stage {
			copy(state.prepared[index:], state.prepared[index+1:])
			state.prepared[len(state.prepared)-1] = nil
			state.prepared = state.prepared[:len(state.prepared)-1]
			return
		}
	}
}

func configStagePointID(step *agentpb.BackupStepAuthority) string {
	if step.GetCapture().GetConfig() != nil {
		return step.GetCapture().PointId
	}
	if step.GetRestore().GetConfig() != nil {
		return step.GetRestore().PointId
	}
	return ""
}
