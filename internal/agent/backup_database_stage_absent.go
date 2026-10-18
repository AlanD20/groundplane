package agent

import (
	"bytes"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (state *backupStagingState) databaseStageAbsent(taskID string, step *agentpb.BackupStepAuthority) (bool, error) {
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
