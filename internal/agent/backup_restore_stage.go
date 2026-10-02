package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (state *backupStagingState) configRestoreStage(ctx context.Context, taskID string,
	step *agentpb.BackupStepAuthority,
) (*backupstage.Stage, *backupstage.Artifact, *backupstage.Artifact, error) {
	if state == nil || !state.ready || state.stager == nil || step.GetRestore().GetConfig() == nil {
		return nil, nil, nil, invalidAgentStaging()
	}
	ids := backupstage.IDs{Task: taskID, Step: step.StepId, Point: step.GetRestore().PointId}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.cleaned[ids] {
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
		if source == nil || stored == nil {
			return nil, nil, nil, invalidAgentStaging()
		}
		return prepared.Stage, source, stored, nil
	}
	if state.active[ids] != nil {
		return nil, nil, nil, invalidAgentStaging()
	}
	required, err := backupconfiguration.RequiredRestoreBytes(step.GetRestore().ExpectedEvidence)
	if err != nil {
		return nil, nil, nil, err
	}
	journalGrowth, err := backupconfigtransfer.JournalGrowthUpperBound(
		step.GetRestore().GetConfig().GetExpectedArchive().GetContent(),
	)
	if err != nil {
		return nil, nil, nil, err
	}
	stage, err := state.stager.Prepare(ctx, ids, backupstage.Bounded(required+journalGrowth))
	if err != nil {
		return nil, nil, nil, err
	}
	state.active[ids] = stage
	return stage, nil, nil, nil
}
