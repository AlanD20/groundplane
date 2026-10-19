package backupruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/agent/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (state *Staging) retainedConfigRestore(
	ctx context.Context,
	assignment *agentpb.BackupRecoveryAssignment,
) (*backupconfiguration.RestoreArtifact, error) {
	step := assignment.Input.Steps[0]
	ids := backupstage.IDs{Task: assignment.Attempt.TaskId, Step: step.ExecutionId, Point: step.GetRestore().PointId}
	state.mu.Lock()
	defer state.mu.Unlock()
	retained := state.prepared[ids]
	stage := state.active[ids]
	if retained != nil {
		if retained.Stage == nil || (stage != nil && stage != retained.Stage) {
			return nil, errs.New(errs.KindStateConflict, "Config Restore staging ownership changed")
		}
		stage = retained.Stage
	}
	key, err := executionplan.BackupSourceStagingKey(ids.Task, ids.Step, ids.Point)
	if err != nil {
		return nil, err
	}
	if state.closed || state.sourceRecovery[string(key)].GetRecoveryRequired() != nil {
		return nil, errs.New(errs.KindStateConflict, "Config Restore recovery requires its retained source archive")
	}
	if stage == nil {
		// Absence is not corruption. The caller can reserve a new stage and
		// download the exact captured object; an existing invalid stage is held.
		return nil, nil
	}
	authority, err := backupartifact.NewRestoreAuthority(assignment.Input.EnvironmentId, step.GetRestore())
	if err != nil {
		return nil, err
	}
	file, evidence, err := stage.PublishedArtifact(ctx, executionplan.BackupSourceStagingFinal)
	if err != nil {
		return nil, err
	}
	return backupconfiguration.PrepareRetainedRestore(ctx, authority, stage, file, evidence)
}

func closeConfigRecoveryArtifact(ctx context.Context, artifact *backupconfiguration.RestoreArtifact, resultErr *error) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := artifact.Close(closeCtx); err != nil {
		*resultErr = errs.WrapJoined(errs.KindStorageUnavailable, *resultErr, err)
	}
}
