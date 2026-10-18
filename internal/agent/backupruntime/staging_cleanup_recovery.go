package backupruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Caller has excluded the former executor and retired any detached writer.
// Startup inventory owns untracked files; a held disposition is never absence.
func (state *Staging) cleanupCapturedSource(ctx context.Context, ids backupstage.IDs) error {
	key, err := executionplan.BackupSourceStagingKey(ids.Task, ids.Step, ids.Point)
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return errs.New(errs.KindStateConflict, "Backup cleanup staging session is closed")
	}
	if state.sourceRecovery[string(key)].GetRecoveryRequired() != nil {
		return errs.New(errs.KindStateConflict, "Backup source files are held by startup recovery")
	}
	stage := state.active[ids]
	if prepared := state.prepared[ids]; prepared != nil {
		if stage != nil && stage != prepared.Stage {
			return errs.New(errs.KindStateConflict, "Backup cleanup source has conflicting staging ownership")
		}
		stage = prepared.Stage
	}
	if stage != nil {
		if err := stage.Cleanup(ctx); err != nil {
			return err
		}
	}
	delete(state.active, ids)
	delete(state.prepared, ids)
	delete(state.sourceRecovery, string(key))
	return nil
}
