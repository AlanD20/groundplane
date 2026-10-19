package backupruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A startup hold preserves descriptors without giving a forward worker access.
// A durably started recovery assignment may acquire only its original source,
// after execution-side ownership proves the former executor cannot continue.
func (state *Staging) AuthorizeRecovery(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	attempt workexecution.Attempt, ownership *agentownership.Lease,
) error {
	if state == nil || ctx == nil || ownership == nil || attempt.StartedAt == nil ||
		!time.Now().Before(attempt.Deadline) {
		return errs.New(errs.KindStateConflict, "Staging recovery requires its started assignment")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	owned, expected, source, err := executionplan.OpenBackupRecoveryAssignment(assignment, attempt.Executor)
	if err != nil {
		return err
	}
	if !workexecution.SameAssignment(expected, attempt) {
		return errs.New(errs.KindStateConflict, "Staging recovery differs from its active assignment")
	}
	if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return errs.New(errs.KindStateConflict, "Staging recovery session is closed")
	}
	ids := make([]backupstage.IDs, 0, len(owned.Input.Steps))
	keys := make([]string, 0, len(owned.Input.Steps))
	for _, step := range owned.Input.Steps {
		point := step.GetCapture().GetPointId()
		if point == "" {
			point = step.GetRestore().GetPointId()
		}
		if point == "" {
			continue
		}
		id := backupstage.IDs{Task: attempt.TaskID, Step: step.ExecutionId, Point: point}
		key, err := executionplan.BackupSourceStagingKey(id.Task, id.Step, id.Point)
		if err != nil {
			return err
		}
		hold := state.sourceRecovery[string(key)].GetRecoveryRequired()
		if hold == nil {
			continue
		}
		if owned.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE ||
			owned.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION ||
			state.prepared[id] != nil || state.active[id] != nil {
			return errs.New(errs.KindStateConflict, "Held source is not owned by this Restore recovery")
		}
		retained := state.held[id]
		if retained == nil && len(hold.ExpectedFiles) != 0 {
			return errs.New(errs.KindStateConflict, "Held Restore source descriptors are unavailable")
		}
		ids, keys = append(ids, id), append(keys, string(key))
	}
	// Validate the complete acquisition before transferring any descriptor.
	for index, id := range ids {
		if retained := state.held[id]; retained != nil {
			state.prepared[id] = retained
			delete(state.held, id)
		}
		delete(state.sourceRecovery, keys[index])
	}
	return nil
}
