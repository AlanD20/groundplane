package agent

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// AbortMessage matches a Backup's complete assignment fence before cancelling.
// Terminal projections are not ordinary cancellation and cannot be discarded.
func (p *WorkerPool) AbortMessage(ctx context.Context, abort *agentpb.TaskAbort) error {
	if p == nil || ctx == nil {
		return errs.New(errs.KindInternal, "agent: abort context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	projection, err := validateTaskAbortProjection(abort)
	if err != nil {
		return err
	}
	if abort.RejectedTaskAckSha256 != nil {
		return errs.New(errs.KindValidationFailed, "agent: rejected Task report requires retained journal proof")
	}
	p.mu.Lock()
	reservation := p.reservations[abort.TaskId]
	if reservation == nil || reservation.assignment.AssignmentID != abort.AssignmentId {
		p.mu.Unlock()
		return errs.New(errs.KindStateConflict, "agent: Task abort assignment is not reserved")
	}
	assignment := reservation.assignment
	digest := taskassignment.PlanDigest(assignment.Plan)
	if assignment.BackupAuthority != nil {
		if abort.AssignmentGeneration != assignment.AssignmentGeneration || !bytes.Equal(abort.PlanHash, digest[:]) {
			p.mu.Unlock()
			return errs.New(errs.KindStateConflict, "agent: Backup abort authority changed")
		}
		reservation.cancel()
		p.mu.Unlock()
		// Completion remains responsible for owned transient cleanup. The native
		// projection neither releases this reservation nor changes its report.
		return nil
	} else if abort.AssignmentGeneration != 0 || len(abort.PlanHash) != 0 || projection {
		p.mu.Unlock()
		return errs.New(errs.KindValidationFailed, "agent: ordinary Task abort carries Backup authority")
	}
	reservation.cancel()
	p.mu.Unlock()
	p.materializations.Retire(abort.TaskId)
	p.managedConfigs.Release(abort.TaskId)
	p.backupSecrets.Release(abort.TaskId)
	return nil
}
