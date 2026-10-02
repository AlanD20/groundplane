package agent

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// handleTaskAbort treats a rejected-report projection only as an authenticated
// hint about native Task truth. Journal application and retirement have their
// own receipt handshake; this path never changes capacity or durable progress.
func (c *Client) handleTaskAbort(ctx context.Context, abort *agentpb.TaskAbort) error {
	if c == nil || c.pool == nil || ctx == nil {
		return errs.New(errs.KindInternal, "agent: Task abort client is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := validateTaskAbortProjection(abort); err != nil {
		return err
	}
	if abort.RejectedTaskAckSha256 == nil {
		return c.pool.AbortMessage(ctx, abort)
	}
	if c.terminalJournal == nil {
		return errs.New(errs.KindInternal, "agent: terminal journal is unavailable")
	}
	records, err := c.terminalJournal.List(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		ack := record.Ack
		if ack.TaskId != abort.TaskId {
			continue
		}
		digest, err := executionplan.TaskAcknowledgementSHA256(ack)
		if err != nil {
			return err
		}
		if ack.AssignmentId != abort.AssignmentId || ack.AssignmentGeneration != abort.AssignmentGeneration ||
			!bytes.Equal(ack.PlanHash, abort.PlanHash) || !bytes.Equal(digest, abort.RejectedTaskAckSha256) {
			return errs.New(errs.KindStateConflict, "agent: native Task projection does not match the retained report")
		}
		if receipt := record.Receipt; receipt != nil {
			if receipt.Terminal != abort.Terminal || receipt.DurableTaskModRevision != abort.DurableTaskModRevision ||
				!bytes.Equal(receipt.TerminalReceiptSha256, abort.TerminalReceiptSha256) {
				return errs.New(
					errs.KindStateConflict,
					"agent: native Task projection conflicts with the retained receipt",
				)
			}
		}
		return c.pool.verifyRetainedBackupReport(ack)
	}
	return errs.New(errs.KindStateConflict, "agent: native Task projection has no retained report")
}

func (p *WorkerPool) verifyRetainedBackupReport(ack *agentpb.TaskAck) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	reservation := p.reservations[ack.TaskId]
	// Only the caller's already-validated durable report permits this case:
	// reconnect creates a new pool without the old terminal reservation.
	if reservation == nil {
		return nil
	}
	assignment := reservation.assignment
	digest := taskassignment.PlanDigest(assignment.Plan)
	if !reservation.terminalProduced || assignment.BackupAuthority == nil || assignment.TaskID != ack.TaskId ||
		assignment.AssignmentID != ack.AssignmentId || assignment.AssignmentGeneration != ack.AssignmentGeneration ||
		assignment.ExecutionEpoch != ack.ExecutionEpoch || !bytes.Equal(digest[:], ack.PlanHash) {
		return errs.New(errs.KindStateConflict, "agent: retained Task report does not match the terminal reservation")
	}
	return nil
}

// Native projection fields form one closed group. Optional rejected SHA
// presence is meaningful even when its byte slice is empty.
func validateTaskAbortProjection(abort *agentpb.TaskAbort) (bool, error) {
	if abort == nil || executionplan.RejectUnknown(abort) != nil || ids.Validate(ids.KindTask, abort.TaskId) != nil ||
		ids.Validate(ids.KindAssignment, abort.AssignmentId) != nil {
		return false, errs.New(errs.KindValidationFailed, "agent: Task abort identity is invalid")
	}
	projection := abort.Terminal != agentpb.TaskTerminal_TASK_TERMINAL_UNSPECIFIED ||
		len(abort.TerminalReceiptSha256) != 0 ||
		abort.DurableTaskModRevision != 0 ||
		abort.RejectedTaskAckSha256 != nil
	if !projection {
		return false, nil
	}
	if !knownTaskAbortTerminal(abort.Terminal) || len(abort.TerminalReceiptSha256) != sha256.Size ||
		abort.DurableTaskModRevision <= 0 ||
		abort.AssignmentGeneration == 0 ||
		len(abort.PlanHash) != sha256.Size ||
		(abort.RejectedTaskAckSha256 != nil && len(abort.RejectedTaskAckSha256) != sha256.Size) {
		return false, errs.New(errs.KindValidationFailed, "agent: native Task abort projection is incomplete")
	}
	return true, nil
}

func knownTaskAbortTerminal(terminal agentpb.TaskTerminal) bool {
	switch terminal {
	case agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED, agentpb.TaskTerminal_TASK_TERMINAL_FAILED,
		agentpb.TaskTerminal_TASK_TERMINAL_TIMED_OUT, agentpb.TaskTerminal_TASK_TERMINAL_ABORTED:
		return true
	default:
		return false
	}
}
