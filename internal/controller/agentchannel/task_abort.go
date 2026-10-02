package agentchannel

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (s *Server) taskAbortMessage(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	command taskAbortCommand,
) (*agentpb.TaskAbort, error) {
	task, err := s.tasks.GetTask(ctx, command.taskID)
	if err != nil {
		return nil, err
	}
	message := &agentpb.TaskAbort{TaskId: command.taskID, AssignmentId: command.assignmentID, Reason: command.reason}
	switch task.Record.Type {
	case taskjournal.TaskBackup, taskjournal.TaskBackupPrune, taskjournal.TaskRestore:
	default:
		return message, nil
	}
	if taskjournal.IsTerminalTaskStatus(task.Record.Status) {
		identity := task.Record.TerminalAssignment
		if identity == nil {
			return nil, errs.New(errs.KindStateConflict, "Backup terminal assignment is unavailable")
		}
		store, ok := s.tasks.(taskTerminalDeliveryStore)
		if !ok {
			return nil, errs.New(errs.KindInternal, "Task terminal delivery store is unavailable")
		}
		stored, err := store.ResolveTaskTerminalReceipt(
			ctx,
			agentID,
			agentGeneration,
			command.taskID,
			command.assignmentID,
			identity.AssignmentGeneration,
		)
		if err != nil {
			return nil, err
		}
		return terminalTaskAbort(stored, command.reason)
	}
	assignments, ok := s.tasks.(interface {
		GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
	})
	if !ok {
		return nil, errs.New(errs.KindInternal, "Backup abort assignment reader is unavailable")
	}
	claim, err := assignments.GetTaskAssignment(ctx, command.taskID)
	if err != nil {
		return nil, err
	}
	assignment := claim.Assignment.Record
	if assignment.AgentID != agentID || assignment.AgentGeneration != agentGeneration ||
		assignment.AssignmentID != command.assignmentID ||
		assignment.BackupAuthorityFence == nil ||
		claim.Task.Record.PlanHash != task.Record.PlanHash {
		return nil, errs.New(errs.KindStateConflict, "Backup abort assignment authority changed")
	}
	digest, err := hex.DecodeString(task.Record.PlanHash)
	if err != nil || len(digest) != 32 {
		return nil, errs.New(errs.KindInternal, "Backup abort plan digest is invalid")
	}
	message.AssignmentGeneration, message.PlanHash = assignment.BackupAuthorityFence.AssignmentGeneration, digest
	return message, nil
}

// Native terminal truth is a cancellation hint, not authority to retire a
// reservation. Retirement still requires the exact receipt delivery handshake.
func terminalTaskAbort(
	stored etcdstore.Versioned[taskjournal.TaskTerminalReceiptRecord],
	reason string,
) (*agentpb.TaskAbort, error) {
	digest, err := taskjournal.TaskTerminalReceiptSHA256(stored.Record)
	if err != nil {
		return nil, err
	}
	receiptSHA, err := hex.DecodeString(digest)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "Task terminal receipt digest is invalid")
	}
	planSHA, err := hex.DecodeString(stored.Record.PlanHash)
	if err != nil || len(planSHA) != 32 || stored.Revision <= 0 {
		return nil, errs.New(errs.KindInternal, "Task terminal projection authority is invalid")
	}
	return &agentpb.TaskAbort{
		TaskId: stored.Record.TaskID, AssignmentId: stored.Record.AssignmentID, Reason: reason,
		AssignmentGeneration: stored.Record.AssignmentGeneration, PlanHash: planSHA,
		Terminal: taskjournal.TaskTerminalWire(stored.Record.Terminal), TerminalReceiptSha256: receiptSHA,
		DurableTaskModRevision: stored.Revision,
	}, nil
}
