package agent

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/agentterminaljournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (p *WorkerPool) retireBackupAssignment(receipt *agentpb.TaskTerminalReceiptAck) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	reservation := p.reservations[receipt.TaskId]
	// A reconnect creates a new pool; only its durable journal authorizes this
	// replay, checked by the caller before touching the reservation.
	if reservation == nil {
		return nil
	}
	assignment := reservation.assignment
	digest := taskassignment.PlanDigest(assignment.Plan)
	if !reservation.terminalProduced || assignment.BackupAuthority == nil ||
		assignment.AssignmentID != receipt.AssignmentId || assignment.AssignmentGeneration != receipt.AssignmentGeneration ||
		!bytes.Equal(digest[:], receipt.PlanHash) {
		return errs.New(errs.KindStateConflict, "agent: terminal retirement does not match the reserved assignment")
	}
	delete(p.reservations, receipt.TaskId)
	return nil
}

func (c *Client) terminalDeliveryClean() (bool, error) {
	c.pool.mu.Lock()
	failure := c.pool.terminalDeliveryError
	c.pool.mu.Unlock()
	if failure != nil {
		return false, failure
	}
	if c.terminalJournal == nil {
		return false, errs.New(errs.KindInternal, "agent: terminal journal is unavailable")
	}
	records, err := c.terminalJournal.List(context.Background())
	return len(records) == 0, err
}

func (c *Client) replayTerminalDelivery(ctx context.Context, stream agentStream) error {
	records, err := c.terminalJournal.List(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Phase == agentterminaljournal.PhasePending ||
			!bytes.Equal(record.Receipt.ProcessGeneration, c.processGeneration[:]) {
			err = stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_TaskAck{TaskAck: record.Ack}})
		} else {
			err = sendTerminalJournalRecord(stream, record)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func sendTerminalJournalRecord(stream agentStream, record agentterminaljournal.Record) error {
	if record.Phase == agentterminaljournal.PhaseApplied {
		applied, err := executionplan.TaskTerminalReceiptApplied(record.Receipt)
		if err != nil {
			return err
		}
		return stream.Send(
			&agentpb.AgentMessage{
				Payload: &agentpb.AgentMessage_TaskTerminalReceiptApplied{TaskTerminalReceiptApplied: applied},
			},
		)
	}
	if record.Phase != agentterminaljournal.PhaseRetired {
		return errs.New(errs.KindInternal, "agent: terminal journal phase is invalid")
	}
	receipt := record.Receipt
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_TaskTerminalAssignmentRetired{
		TaskTerminalAssignmentRetired: &agentpb.TaskTerminalAssignmentRetired{
			ProcessGeneration: append(
				[]byte(nil),
				receipt.ProcessGeneration...), TaskId: receipt.TaskId, AssignmentId: receipt.AssignmentId,
			AssignmentGeneration: receipt.AssignmentGeneration, PlanHash: append([]byte(nil), receipt.PlanHash...), Terminal: receipt.Terminal,
		},
	}})
}

func (c *Client) handleTerminalDelivery(
	ctx context.Context,
	stream agentStream,
	message *agentpb.ControllerMessage,
) (bool, error) {
	receipt := message.GetTaskTerminalReceiptAck()
	applied := message.GetTaskTerminalReceiptAppliedAck()
	retired := message.GetTaskTerminalAssignmentRetiredAck()
	if receipt == nil && applied == nil && retired == nil {
		return false, nil
	}
	if err := executionplan.RejectUnknown(message); err != nil {
		return true, err
	}
	if c.terminalJournal == nil {
		return true, errs.New(errs.KindInternal, "agent: terminal journal is unavailable")
	}
	if applied != nil {
		if err := executionplan.RejectUnknown(applied); err != nil {
			return true, err
		}
		receipt = &agentpb.TaskTerminalReceiptAck{ProcessGeneration: applied.ProcessGeneration, TaskId: applied.TaskId,
			AssignmentId: applied.AssignmentId, AssignmentGeneration: applied.AssignmentGeneration, PlanHash: applied.PlanHash,
			Terminal: applied.Terminal, TaskAckSha256: applied.TaskAckSha256, TerminalReceiptSha256: applied.TerminalReceiptSha256,
			DurableTaskModRevision: applied.DurableTaskModRevision}
	}
	if retired != nil {
		return true, c.removeRetiredTerminal(ctx, stream, retired)
	}
	if err := executionplan.ValidateTaskTerminalReceiptAck(receipt); err != nil {
		return true, err
	}
	if !bytes.Equal(receipt.ProcessGeneration, c.processGeneration[:]) {
		return true, errs.New(errs.KindStateConflict, "agent: terminal receipt belongs to another process")
	}
	if applied == nil {
		if err := c.terminalJournal.Apply(ctx, receipt); err != nil {
			return true, err
		}
		records, err := c.terminalJournal.List(ctx)
		if err != nil {
			return true, err
		}
		for _, record := range records {
			if record.Ack.TaskId == receipt.TaskId {
				return true, sendTerminalJournalRecord(stream, record)
			}
		}
		return true, errs.New(errs.KindInternal, "agent: applied terminal journal disappeared")
	}
	if err := c.terminalJournal.Retire(ctx, receipt); err != nil {
		return true, err
	}
	if err := c.pool.retireBackupAssignment(receipt); err != nil {
		return true, err
	}
	return true, sendTerminalJournalRecord(
		stream,
		agentterminaljournal.Record{Receipt: proto.CloneOf(receipt), Phase: agentterminaljournal.PhaseRetired},
	)
}

func (c *Client) removeRetiredTerminal(
	ctx context.Context,
	stream agentStream,
	retired *agentpb.TaskTerminalAssignmentRetiredAck,
) error {
	if err := executionplan.RejectUnknown(retired); err != nil {
		return err
	}
	records, err := c.terminalJournal.List(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		receipt := record.Receipt
		if record.Ack.TaskId != retired.TaskId {
			continue
		}
		if receipt == nil || !bytes.Equal(retired.ProcessGeneration, c.processGeneration[:]) ||
			!bytes.Equal(
				retired.ProcessGeneration,
				receipt.ProcessGeneration,
			) || retired.AssignmentId != receipt.AssignmentId ||
			retired.AssignmentGeneration != receipt.AssignmentGeneration || retired.Terminal != receipt.Terminal || !bytes.Equal(retired.PlanHash, receipt.PlanHash) {
			return errs.New(
				errs.KindStateConflict,
				"agent: terminal retirement acknowledgement does not match the journal",
			)
		}
		if err := c.terminalJournal.RemoveRetired(ctx, receipt); err != nil {
			return err
		}
		return c.sendReady(stream)
	}
	return errs.New(errs.KindStateConflict, "agent: terminal retirement acknowledgement has no journal")
}
