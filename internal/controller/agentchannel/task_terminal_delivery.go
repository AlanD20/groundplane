package agentchannel

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type taskTerminalDeliveryStore interface {
	ResolveTaskTerminalReceipt(
		context.Context,
		string,
		uint64,
		string,
		string,
		uint64,
	) (etcdstore.Versioned[taskjournal.TaskTerminalReceiptRecord], error)
	BeginTaskTerminalDelivery(context.Context, string, uint64, *agentpb.TaskTerminalReceiptAck, *agentpb.TaskAck) error
	ApplyTaskTerminalReceipt(context.Context, string, uint64, *agentpb.TaskTerminalReceiptApplied) error
	RetireTaskTerminalAssignment(context.Context, string, uint64, *agentpb.TaskTerminalAssignmentRetired) error
}

func (s *Server) sendTaskTerminalReceipt(
	stream agentpb.AgentChannel_ConnectServer,
	session *Session,
	agentID string,
	agentGeneration uint64,
	ack *agentpb.TaskAck,
	rejected bool,
) error {
	store, ok := s.tasks.(taskTerminalDeliveryStore)
	if !ok {
		return errs.New(errs.KindInternal, "Task terminal delivery store is unavailable")
	}
	process, err := session.ProcessAuthentication()
	if err != nil {
		return err
	}
	stored, err := store.ResolveTaskTerminalReceipt(
		stream.Context(),
		agentID,
		agentGeneration,
		ack.TaskId,
		ack.AssignmentId,
		ack.AssignmentGeneration,
	)
	if err != nil {
		return err
	}
	// The immutable receipt proves settlement; an unaccepted worker report does
	// not authorize destruction of a request-only identity.
	if s.secrets != nil {
		if err := s.secrets.ReleaseRestoreIdentity(stream.Context(), ack.TaskId); err != nil {
			return err
		}
	}
	receiptSHA, err := taskjournal.TaskTerminalReceiptSHA256(stored.Record)
	if err != nil {
		return err
	}
	receiptDigest, err := hex.DecodeString(receiptSHA)
	if err != nil {
		return errs.New(errs.KindInternal, "Task terminal receipt digest is invalid")
	}
	ackDigest, err := executionplan.TaskAcknowledgementSHA256(ack)
	if err != nil {
		return err
	}
	receipt := &agentpb.TaskTerminalReceiptAck{
		ProcessGeneration: append(
			[]byte(nil),
			process.ProcessGeneration[:]...), TaskId: ack.TaskId, AssignmentId: ack.AssignmentId,
		AssignmentGeneration: ack.AssignmentGeneration, PlanHash: append([]byte(nil), ack.PlanHash...), Terminal: taskjournal.TaskTerminalWire(stored.Record.Terminal),
		TaskAckSha256: ackDigest, TerminalReceiptSha256: receiptDigest, DurableTaskModRevision: stored.Revision,
	}
	if err := store.BeginTaskTerminalDelivery(stream.Context(), agentID, agentGeneration, receipt, ack); err != nil {
		return err
	}
	if rejected {
		projection, err := terminalTaskAbort(stored, "native_terminal_result")
		if err != nil {
			return err
		}
		projection.RejectedTaskAckSha256 = append([]byte(nil), ackDigest...)
		if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_TaskAbort{TaskAbort: projection}}); err != nil {
			return err
		}
	}
	return stream.Send(
		&agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_TaskTerminalReceiptAck{TaskTerminalReceiptAck: receipt},
		},
	)
}

func (s *Server) handleTaskTerminalDelivery(
	stream agentpb.AgentChannel_ConnectServer, session *Session, agentID string, agentGeneration uint64,
	message *agentpb.AgentMessage, delivered, quarantined map[string]string,
) (bool, error) {
	applied, retired := message.GetTaskTerminalReceiptApplied(), message.GetTaskTerminalAssignmentRetired()
	if applied == nil && retired == nil {
		return false, nil
	}
	if err := executionplan.RejectUnknown(message); err != nil {
		return true, err
	}
	store, ok := s.tasks.(taskTerminalDeliveryStore)
	if !ok {
		return true, errs.New(errs.KindInternal, "Task terminal delivery store is unavailable")
	}
	process, err := session.ProcessAuthentication()
	if err != nil {
		return true, err
	}
	if applied != nil {
		if !bytes.Equal(applied.ProcessGeneration, process.ProcessGeneration[:]) {
			return true, errs.New(errs.KindStateConflict, "Task receipt belongs to another Agent process")
		}
		if err := store.ApplyTaskTerminalReceipt(stream.Context(), agentID, agentGeneration, applied); err != nil {
			return true, err
		}
		return true, stream.Send(
			&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_TaskTerminalReceiptAppliedAck{
				TaskTerminalReceiptAppliedAck: &agentpb.TaskTerminalReceiptAppliedAck{
					ProcessGeneration: append(
						[]byte(nil),
						applied.ProcessGeneration...), TaskId: applied.TaskId, AssignmentId: applied.AssignmentId,
					AssignmentGeneration: applied.AssignmentGeneration, PlanHash: append([]byte(nil), applied.PlanHash...), Terminal: applied.Terminal,
					TaskAckSha256: append(
						[]byte(nil),
						applied.TaskAckSha256...), TerminalReceiptSha256: append([]byte(nil), applied.TerminalReceiptSha256...),
					DurableTaskModRevision: applied.DurableTaskModRevision,
				},
			}},
		)
	}
	if !bytes.Equal(retired.ProcessGeneration, process.ProcessGeneration[:]) {
		return true, errs.New(errs.KindStateConflict, "Task retirement belongs to another Agent process")
	}
	if err := store.RetireTaskTerminalAssignment(stream.Context(), agentID, agentGeneration, retired); err != nil {
		return true, err
	}
	if err := session.RecordTaskTerminal(retired.TaskId, retired.AssignmentId); err != nil {
		return true, err
	}
	delete(delivered, retired.TaskId)
	delete(quarantined, retired.TaskId)
	return true, stream.Send(
		&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_TaskTerminalAssignmentRetiredAck{
			TaskTerminalAssignmentRetiredAck: &agentpb.TaskTerminalAssignmentRetiredAck{
				ProcessGeneration: append(
					[]byte(nil),
					retired.ProcessGeneration...), TaskId: retired.TaskId, AssignmentId: retired.AssignmentId,
				AssignmentGeneration: retired.AssignmentGeneration, PlanHash: append([]byte(nil), retired.PlanHash...), Terminal: retired.Terminal,
			},
		}},
	)
}
