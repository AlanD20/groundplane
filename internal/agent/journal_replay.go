package agent

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workreceipt"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Replays durable reports after authentication and before any Ready, worker or
// replacement fallback. No operation Run/Start/helper is called in this phase.
func (client *Client) replayWorkJournal(ctx context.Context, stream agentStream) error {
	after := ""
	for {
		page, err := client.workJournal.Inventory(ctx, after)
		if err != nil {
			return err
		}
		for _, entry := range page.Entries {
			if entry.Delivery == nil {
				continue
			}
			if len(entry.Delivery.Reply) != 0 {
				if err := client.workJournal.SettleTerminalDelivery(ctx, entry.Attempt.ID); err != nil {
					client.logger.Warn("saved work acknowledgement retains local evidence", "attempt_id", entry.Attempt.ID, "error", err)
				}
				continue
			}
			if err := client.processOwnership.Check(); err != nil {
				return err
			}
			if err := stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_WorkJournalReplayRequest{
				WorkJournalReplayRequest: &agentpb.WorkJournalReplayRequest{Report: entry.Delivery.Request},
			}}); err != nil {
				return err
			}
			response, err := stream.Recv()
			if err != nil {
				return err
			}
			ack := response.GetWorkJournalReplayAck()
			digest := sha256.Sum256(entry.Delivery.Request)
			if ack == nil || executionplan.RejectUnknown(response) != nil || proto.Size(response) > workreceipt.MaximumBytes+1024 ||
				ack.Complete || !bytes.Equal(ack.ReportSha256, digest[:]) ||
				(ack.FailureCode != "") == (len(ack.Acknowledgement) != 0) ||
				ack.FailureCode == "" && ack.FailureDetail != "" {
				return errs.New(errs.KindStateConflict, "startup result acknowledgement differs from the retained report")
			}
			if ack.FailureCode != "" {
				client.logger.Warn("saved work result remains unresolved", "task_id", entry.Attempt.TaskID, "work_id", entry.Attempt.WorkItemID, "attempt_id", entry.Attempt.ID, "code", ack.FailureCode, "detail", ack.FailureDetail)
				continue
			}
			request, err := entry.Delivery.Message()
			if err != nil {
				return err
			}
			reply := new(agentpb.ControllerMessage)
			if err := proto.Unmarshal(ack.Acknowledgement, reply); err != nil {
				return errs.New(errs.KindStateConflict, "startup result acknowledgement cannot be decoded")
			}
			if err := client.workJournal.AcknowledgeTerminalDelivery(ctx, request, reply); err != nil {
				return err
			}
			if err := client.workJournal.SettleTerminalDelivery(ctx, entry.Attempt.ID); err != nil {
				client.logger.Warn("replayed work acknowledgement retains local evidence", "attempt_id", entry.Attempt.ID, "error", err)
			}
		}
		if page.After == "" {
			break
		}
		after = page.After
	}
	if err := stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_WorkJournalReplayRequest{
		WorkJournalReplayRequest: &agentpb.WorkJournalReplayRequest{Complete: true},
	}}); err != nil {
		return err
	}
	response, err := stream.Recv()
	if err != nil {
		return err
	}
	ack := response.GetWorkJournalReplayAck()
	if ack == nil || executionplan.RejectUnknown(response) != nil || !ack.Complete || len(ack.ReportSha256) != 0 ||
		len(ack.Acknowledgement) != 0 || ack.FailureCode != "" || ack.FailureDetail != "" {
		return errs.New(errs.KindStateConflict, "Controller did not acknowledge completed startup result replay")
	}
	return nil
}
