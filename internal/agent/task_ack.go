package agent

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/internal/infra/agentterminaljournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func buildTaskAcknowledgement(result TaskResult) (*agentpb.TaskAck, error) {
	variants := 0
	if result.Compose != nil {
		variants++
	}
	if result.EnvironmentDirectory != nil {
		variants++
	}
	if result.Backup != nil {
		variants++
	}
	if variants != 1 || (result.Backup != nil) != (result.AssignmentGeneration > 0) {
		return nil, errs.New(errs.KindInternal, "agent: worker returned an invalid task result union")
	}
	terminal := agentpb.TaskTerminal_TASK_TERMINAL_UNSPECIFIED
	switch result.Terminal {
	case TaskTerminalCompleted:
		terminal = agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED
	case TaskTerminalFailed:
		terminal = agentpb.TaskTerminal_TASK_TERMINAL_FAILED
	case TaskTerminalTimedOut:
		terminal = agentpb.TaskTerminal_TASK_TERMINAL_TIMED_OUT
	case TaskTerminalAborted:
		terminal = agentpb.TaskTerminal_TASK_TERMINAL_ABORTED
	default:
		return nil, errs.New(errs.KindInternal, "agent: worker returned an invalid terminal state")
	}
	ack := &agentpb.TaskAck{
		TaskId:                      result.TaskID,
		AssignmentId:                result.AssignmentID,
		PlanHash:                    append([]byte(nil), result.PlanHash[:]...),
		Terminal:                    terminal,
		ExitCode:                    result.ExitCode,
		ExecutionEpoch:              result.ExecutionEpoch,
		AssignmentGeneration:        result.AssignmentGeneration,
		ReleaseRecoveryRecordSha256: append([]byte(nil), result.ReleaseRecoveryRecordSHA256...),
	}
	if result.Backup != nil {
		ack.Result = &agentpb.TaskAck_BackupResult{BackupResult: proto.CloneOf(result.Backup)}
	} else if result.Compose != nil {
		for _, evidence := range []*agentpb.DNSResolverObservationEvidence{
			result.Compose.GetDnsResolverCandidateObservation(), result.Compose.GetDnsResolverRollbackObservation(),
		} {
			if evidence != nil {
				if err := dnsproof.Verify(evidence); err != nil {
					return nil, errs.New(errs.KindInternal, "agent: worker returned a corrupt DNS resolver proof")
				}
			}
		}
		ack.Result = &agentpb.TaskAck_ComposeResult{ComposeResult: proto.CloneOf(result.Compose)}
	} else {
		ack.Result = &agentpb.TaskAck_EnvironmentDirectoryResult{EnvironmentDirectoryResult: proto.CloneOf(result.EnvironmentDirectory)}
	}
	return ack, nil
}

// Persist before queueing output: a disconnect may cancel the stream before its
// writer reads that output. Cancellation cannot erase an already-produced result.
func (c *Client) persistBackupTerminal(ctx context.Context, result TaskResult) error {
	if c.terminalJournal == nil {
		return errs.New(errs.KindInternal, "agent: terminal journal is unavailable")
	}
	ack, err := buildTaskAcknowledgement(result)
	if err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return c.terminalJournal.PutPending(bounded, ack)
}

func (c *Client) sendTaskAck(stream agentStream, result TaskResult) error {
	ack, err := buildTaskAcknowledgement(result)
	if err != nil {
		return err
	}
	if result.Backup != nil {
		records, err := c.terminalJournal.List(context.Background())
		if err != nil {
			return err
		}
		for _, record := range records {
			if record.Ack.TaskId != ack.TaskId {
				continue
			}
			if !proto.Equal(record.Ack, ack) {
				return errs.New(errs.KindStateConflict, "agent: terminal result changed after journaling")
			}
			if record.Phase != agentterminaljournal.PhasePending {
				return sendTerminalJournalRecord(stream, record)
			}
			return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_TaskAck{TaskAck: record.Ack}})
		}
		return errs.New(errs.KindStateConflict, "agent: terminal result was not journaled")
	}
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_TaskAck{TaskAck: ack}})
}
