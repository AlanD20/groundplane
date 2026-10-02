package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A terminal race preserves both the native Task outcome and the worker's
// original report. A live conflict never enters this delivery path.
func (s *Server) sendRejectedBackupTaskTerminal(
	stream agentpb.AgentChannel_ConnectServer, session *Session, agentID string, agentGeneration uint64,
	report *agentpb.TaskAck,
) (bool, error) {
	if _, err := executionplan.TaskAcknowledgementSHA256(report); err != nil {
		return false, err
	}
	task, err := s.tasks.GetTask(stream.Context(), report.TaskId)
	if err != nil {
		return false, err
	}
	if !taskjournal.IsTerminalTaskStatus(task.Record.Status) {
		return false, nil
	}
	switch task.Record.Type {
	case taskjournal.TaskBackup, taskjournal.TaskBackupPrune, taskjournal.TaskRestore:
	default:
		return false, nil
	}
	// Receipt resolution and delivery publication prove the full assignment,
	// plan, native revision, execution epoch and retained report digest.
	return true, s.sendTaskTerminalReceipt(stream, session, agentID, agentGeneration, report, true)
}
