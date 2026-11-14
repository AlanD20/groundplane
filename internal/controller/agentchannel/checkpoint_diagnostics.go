package agentchannel

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type CheckpointDiagnostics interface {
	CheckpointRejected(context.Context, workexecution.Attempt, string) error
}

func (server *Server) ConfigureCheckpointDiagnostics(diagnostics CheckpointDiagnostics) {
	server.checkpointDiagnostics = diagnostics
}

func (server *Server) reportCheckpointRejection(ctx context.Context, identity *agentpb.WorkAttemptIdentity,
	detail string,
) {
	if server.checkpointDiagnostics == nil {
		return
	}
	attempt, err := workexecution.DecodeAgentAttempt(identity, workexecution.Identity{
		Executor: workexecution.Agent, HostID: identity.HostId, AgentID: identity.AgentId,
		Generation: identity.ExecutorGeneration})
	if err != nil {
		return
	}
	// The detail is the public projection, never raw subprocess output.
	if len(detail) > 1600 {
		detail = "The Controller refused the execution checkpoint; inspect the correlated Controller log."
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := server.checkpointDiagnostics.CheckpointRejected(bounded, attempt, detail); err != nil {
		slog.WarnContext(bounded, "Checkpoint refusal diagnostics could not be saved",
			"task_id", attempt.TaskID, "work_id", attempt.WorkItemID, "attempt_id", attempt.ID,
			"error", err)
	}
}
