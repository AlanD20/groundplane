package agentchannel

import (
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/workreceipt"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Each compiled operation owns authentication and typed decoding. The stream
// loop owns sending and clears transient file/hook bytes even when Send fails.
func (server *Server) nativeOperationReport(
	session *Session,
	generation uint64,
	message *agentpb.AgentMessage,
) (bool, *agentpb.ControllerMessage, error) {
	identity, kind, checkpoint, err := workreceipt.DescribeCheckpoint(message)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	if checkpoint {
		authorization, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
		if err != nil {
			return true, nil, taskStoreStatus(err)
		}
		if _, err := workexecution.DecodeAgentAttempt(identity, authorization.Identity()); err != nil {
			return true, nil, taskStoreStatus(err)
		}
	}
	for _, route := range []func(*Session, uint64, *agentpb.AgentMessage) (bool, *agentpb.ControllerMessage, error){
		server.resolverReport, server.backingReport, server.entryReport, server.componentReport,
		server.networkReport, server.volumeReport, server.removalRecoveryReport,
		server.backupWorkReport, server.lifecycleReport, server.attachReport,
	} {
		for attempt := 0; ; attempt++ {
			handled, reply, err := route(session, generation, message)
			if !handled {
				break
			}
			if !checkpoint || err == nil {
				return true, reply, err
			}
			// Only a failed atomic publication is retryable. Each call reloads
			// authority and progress; it never repeats an external effect.
			if status.Code(err) == codes.Aborted && attempt < 7 {
				timer := time.NewTimer(time.Duration(attempt+1) * 5 * time.Millisecond)
				select {
				case <-session.ctx.Done():
					timer.Stop()
					return true, nil, session.ctx.Err()
				case <-timer.C:
					continue
				}
			}
			if code := status.Code(err); code != codes.Aborted && code != codes.FailedPrecondition {
				return true, nil, err
			}
			slog.WarnContext(session.ctx, "Agent checkpoint was not accepted", "task_id", identity.TaskId,
				"work_id", identity.WorkItemId, "attempt_id", identity.AttemptId,
				"report_kind", kind, "publication_attempts", attempt+1, "error", err)
			server.reportCheckpointRejection(session.ctx, identity, status.Convert(err).Message())
			refusal, rejectErr := workreceipt.Reject(message, "state.conflict", status.Convert(err).Message())
			return true, refusal, rejectErr
		}
	}
	return false, nil, nil
}

func clearNativeOperationReply(reply *agentpb.ControllerMessage) {
	backingprovision.ClearPrivateDelivery(reply)
	if delivery := reply.GetEntryConfigurationFileDelivery(); delivery != nil {
		clear(delivery.Content)
		delivery.Content = nil
	}
	if delivery := reply.GetComponentConfigurationFileDelivery(); delivery != nil {
		clear(delivery.Content)
		delivery.Content = nil
	}
	if delivery := reply.GetServiceRemovalConfigurationFileDelivery(); delivery != nil {
		clear(delivery.Content)
		delivery.Content = nil
	}
	clearLifecycleHookReply(reply)
}
