package agentchannel

import (
	"log/slog"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (server *Server) dispatchWork(stream agentpb.AgentChannel_ConnectServer, session *Session, generation uint64, capacity, configuredCapacity int32, delivered map[string]string, payloads *assignmentPayloadDelivery) error {
	if server.workDispatch == nil {
		return errs.New(errs.KindInternal, "native Agent dispatch is not configured")
	}
	if configuredCapacity < 1 || configuredCapacity > 256 || capacity < 0 || capacity > configuredCapacity {
		return errs.New(errs.KindValidationFailed, "Agent dispatch capacity exceeds its configuration")
	}
	release, allowed := session.beginDispatch()
	if !allowed {
		return nil
	}
	defer release()
	authorization, err := server.authenticatedWorkIdentity(stream.Context(), session, generation)
	if err != nil {
		return err
	}
	readinessIssues, err := server.workDispatch.Ready(stream.Context(), authorization, uint16(configuredCapacity))
	for _, issue := range readinessIssues {
		slog.ErrorContext(stream.Context(), "Agent work retains unresolved effects", "task_id", issue.TaskID, "work_id", issue.WorkID, "error", issue.Err)
	}
	if err != nil {
		return err
	}
	remaining := capacity
	issues, err := server.workDispatch.Dispatch(stream.Context(), authorization, capacity, delivered, func(message *agentpb.ControllerMessage) (bool, error) {
		cancel := func() {}
		if assignment := message.GetBackupWorkAssignment(); assignment != nil {
			var err error
			cancel, err = payloads.prepareBackupPayloads(stream, assignment, authorization, server.sendBackupPayloads)
			if err != nil {
				return false, err
			}
		}
		if assignment := message.GetBackupRecoveryAssignment(); assignment != nil {
			var err error
			cancel, err = payloads.prepareBackupRecoveryPayloads(stream, assignment, authorization, server.receiveRecoveryVolumeManifest)
			if err != nil {
				return false, err
			}
		}
		sent, err := session.sendAssignment(func() error { return stream.Send(message) })
		if !sent || err != nil {
			cancel()
		}
		if sent && message.GetWorkCancel() == nil {
			remaining--
			session.recordDispatchCapacity(remaining)
		}
		return sent, err
	})
	for _, issue := range issues {
		slog.ErrorContext(stream.Context(), "Agent work requires attention", "task_id", issue.TaskID, "work_id", issue.WorkID, "error", issue.Err)
	}
	return err
}
