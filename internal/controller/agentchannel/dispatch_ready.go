package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (s *Server) dispatchReady(
	stream agentpb.AgentChannel_ConnectServer,
	session *Session,
	agentID string,
	authorization Authorization,
	capacity int32,
	delivered map[string]string,
	quarantined map[string]string,
) error {
	if capacity == 0 || !session.AssignmentsAllowed() {
		return nil
	}
	recovered, err := s.tasks.ListAgentAssignments(
		stream.Context(),
		agentID,
		authorization.Generation,
		authorization.Config.MaxConcurrentTasks,
	)
	if err != nil {
		return err
	}
	if !session.AssignmentsAllowed() {
		return nil
	}
	remaining := capacity
	for _, assignment := range recovered {
		if !session.AssignmentsAllowed() {
			return nil
		}
		if delivered[assignment.Task.Record.ID] == assignment.Assignment.Record.AssignmentID ||
			quarantined[assignment.Task.Record.ID] == assignment.Assignment.Record.AssignmentID {
			continue
		}
		if assignment.Task.Record.Params[etcd.TaskReleasePublicationParam] != "" {
			assignment, err = s.tasks.ReconnectAgentAssignment(stream.Context(), assignment)
			if err != nil {
				return err
			}
			if controllerCompletedAssignment(assignment) {
				continue
			}
		}
		if err := validateAgentDispatchClaim(
			assignment,
			agentID,
			authorization.Generation,
		); err != nil {
			return err
		}
		if assignment.Assignment.Record.ExecutionMode == etcd.TaskExecutionModeForward &&
			!s.now().UTC().Before(assignment.Assignment.Record.Deadline.UTC()) {
			continue
		}
		if remaining == 0 {
			break
		}
		sent, err := s.dispatchTaskAssignment(session, stream, assignment, true)
		if err != nil {
			return err
		}
		if sent {
			remaining--
			session.recordDispatchCapacity(remaining)
			delivered[assignment.Task.Record.ID] = assignment.Assignment.Record.AssignmentID
		} else {
			quarantined[assignment.Task.Record.ID] = assignment.Assignment.Record.AssignmentID
		}
	}
	for remaining > 0 {
		if !session.AssignmentsAllowed() {
			return nil
		}
		assignment, found, err := s.tasks.ClaimNextTask(
			stream.Context(),
			agentID,
			authorization.Generation,
			s.now().UTC(),
		)
		if err != nil {
			return err
		}
		if !found || !session.AssignmentsAllowed() {
			return nil
		}
		if err := validateAgentDispatchClaim(
			assignment,
			agentID,
			authorization.Generation,
		); err != nil {
			return err
		}
		if !session.AssignmentsAllowed() {
			return nil
		}
		sent, err := s.dispatchTaskAssignment(session, stream, assignment, false)
		if err != nil {
			return err
		}
		if !session.AssignmentsAllowed() {
			return nil
		}
		if sent {
			remaining--
			session.recordDispatchCapacity(remaining)
			delivered[assignment.Task.Record.ID] = assignment.Assignment.Record.AssignmentID
		} else {
			quarantined[assignment.Task.Record.ID] = assignment.Assignment.Record.AssignmentID
		}
	}
	session.recordDispatchCapacity(remaining)
	return nil
}

func validateAgentDispatchClaim(
	assignment etcd.TaskAssignment,
	agentID string,
	generation uint64,
) error {
	record := assignment.Assignment.Record
	if record.Executor != etcd.TaskExecutorAgent || record.AgentID != agentID ||
		record.AgentGeneration != generation || record.TaskID != assignment.Task.Record.ID ||
		ids.Validate(ids.KindAssignment, record.AssignmentID) != nil {
		return errs.New(
			errs.KindInternal,
			"durable Agent Task claim does not match its dispatch session",
		)
	}
	return nil
}

func controllerCompletedAssignment(assignment etcd.TaskAssignment) bool {
	switch assignment.Task.Record.Status {
	case etcd.TaskStatusCompleted, etcd.TaskStatusFailed, etcd.TaskStatusAborted:
		return true
	default:
		return false
	}
}
