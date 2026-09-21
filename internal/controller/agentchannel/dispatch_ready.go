package agentchannel

import (
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"log/slog"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type assignmentDispatch uint8

const (
	assignmentDeferred assignmentDispatch = iota
	assignmentSent
	assignmentQuarantined
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
	if capacity == 0 {
		return nil
	}
	release, allowed := session.beginDispatch()
	if !allowed {
		return nil
	}
	defer release()
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
	claimCapacity := authorization.Config.MaxConcurrentTasks - int32(len(recovered))
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
				claimCapacity++
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
		disposition, err := s.dispatchTaskAssignment(session, stream, assignment, true)
		if err != nil {
			return err
		}
		switch disposition {
		case assignmentSent:
			remaining--
			session.recordDispatchCapacity(remaining)
			delivered[assignment.Task.Record.ID] = assignment.Assignment.Record.AssignmentID
		case assignmentQuarantined:
			quarantined[assignment.Task.Record.ID] = assignment.Assignment.Record.AssignmentID
		case assignmentDeferred:
			return nil
		}
	}
	for remaining > 0 && claimCapacity > 0 {
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
		claimCapacity--
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
		disposition, err := s.dispatchTaskAssignment(session, stream, assignment, false)
		if err != nil {
			return err
		}
		switch disposition {
		case assignmentSent:
			remaining--
			session.recordDispatchCapacity(remaining)
			delivered[assignment.Task.Record.ID] = assignment.Assignment.Record.AssignmentID
		case assignmentQuarantined:
			quarantined[assignment.Task.Record.ID] = assignment.Assignment.Record.AssignmentID
		case assignmentDeferred:
			return nil
		}
	}
	session.recordDispatchCapacity(remaining)
	return nil
}

func (s *Server) dispatchTaskAssignment(
	session *Session,
	stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment,
	recovered bool,
) (assignmentDispatch, error) {
	assignment, err := s.taskAssignmentMessage(stream.Context(), claim, recovered)
	if err != nil {
		slog.Error(
			"controller: quarantine Agent Task assignment",
			slog.String("task_id", claim.Task.Record.ID),
			slog.String("assignment_id", claim.Assignment.Record.AssignmentID),
			slog.Any("error", err),
		)
		return assignmentQuarantined, nil
	}
	sent, err := s.dispatchResolvedTaskAssignment(session, stream, claim, assignment, recovered)
	if sent {
		return assignmentSent, err
	}
	return assignmentDeferred, err
}

func validateAgentDispatchClaim(
	assignment etcd.TaskAssignment,
	agentID string,
	generation uint64,
) error {
	record := assignment.Assignment.Record
	if record.Executor != taskjournal.TaskExecutorAgent || record.AgentID != agentID ||
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
	case taskjournal.TaskStatusCompleted, taskjournal.TaskStatusFailed, taskjournal.TaskStatusAborted:
		return true
	default:
		return false
	}
}
