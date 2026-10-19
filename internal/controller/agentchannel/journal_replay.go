package agentchannel

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/workreceipt"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (server *Server) replayWorkJournal(
	ctx context.Context,
	session *Session,
	generation uint64,
	request *agentpb.WorkJournalReplayRequest,
) (*agentpb.WorkJournalReplayAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || proto.Size(request) > workreceipt.MaximumBytes {
		return nil, errs.New(errs.KindValidationFailed, "startup result replay is malformed or oversized")
	}
	forms := 0
	for _, present := range []bool{request.Complete, len(request.Report) != 0, request.UnreportedAttempt != nil, request.UncommittedAttemptId != ""} {
		if present {
			forms++
		}
	}
	if forms != 1 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"startup journal request requires exactly one replay, inventory or completion form",
		)
	}
	authorization, err := server.authenticatedWorkIdentity(ctx, session, generation)
	if err != nil {
		return nil, err
	}
	if request.Complete {
		return &agentpb.WorkJournalReplayAck{Complete: true}, nil
	}
	if request.UnreportedAttempt != nil || request.UncommittedAttemptId != "" {
		attemptID := request.UncommittedAttemptId
		if request.UnreportedAttempt != nil {
			attemptID = request.UnreportedAttempt.AttemptId
		}
		bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if err := server.workDispatch.AccountJournal(bounded, authorization, attemptID, request.UnreportedAttempt); err != nil {
			return nil, err
		}
		return &agentpb.WorkJournalReplayAck{AccountedAttemptId: attemptID}, nil
	}
	message := new(agentpb.AgentMessage)
	if err := proto.Unmarshal(request.Report, message); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "startup result report cannot be decoded")
	}
	identity, _, supported, err := workreceipt.Describe(message)
	if err != nil {
		return nil, err
	}
	if !supported {
		return nil, errs.New(errs.KindValidationFailed, "startup replay cannot authorize execution or deliver inputs")
	}
	previous := workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     identity.HostId,
		AgentID:    identity.AgentId,
		Generation: identity.ExecutorGeneration,
	}
	retained, err := authorization.ForReceipt(previous)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(request.Report)
	ack := &agentpb.WorkJournalReplayAck{ReportSha256: digest[:]}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	reply, err := server.terminalReport(bounded, retained, message)
	if err != nil {
		slog.WarnContext(
			ctx,
			"retained Agent result requires reconciliation",
			"task_id",
			identity.TaskId,
			"work_id",
			identity.WorkItemId,
			"attempt_id",
			identity.AttemptId,
			"error",
			err,
		)
		problem := errs.New(errs.KindInternal, "").ToProblem()
		var productError *errs.Error
		if errors.As(err, &productError) {
			problem = productError.ToProblem()
		}
		ack.FailureCode, ack.FailureDetail = string(problem.Code), problem.Detail
		return ack, nil
	}
	if err := workreceipt.Match(message, reply); err != nil {
		return nil, err
	}
	ack.Acknowledgement, err = proto.MarshalOptions{Deterministic: true}.Marshal(reply)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return ack, nil
}
