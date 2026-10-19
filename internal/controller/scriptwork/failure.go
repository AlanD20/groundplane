package scriptwork

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *Service) FailScript(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ScriptWorkFailureRequest,
) (*agentpb.ScriptWorkFailureAck, error) {
	if service == nil || ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Script failure request is invalid")
	}
	claimed, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	if _, err := authorization.Conditions(); err != nil {
		return nil, err
	}
	view, err := authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return nil, err
	}
	if view.Work.Item.Item.Operation != workexecution.Script ||
		!workexecution.SameAssignment(claimed, view.Attempt.Attempt) {
		return nil, errs.New(errs.KindStateConflict, "Script failure belongs to another assignment")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		if err := service.repository.FailScriptWork(ctx, view, authorization, time.Now().UTC()); err != nil {
			return nil, err
		}
		view, err = authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
		if err != nil {
			return nil, err
		}
	}
	terminal := view.Attempt.Attempt
	if terminal.FinishedAt == nil || terminal.Result == nil || !workexecution.SameAssignment(claimed, terminal) {
		return nil, errs.New(errs.KindStateConflict, "Script failure has no saved settlement")
	}
	digest, err := hex.DecodeString(terminal.Result.SHA256)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return &agentpb.ScriptWorkFailureAck{Attempt: proto.CloneOf(request.Attempt), Outcome: string(terminal.Outcome),
		Evidence: &agentpb.WorkInputReference{
			InputId: terminal.Result.ID,
			Sha256:  digest,
		}, FinishedAt: timestamppb.New(*terminal.FinishedAt)}, nil
}
