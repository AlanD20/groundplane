package taskcheckpoint

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *ReleaseCheckpointService) FailRelease(ctx context.Context, authorization agentregistration.WorkAuthorization, request *agentpb.ReleaseWorkFailureRequest) (*agentpb.ReleaseWorkFailureAck, error) {
	if ctx == nil || service == nil || request == nil || executionplan.RejectUnknown(request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Release failure request is invalid")
	}
	reason := releaseexecution.FailureReason(request.Reason)
	if reason != releaseexecution.ExecutionFailed && reason != releaseexecution.DeadlineExpired && reason != releaseexecution.ExecutionInterrupted {
		return nil, errs.New(errs.KindValidationFailed, "Release failure reason is unsupported")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	view, err := authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(claimed, view.Attempt.Attempt) || service.outcomes[view.Work.Item.Item.Operation] == nil {
		return nil, errs.New(errs.KindStateConflict, "Release failure does not match its saved attempt")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		if !view.ActiveAttempt || !view.CanSettleResult() {
			return nil, errs.New(errs.KindStateConflict, "Release failure no longer owns its attempt")
		}
		owner := service.outcomes[view.Work.Item.Item.Operation]
		if owner == nil {
			return nil, errs.New(errs.KindStateConflict, "Release failure owner is unavailable")
		}
		if err := owner.SettleFailed(ctx, view, reason, guards, time.Now().UTC()); err != nil {
			return nil, err
		}
		view, err = authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
		if err != nil {
			return nil, err
		}
	}
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil ||
		(attempt.Outcome != workexecution.Failed && attempt.Outcome != workexecution.Cancelled && attempt.Outcome != workexecution.Superseded) {
		return nil, errs.New(errs.KindStateConflict, "saved Release outcome is not this failure")
	}
	key := releasework.FailureKey(attempt.Result.ID)
	read, err := service.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "saved Release failure evidence is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
		return nil, errs.New(errs.KindStateConflict, "Release failure evidence revision is invalid")
	}
	failure, err := releaseexecution.DecodeFailure(attempt, value.Value, *attempt.Result)
	if err != nil || failure.Reason != reason || !failure.RecordedAt.Equal(*attempt.FinishedAt) {
		return nil, errs.New(errs.KindStateConflict, "Release failure replay differs from the recorded result")
	}
	digest, err := hex.DecodeString(attempt.Result.SHA256)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	ack := &agentpb.ReleaseWorkFailureAck{Attempt: proto.CloneOf(request.Attempt), Evidence: &agentpb.WorkInputReference{InputId: attempt.Result.ID, Sha256: digest},
		Failure: append([]byte(nil), value.Value...), Outcome: string(attempt.Outcome), FinishedAt: timestamppb.New(*attempt.FinishedAt)}
	if attempt.StartedAt != nil {
		ack.StartedAt = timestamppb.New(*attempt.StartedAt)
	}
	return ack, nil
}
