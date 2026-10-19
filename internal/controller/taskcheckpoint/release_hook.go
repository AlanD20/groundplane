package taskcheckpoint

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *ReleaseCheckpointService) LoadReleaseHook(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ReleaseHookRequest,
) (*agentpb.ReleaseHookDelivery, error) {
	if service == nil || ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Release hook request is invalid")
	}
	if _, err := authorization.Conditions(); err != nil {
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
	if !view.ActiveAttempt || !view.CurrentExecutor || !workexecution.SameAssignment(claimed, view.Attempt.Attempt) ||
		view.Attempt.Attempt.StartedAt == nil || view.Work.Task.Task.FinishedAt != nil || view.Work.Task.Task.CollectionStartedAt != nil {
		return nil, errs.New(errs.KindStateConflict, "Release hook request does not own an active started attempt")
	}
	if service.restoring(view) {
		owner := service.restorations[view.Work.Item.Item.Operation]
		if owner == nil {
			return nil, errs.New(errs.KindStateConflict, "Restoration hook owner is unavailable")
		}
		input, err := owner.LoadRecoveryInput(ctx, view)
		if err != nil {
			return nil, err
		}
		saved, err := releasework.LoadRecovery(ctx, service.store, input, view.Work.ReadRevision)
		if err != nil {
			return nil, err
		}
		if saved.Record != nil && saved.Record.Report.Progress.Phase == releaseexecution.FailureHooks {
			captured, err := owner.LoadRecoveryHookOwnership(ctx, view, request.ExecutionId)
			if err != nil {
				return nil, err
			}
			if captured != nil && captured.AttemptID != "" && captured.AttemptID != view.Attempt.Attempt.ID {
				if saved.Record.Proofs[0] == nil {
					return nil, errs.New(errs.KindStateConflict, "interrupted failure hook has no quiescence proof")
				}
				return interruptedHookDelivery(view.Attempt.Attempt, captured, *saved.Record.Proofs[0])
			}
			return service.loadActiveHookDelivery(ctx, view, request)
		}
		captured, err := owner.LoadRecoveryHookOwnership(ctx, view, request.ExecutionId)
		if err != nil {
			return nil, err
		}
		if captured == nil || captured.Run == nil || captured.Run.ScriptExecutionId != request.ExecutionId ||
			captured.Projection == nil {
			return nil, errs.New(errs.KindStateConflict, "captured hook cleanup ownership is missing")
		}
		digest, err := hex.DecodeString(captured.Input.SHA256)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		attempt, err := workexecution.EncodeAgentAttempt(view.Attempt.Attempt)
		if err != nil {
			return nil, err
		}
		return proto.CloneOf(&agentpb.ReleaseHookDelivery{Attempt: attempt, OperationId: captured.OperationID,
			Input: &agentpb.WorkInputReference{
				InputId: captured.Input.ID,
				Sha256:  digest,
			}, Run: captured.Run, Projection: captured.Projection, Cleanup: captured.Cleanup}), nil
	}
	return service.loadActiveHookDelivery(ctx, view, request)
}

func (service *ReleaseCheckpointService) loadActiveHookDelivery(
	ctx context.Context,
	view workqueue.ResumeView,
	request *agentpb.ReleaseHookRequest,
) (*agentpb.ReleaseHookDelivery, error) {
	owner := service.forwards[view.Work.Item.Item.Operation]
	if owner == nil {
		return nil, errs.New(errs.KindStateConflict, "Release hook owner is unavailable")
	}
	delivery, err := owner.LoadActiveHook(ctx, view)
	if delivery != nil {
		defer delivery.Clear()
	}
	if err != nil {
		return nil, err
	}
	if delivery == nil || delivery.Captured == nil || delivery.Captured.Run == nil ||
		delivery.Captured.Run.ScriptExecutionId != request.ExecutionId {
		return nil, errs.New(errs.KindStateConflict, "requested hook is not selected by the Release cursor")
	}
	execution := delivery.Captured.Input.Execution
	digest, err := hex.DecodeString(execution.Input.SHA256)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	attempt, err := workexecution.EncodeAgentAttempt(view.Attempt.Attempt)
	if err != nil {
		return nil, err
	}
	return proto.CloneOf(&agentpb.ReleaseHookDelivery{
		Attempt: attempt, OperationId: execution.OperationID,
		Input: &agentpb.WorkInputReference{InputId: execution.Input.ID, Sha256: digest},
		Run:   delivery.Captured.Run, Snapshot: delivery.Captured.Snapshot, Projection: delivery.Captured.Projection,
		Body: delivery.Captured.Body, Entries: delivery.Entries, Checkpoint: delivery.Checkpoint,
	}), nil
}
