package taskcheckpoint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *ReleaseCheckpointService) CheckpointRecovery(ctx context.Context, authorization agentregistration.WorkAuthorization, request *agentpb.ReleaseRecoveryCheckpointRequest) (*agentpb.ReleaseRecoveryCheckpointAck, error) {
	if service == nil || ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || len(request.Report) == 0 || len(request.Report) > releaseexecution.MaximumCheckpointBytes {
		return nil, errs.New(errs.KindValidationFailed, "Release recovery checkpoint is invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	if claimed.Direction != workexecution.Recovery {
		return nil, errs.New(errs.KindStateConflict, "Recovery checkpoint requires a cleanup attempt")
	}
	view, err := service.queue.ReadResume(ctx, claimed.TaskID, claimed.WorkItemID, claimed.ID, authorization.Identity())
	if err != nil {
		return nil, err
	}
	if !view.ActiveAttempt || !view.CurrentExecutor || !workexecution.SameAssignment(claimed, view.Attempt.Attempt) || view.Attempt.Attempt.StartedAt == nil ||
		view.Work.Task.Task.FinishedAt != nil || view.Work.Task.Task.CollectionStartedAt != nil || !view.Work.Task.Task.PublicationComplete {
		return nil, errs.New(errs.KindStateConflict, "Recovery checkpoint no longer owns an active cleanup attempt")
	}
	owner := service.owners[view.Work.Item.Item.Operation]
	if owner == nil {
		return nil, errs.New(errs.KindStateConflict, "Recovery operation owner is unavailable")
	}
	input, err := owner.LoadRecoveryInput(ctx, view)
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(input.Input.Attempt, view.Attempt.Attempt) || input.Input.Attempt.StartedAt == nil ||
		!input.Input.Attempt.StartedAt.Equal(*view.Attempt.Attempt.StartedAt) {
		return nil, errs.New(errs.KindInternal, "Recovery owner returned a different attempt")
	}
	report, err := releaseexecution.DecodeRecoveryRequest(input, request)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if report.Progress.UpdatedAt.After(now) {
		return nil, errs.New(errs.KindValidationFailed, "Recovery checkpoint time is in the future")
	}
	previous, err := releasework.LoadRecovery(ctx, service.store, input, view.Work.ReadRevision)
	if err != nil {
		return nil, err
	}
	change, err := releasework.PrepareRecovery(view, input, previous, report)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	if previous.Record != nil && releaseexecution.EqualRecoveryProgress(previous.Record.Report.Progress, report.Progress) {
		service.reportRecovery(ctx, input, previous, report)
		return releaseexecution.EncodeRecoveryAck(input, report)
	}
	if !now.Before(input.Input.Attempt.Deadline) {
		return nil, errs.New(errs.KindStateConflict, "Recovery attempt deadline expired")
	}
	// The owner verifies exact captured sources and each completed action's
	// evidence. The transport cannot grant restoration from a status string.
	evidence, err := owner.PrepareRecoveryEvidence(ctx, view, input, previous, report)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(evidence.Mutations)
	if len(evidence.Conditions) == 0 {
		return nil, errs.New(errs.KindInternal, "Recovery checkpoint requires operation evidence comparisons")
	}
	change.Conditions = append(change.Conditions, guards...)
	result, err := service.queue.Commit(ctx, evidence, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Recovery checkpoint authority changed; reload the saved cleanup attempt")
	}
	service.reportRecovery(ctx, input, previous, report)
	return releaseexecution.EncodeRecoveryAck(input, report)
}
