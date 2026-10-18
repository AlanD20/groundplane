package taskcheckpoint

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ReleaseCheckpointOwner owns effect admission and evidence, not the transport.
// Begin compares current permission; Confirm verifies the exact captured effect.
// Both return comparisons that commit atomically with the shared checkpoint.
type ReleaseCheckpointOwner interface {
	LoadRecoveryHookOwnership(context.Context, workqueue.ResumeView, string) (*etcd.ScriptWorkOwnership, error)
	LoadRecoveryInput(context.Context, workqueue.ResumeView) (releaseexecution.RecoveryInput, error)
	PrepareRecoveryEvidence(context.Context, workqueue.ResumeView, releaseexecution.RecoveryInput, releasework.RecoverySnapshot, releaseexecution.RecoveryReport) (workqueue.Change, error)
	SettleCompleted(context.Context, workqueue.ResumeView, []keyvalue.Condition, time.Time) error
	SettleRecovered(context.Context, workqueue.ResumeView, []keyvalue.Condition, time.Time) error
	SettleFailed(context.Context, workqueue.ResumeView, releaseexecution.FailureReason, []keyvalue.Condition, time.Time) error
	LoadActiveConfiguration(context.Context, workqueue.ResumeView) (releaseexecution.Input, releaseexecution.CheckpointReport, []taskmaterialization.Record, error)
	LoadActiveRecoveryConfiguration(context.Context, workqueue.ResumeView) (releaseexecution.RecoveryInput, releaseexecution.RecoveryReport, []taskmaterialization.Record, error)
	LoadActiveHook(context.Context, workqueue.ResumeView) (*taskplanning.ScriptWorkDelivery, error)
	LoadCheckpointInput(context.Context, workqueue.ResumeView) (releaseexecution.Input, error)
	PrepareCheckpointEvidence(context.Context, workqueue.ResumeView, releaseexecution.Input, releasework.Snapshot, releaseexecution.CheckpointReport) (workqueue.Change, error)
}

type ReleaseCheckpointRegistration struct {
	Operation workexecution.Operation
	Owner     ReleaseCheckpointOwner
}

type ReleaseCheckpointService struct {
	diagnostics *executiondiagnostics.Repository
	logger      *slog.Logger
	files       CapturedFileReader
	queue       *workqueue.Repository
	store       releasework.Reader
	owners      map[workexecution.Operation]ReleaseCheckpointOwner
}

func NewReleaseCheckpointService(queue *workqueue.Repository, store releasework.Reader, files CapturedFileReader, diagnostics *executiondiagnostics.Repository, logger *slog.Logger, registrations []ReleaseCheckpointRegistration) (*ReleaseCheckpointService, error) {
	if queue == nil || store == nil || files == nil || diagnostics == nil || logger == nil || len(registrations) == 0 {
		return nil, errs.New(errs.KindInternal, "Release checkpoints require storage and operation owners")
	}
	owners := make(map[workexecution.Operation]ReleaseCheckpointOwner, len(registrations))
	for _, registration := range registrations {
		if !workexecution.ValidOperation(registration.Operation) || registration.Owner == nil || owners[registration.Operation] != nil {
			return nil, errs.New(errs.KindInternal, "Release checkpoint operation is invalid or duplicated")
		}
		owners[registration.Operation] = registration.Owner
	}
	return &ReleaseCheckpointService{queue: queue, store: store, files: files, owners: owners, diagnostics: diagnostics, logger: logger}, nil
}

func (service *ReleaseCheckpointService) CheckpointRelease(ctx context.Context, authorization agentregistration.WorkAuthorization, request *agentpb.ReleaseWorkCheckpointRequest) (*agentpb.ReleaseWorkCheckpointAck, error) {
	if ctx == nil || service == nil || request == nil || len(request.ProtoReflect().GetUnknown()) != 0 ||
		len(request.Report) == 0 || len(request.Report) > releaseexecution.MaximumCheckpointBytes {
		return nil, errs.New(errs.KindValidationFailed, "Release checkpoint request is invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	view, err := service.queue.ReadResume(ctx, claimed.TaskID, claimed.WorkItemID, claimed.ID, authorization.Identity())
	if err != nil {
		return nil, err
	}
	if !view.ActiveAttempt || !view.CurrentExecutor || !workexecution.SameAssignment(view.Attempt.Attempt, claimed) ||
		view.Attempt.Attempt.StartedAt == nil || view.Work.Task.Task.FinishedAt != nil || view.Work.Task.Task.CollectionStartedAt != nil || !view.Work.Task.Task.PublicationComplete {
		return nil, errs.New(errs.KindStateConflict, "Release checkpoint no longer owns an active started attempt")
	}
	owner := service.owners[view.Work.Item.Item.Operation]
	if owner == nil {
		return nil, errs.New(errs.KindStateConflict, "Release operation has no checkpoint owner")
	}
	input, err := owner.LoadCheckpointInput(ctx, view)
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(input.Attempt, view.Attempt.Attempt) || input.Attempt.StartedAt == nil || !input.Attempt.StartedAt.Equal(*view.Attempt.Attempt.StartedAt) {
		return nil, errs.New(errs.KindInternal, "Release checkpoint owner returned a different attempt")
	}
	report, err := releaseexecution.DecodeCheckpointRequest(input, request)
	if err != nil {
		return nil, err
	}
	if report.Progress.UpdatedAt.After(time.Now().UTC()) {
		return nil, errs.New(errs.KindValidationFailed, "Release checkpoint time is in the future")
	}
	previous, err := releasework.Load(ctx, service.store, input, view.Work.ReadRevision)
	if err != nil {
		return nil, err
	}
	change, err := releasework.Prepare(view, input, previous, report)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	// Exact replay returns saved permission/evidence; it must not seek a new
	// cutover grant or repeat an operation-owned effect.
	if previous.Record != nil && previous.Record.CheckpointSequence == report.Sequence && releaseexecution.EqualProgress(previous.Record.Progress, report.Progress) {
		return releaseexecution.EncodeCheckpointAck(input, report)
	}
	if report.Progress.InFlight && !time.Now().UTC().Before(input.Attempt.Deadline) {
		return nil, errs.New(errs.KindStateConflict, "Release deadline expired before the next effect was authorized")
	}
	var hookConditions []keyvalue.Condition
	if previous.Record != nil && !report.Progress.InFlight &&
		(previous.Record.Progress.Phase == releaseexecution.PreHooks || previous.Record.Progress.Phase == releaseexecution.PostHooks) {
		hookConditions, err = releasework.HookCompletionConditions(ctx, service.store, view, input, previous, report)
		if err != nil {
			return nil, err
		}
	}
	if previous.Record != nil && !report.Progress.InFlight &&
		(previous.Record.Progress.Phase == releaseexecution.Rollout || previous.Record.Progress.Phase == releaseexecution.Configuration) {
		conditions, err := releasework.EffectCompletionConditions(ctx, service.store, view, input, previous, report)
		if err != nil {
			return nil, err
		}
		hookConditions = append(hookConditions, conditions...)
	}
	evidence, err := owner.PrepareCheckpointEvidence(ctx, view, input, previous, report)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ZeroMutationBytes(evidence.Mutations)
	if len(evidence.Conditions) == 0 {
		return nil, errs.New(errs.KindInternal, "Release checkpoint requires operation evidence comparisons")
	}
	result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, workqueue.Change{Conditions: hookConditions}, evidence, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Release checkpoint authority changed; reload the saved attempt")
	}
	service.reportDeployment(ctx, input, previous, report)
	return releaseexecution.EncodeCheckpointAck(input, report)
}
