package blueprintrelease

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LoadActiveHook delivers only the hook selected by the committed Release
// cursor. The caller must authenticate the ResumeView's executor before calling;
// delivery itself does not grant another Script start or choose newer intent.
func (service *Service) LoadActiveHook(
	ctx context.Context,
	view workqueue.ResumeView,
) (*taskplanning.ScriptWorkDelivery, error) {
	if service == nil || service.artifacts == nil {
		return nil, errs.New(errs.KindInternal, "Release hook delivery is not configured")
	}
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		return service.loadActiveFailureHook(ctx, view)
	}
	input, err := service.LoadCheckpointInput(ctx, view)
	if err != nil {
		return nil, err
	}
	progress, err := service.ledger.LoadReleaseWorkProgress(ctx, view, input)
	if err != nil {
		return nil, err
	}
	if progress.Record == nil {
		return nil, errs.New(errs.KindStateConflict, "Release has not authorized a hook boundary")
	}
	hook, err := progress.Record.Progress.ActiveHook(input)
	if err != nil || progress.Record.ActiveHookID != hook.GetExecutionId() {
		return nil, errs.New(errs.KindStateConflict, "Release cursor does not select a captured hook")
	}
	return service.artifacts.ResolveReleaseScriptWorkArtifacts(ctx, view.Work, hook.ExecutionId,
		workexecution.Input{ID: hook.Input.InputId, SHA256: hex.EncodeToString(hook.Input.Sha256)})
}

func (service *Service) loadActiveFailureHook(
	ctx context.Context,
	view workqueue.ResumeView,
) (*taskplanning.ScriptWorkDelivery, error) {
	if !view.ActiveAttempt || !view.CurrentExecutor || view.Checkpoint.Record == nil {
		return nil, errs.New(errs.KindStateConflict, "failure hook requires its active recovery executor")
	}
	input, err := service.LoadRecoveryInput(ctx, view)
	if err != nil {
		return nil, err
	}
	saved, err := service.ledger.LoadReleaseRecoveryProgress(ctx, view, input)
	if err != nil {
		return nil, err
	}
	if saved.Record == nil || saved.Record.Report.Progress.Phase != releaseexecution.FailureHooks {
		return nil, errs.New(errs.KindStateConflict, "recovery has not authorized a failure hook")
	}
	hook, err := saved.Record.Report.Progress.ActiveHook(input)
	if err != nil || saved.Record.ActiveHookID != hook.GetExecutionId() {
		return nil, errs.New(errs.KindStateConflict, "failure hook differs from the captured recovery selection")
	}
	checkpoint, err := saved.Record.Report.Checkpoint(input)
	if err != nil || view.Checkpoint.Record.Sequence < checkpoint.Sequence ||
		view.Checkpoint.Record.Sequence == checkpoint.Sequence &&
			(saved.Revision != view.Checkpoint.Revision || !releaseexecution.EqualCheckpoint(checkpoint, *view.Checkpoint.Record)) {
		return nil, errs.New(errs.KindStateConflict, "failure hook no longer owns its saved recovery boundary")
	}
	return service.artifacts.ResolveReleaseScriptWorkArtifacts(ctx, view.Work, hook.ExecutionId,
		workexecution.Input{ID: hook.Input.InputId, SHA256: hex.EncodeToString(hook.Input.Sha256)})
}
