package blueprintrelease

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LoadActiveHook delivers only the hook selected by the committed Release
// cursor. The caller must authenticate the ResumeView's executor before calling;
// delivery itself does not grant another Script start or choose newer intent.
func (service *Service) LoadActiveHook(ctx context.Context, view workqueue.ResumeView) (*taskplanning.ScriptWorkDelivery, error) {
	if service == nil || service.artifacts == nil {
		return nil, errs.New(errs.KindInternal, "Release hook delivery is not configured")
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
