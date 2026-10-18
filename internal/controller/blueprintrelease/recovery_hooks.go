package blueprintrelease

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) LoadRecoveryHookOwnership(ctx context.Context, view workqueue.ResumeView, executionID string) (*etcd.ScriptWorkOwnership, error) {
	if service == nil || service.artifacts == nil || !view.ActiveAttempt || !view.CurrentExecutor {
		return nil, errs.New(errs.KindStateConflict, "hook cleanup requires the current recovery executor")
	}
	input, err := service.LoadRecoveryInput(ctx, view)
	if err != nil {
		return nil, err
	}
	saved, err := service.ledger.LoadReleaseRecoveryProgress(ctx, view, input)
	if err != nil {
		return nil, err
	}
	if saved.Record == nil || view.Checkpoint.Record == nil || saved.Revision != view.Checkpoint.Revision ||
		!saved.Record.Report.Progress.InFlight || saved.Record.Report.Progress.Phase != releaseexecution.QuiesceForward {
		return nil, errs.New(errs.KindStateConflict, "hook cleanup requires its saved quiescence boundary")
	}
	checkpoint, err := saved.Record.Report.Checkpoint(input)
	if err != nil || !releaseexecution.EqualCheckpoint(checkpoint, *view.Checkpoint.Record) {
		return nil, errs.New(errs.KindStateConflict, "hook cleanup checkpoint differs from its saved receipt")
	}
	for _, hook := range input.Input.Release.Hooks {
		if hook.ExecutionId == executionID {
			return service.artifacts.ResolveReleaseScriptOwnership(ctx, view.Work, executionID,
				workexecution.Input{ID: hook.Input.InputId, SHA256: hex.EncodeToString(hook.Input.Sha256)})
		}
	}
	return nil, errs.New(errs.KindStateConflict, "requested Script is not a captured Release hook")
}
