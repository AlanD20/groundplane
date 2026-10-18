package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LoadReleaseScriptWork resolves a hook selected by the captured Release input.
// WorkInput is the binding written with native Release publication, while input
// is the hook's own immutable reference. Neither may be replaced by current state.
func (repository *ScriptRepository) LoadReleaseScriptWork(ctx context.Context, view workqueue.View,
	executionID string, input workexecution.Input,
) (ScriptWorkInput, error) {
	loaded, err := repository.loadCapturedReleaseScriptWork(ctx, view, executionID, input)
	if err != nil {
		return ScriptWorkInput{}, err
	}
	return repository.requireScriptWorkSources(ctx, view, loaded)
}

// Captured ownership remains readable for cleanup after executable source
// retention has closed. This read cannot authorize a Script start or retry.
func (repository *ScriptRepository) loadCapturedReleaseScriptWork(ctx context.Context, view workqueue.View,
	executionID string, input workexecution.Input,
) (ScriptWorkInput, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return ScriptWorkInput{}, err
	}
	task, item := view.Task.Task, view.Item.Item
	if repository == nil || repository.store == nil || task.Validate() != nil || item.Validate() != nil ||
		view.ReadRevision <= 0 || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		view.Task.Revision > view.ReadRevision || view.Item.Revision > view.ReadRevision ||
		item.TaskID != task.ID || item.Operation != workexecution.Release || item.Input == nil ||
		item.Placement.Executor != workexecution.Agent || !scriptexecutions.ValidRawScriptExecutionID(executionID) ||
		input.Validate() != nil || task.Action != workexecution.ActionDeploy &&
		task.Action != workexecution.ActionRollback && task.Action != workexecution.ActionApply {
		return ScriptWorkInput{}, errs.New(errs.KindValidationFailed, "Release hook work identity is invalid")
	}
	loaded, err := repository.loadScriptExecutionForWork(ctx, view, executionID, input)
	if err != nil {
		return ScriptWorkInput{}, err
	}
	if loaded.Execution.ServiceID != item.TargetID {
		loaded.Execution.Clear()
		return ScriptWorkInput{}, errs.New(errs.KindStateConflict, "Release hook belongs to another Service")
	}
	return loaded, nil
}

func (repository *ScriptRepository) ResolveReleaseScriptWorkArtifacts(ctx context.Context, view workqueue.View,
	executionID string, input workexecution.Input,
) (*ScriptWorkArtifacts, error) {
	loaded, err := repository.LoadReleaseScriptWork(ctx, view, executionID, input)
	if err != nil {
		return nil, err
	}
	return repository.resolveScriptWorkArtifacts(ctx, view, loaded)
}
