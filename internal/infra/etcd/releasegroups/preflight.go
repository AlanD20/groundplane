package releasegroups

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PreflightAuthority verifies a Script as an explicit required member of this
// accepted Group. A shared Task id alone is never permission to run a Script.
func (repository *ExecutionRepository) PreflightAuthority(ctx context.Context, view workqueue.View) ([]keyvalue.Condition, error) {
	root, err := repository.Load(ctx, view.Task, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	item := view.Item.Item
	index := slices.IndexFunc(root.Record.Preflight, func(hook PreflightHook) bool { return hook.WorkItemID == item.ID })
	if index < 0 || item.Operation != workexecution.Script || item.Input == nil {
		return nil, errs.New(errs.KindStateConflict, "Script is not required by this Group's accepted preflight")
	}
	hook := root.Record.Preflight[index]
	expected := root.Record.memberWork(item.ID)
	if hook.ScriptID != item.TargetID || hook.Input != *item.Input || item.TaskID != view.Task.Task.ID ||
		item.Placement != root.Record.Placement || !item.CreatedAt.Equal(expected.CreatedAt) ||
		item.AttemptNumber == 0 && !item.Deadline.Equal(expected.Deadline) || !slices.Equal(item.Claims, expected.Claims) ||
		!slices.Equal(item.Dependencies, expected.Dependencies) {
		return nil, errs.New(errs.KindStateConflict, "Script differs from its accepted Group preflight")
	}
	return []keyvalue.Condition{{Key: ExecutionCaptureKey(view.Task.Task.ID), ModRevision: root.Revision}}, nil
}
