package releasegroups

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareFinish proves membership and the captured Group policy after each
// operation has settled its effects. Failed restoration cannot release the
// Environment reservation merely because no Agent is still running.
func (repository *ExecutionRepository) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
	at time.Time,
) (workqueue.Change, error) {
	task := finished.Task.Task
	if !task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		task.Progress.Unsettled() != 0 || len(finished.Items) != int(task.ExpectedItems) || at.Before(task.UpdatedAt) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group completion requires its complete settled work graph",
		)
	}
	root, err := repository.Load(ctx, finished.Task, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	expected := make(map[string]workexecution.Item, len(root.Record.Graph))
	for _, item := range root.Record.Graph {
		expected[item.ID] = item
	}
	members := make([]workexecution.Item, 0, len(root.Record.Policy.Members)*2)
	for _, snapshot := range finished.Items {
		item := snapshot.Item
		captured, found := expected[item.ID]
		if !found || snapshot.Revision <= 0 || snapshot.Revision > finished.ReadRevision || item.TaskID != task.ID ||
			item.Operation != captured.Operation || item.TargetID != captured.TargetID || item.Placement != captured.Placement ||
			!item.CreatedAt.Equal(
				captured.CreatedAt,
			) || item.AttemptNumber == 0 && !item.Deadline.Equal(captured.Deadline) ||
			!slices.Equal(item.Claims, captured.Claims) || !slices.Equal(item.Dependencies, captured.Dependencies) ||
			item.Phase != workexecution.Settled || item.Result == nil || item.ActiveAttemptID != "" ||
			item.Operation == workexecution.Network &&
				(item.Input == nil || captured.Input == nil || *item.Input != *captured.Input) {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Group completion differs from its accepted work membership",
			)
		}
		if item.Operation == workexecution.Release || item.Operation == workexecution.ReleaseRestoration {
			members = append(members, item)
		}
		if item.Operation == workexecution.Script && item.Input != nil {
			index := slices.IndexFunc(
				root.Record.Preflight,
				func(hook PreflightHook) bool { return hook.WorkItemID == item.ID },
			)
			if index < 0 || *item.Input != root.Record.Preflight[index].Input {
				return workqueue.Change{}, errs.New(
					errs.KindStateConflict,
					"Group completion has a different preflight Script input",
				)
			}
		}
		delete(expected, item.ID)
	}
	decision, err := domain.DecideGroupWork(root.Record.Policy, members, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(expected) != 0 || task.Stop == nil && decision.Action != domain.GroupWorkFinish {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group completion still needs member or restoration settlement",
		)
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: finished.Task.Revision},
		{Key: ExecutionCaptureKey(task.ID), ModRevision: root.Revision},
	}}, nil
}
