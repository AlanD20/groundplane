package releaseoperation

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareFinish checks the accepted membership after each operation has settled
// its own resource effects. Finishing the visible action does not redeploy,
// rewrite applied state or turn a failed DNS prerequisite into success.
func (owner *GraphPublication) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
	at time.Time,
) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.Action != workexecution.ActionDeploy && task.Action != workexecution.ActionRollback ||
		!task.PublicationComplete || task.Progress.Unsettled() != 0 || task.FinishedAt != nil ||
		task.CollectionStartedAt != nil || at.Before(task.UpdatedAt) || len(finished.Items) != int(task.ExpectedItems) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Release completion requires its fully settled captured graph",
		)
	}
	if ids.Validate(ids.KindReleaseGroup, task.TargetID) == nil {
		return owner.groups.PrepareFinish(ctx, finished, at)
	}
	request, guard, err := owner.ledger.ReadDirectReleaseTaskPreparation(ctx, finished.Task, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	var release *workqueue.ItemSnapshot
	actual := make(map[string]workqueue.ItemSnapshot, len(finished.Items))
	for index := range finished.Items {
		snapshot := &finished.Items[index]
		if snapshot.Item.Phase != workexecution.Settled || snapshot.Item.Result == nil ||
			snapshot.Item.ActiveAttemptID != "" {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release completion still has unresolved work")
		}
		actual[snapshot.Item.ID] = *snapshot
		if snapshot.Item.ID == request.Configuration.WorkItemID {
			release = snapshot
		}
	}
	if release == nil {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Release completion is missing its selected Service",
		)
	}
	view := workqueue.View{Task: finished.Task, Item: *release, ReadRevision: finished.ReadRevision}
	rollout, err := owner.ledger.GetServiceRollout(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(rollout.Record.CandidateArtifact)
	if err := request.Matches(task, release.Item, rollout.Record); err != nil {
		return workqueue.Change{}, err
	}
	graph, err := request.WorkGraph(task, release.Item)
	if err != nil {
		return workqueue.Change{}, err
	}
	for _, expected := range graph {
		snapshot, found := actual[expected.ID]
		item := snapshot.Item
		if !found || item.Operation != expected.Operation || item.TargetID != expected.TargetID ||
			item.Placement != expected.Placement || !item.CreatedAt.Equal(expected.CreatedAt) ||
			item.AttemptNumber == 0 &&
				!item.Deadline.Equal(expected.Deadline) || !slices.Equal(item.Claims, expected.Claims) ||
			!slices.Equal(item.Dependencies, expected.Dependencies) ||
			item.Operation == workexecution.Network && (item.Input == nil || *item.Input != *expected.Input) {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Release completion membership differs from the accepted action",
			)
		}
	}
	// Every work transition advances the Task revision. Comparing that aggregate
	// fences the complete snapshot without an unbounded per-member transaction.
	return workqueue.Change{Conditions: []keyvalue.Condition{guard,
		{Key: workqueue.TaskKey(task.ID), ModRevision: finished.Task.Revision}}}, nil
}
