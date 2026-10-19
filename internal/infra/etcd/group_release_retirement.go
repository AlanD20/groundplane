package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareGroupReleaseCaptureRetirement is joined to the final empty-graph
// barrier. Accepted staging is owned by this Task, not by orphan-draft cleanup.
func (ledger *ReleaseLedger) PrepareGroupReleaseCaptureRetirement(ctx context.Context, task workqueue.TaskSnapshot,
	root keyvalue.Versioned[releasegroups.ExecutionCapture], at time.Time,
) (workqueue.Change, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, err
	}
	accepted, err := root.Record.Task()
	if err != nil || task.Task.CollectionStartedAt == nil || task.Task.FinishedAt == nil ||
		task.Task.Progress.Unsettled() != 0 ||
		root.Revision <= 0 ||
		root.ReadRevision < task.Revision ||
		accepted.ID != task.Task.ID ||
		accepted.Intent != task.Task.Intent ||
		accepted.TargetID != task.Task.TargetID ||
		accepted.Scope != task.Task.Scope ||
		accepted.Action != task.Task.Action ||
		at.Before(*task.Task.CollectionStartedAt) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Group capture still protects retained execution")
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
		{Key: releasegroups.ExecutionCaptureKey(task.Task.ID), ModRevision: root.Revision},
	}, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: releasegroups.ExecutionCaptureKey(task.Task.ID)},
		{Type: keyvalue.MutationDelete, Key: groupReleaseDraftKey(task.Task.ID), Prefix: true},
	}}, nil
}
