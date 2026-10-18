package operationretention

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// WorkOwner retires operation inputs/results under the collecting Task fence.
// False preserves work and attempts while external protections still hold.
// Prepared absence comparisons are joined to every execution-record deletion.
// The final argument is true only when no attempts remain; only that pass may
// remove metadata needed to retire earlier attempts.
type WorkOwner interface {
	RetireWork(context.Context, workqueue.View, time.Time, bool) (bool, workqueue.Change, error)
	PrepareAttemptRetirement(workqueue.View, workqueue.AttemptSnapshot) (workqueue.Change, error)
}

// UnpreparedWorkOwner releases operation-owned staging even when executable
// input was never published. A generic no-effect receipt does not prove that
// staged snapshots, secret pins or captured source references are gone.
type UnpreparedWorkOwner interface {
	RetireUnpreparedWork(context.Context, workqueue.View, time.Time) (bool, workqueue.Change, error)
}

type WorkRegistration struct {
	Operation workexecution.Operation
	Owner     WorkOwner
}

func (collector *Collector) collectGraph(ctx context.Context, task workqueue.TaskSnapshot, revision int64, at time.Time) (bool, error) {
	if task.Task.CollectionStartedAt == nil {
		change, err := workqueue.PrepareCollection(task, at)
		if err != nil {
			return false, err
		}
		_, err = collector.queue.Commit(ctx, change)
		return false, err
	}
	page, err := collector.queue.ListWork(ctx, task.Task.ID, "", 1, revision)
	if err != nil {
		return false, err
	}
	if len(page.Items) == 0 {
		var authority workqueue.Change
		if owner := collector.taskOwners[task.Task.Action]; owner != nil {
			authority, err = owner.PrepareTaskRetirement(ctx, task, revision, at)
			if err != nil {
				return false, err
			}
		}
		var change workqueue.Change
		var err error
		if task.Task.Action == workexecution.ActionApply {
			change, err = blueprintunits.PrepareGraphCollection(task)
		} else {
			change, err = workqueue.PrepareTaskCollection(task)
		}
		if err != nil {
			return false, err
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: executiondiagnostics.TaskEventsKey(task.Task.ID)})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: executiondiagnostics.TaskExpiryKey(task.Task.ID)})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: scriptexecutions.SourceRetirementPrefix(task.Task.ID), Prefix: true})
		result, err := collector.queue.Commit(ctx, authority, change)
		return result.Succeeded, err
	}
	item := page.Items[0]
	return collector.collectOwnedWork(ctx, task.Task.ID, item.Item.ID, at)
}

func (collector *Collector) collectOwnedWork(ctx context.Context, taskID, workID string, at time.Time) (bool, error) {
	view, attempts, err := collector.queue.CollectionAttempts(ctx, taskID, workID)
	if err != nil {
		return false, err
	}
	item := view.Item
	if item.Item.Input == nil && len(attempts.Attempts) == 0 {
		settlement, err := collector.queue.ReadInputSettlement(ctx, view)
		if err != nil {
			return false, err
		}
		if settlement == nil {
			return false, errs.New(errs.KindStateConflict, "unprepared work has no authentic settlement to collect")
		}
		var authority workqueue.Change
		if owner, ok := collector.workOwners[item.Item.Operation].(UnpreparedWorkOwner); ok {
			retired, prepared, err := owner.RetireUnpreparedWork(ctx, view, at)
			defer keyvalue.ClearMutationValues(prepared.Mutations)
			if err != nil || !retired {
				return false, err
			}
			if len(prepared.Conditions) == 0 {
				return false, errs.New(errs.KindInternal, "unprepared work retirement requires operation protection comparisons")
			}
			authority = prepared
		}
		change, err := workqueue.PrepareWorkCollection(view.Task, view.Item)
		if err != nil {
			return false, err
		}
		_, err = collector.queue.Commit(ctx, authority, change)
		return false, err
	}
	owner := collector.workOwners[item.Item.Operation]
	if owner == nil {
		return false, errs.New(errs.KindStateConflict, "work operation has no registered retention owner")
	}
	retired, authority, err := owner.RetireWork(ctx, view, at, len(attempts.Attempts) == 0)
	defer keyvalue.ClearMutationValues(authority.Mutations)
	if err != nil || !retired {
		return false, err
	}
	if len(authority.Conditions) == 0 {
		return false, errs.New(errs.KindInternal, "work retention requires operation protection comparisons")
	}
	changes := []workqueue.Change{authority}
	defer func() {
		for _, change := range changes {
			keyvalue.ClearMutationValues(change.Mutations)
		}
	}()
	if len(attempts.Attempts) == 0 {
		change, err := workqueue.PrepareWorkCollection(view.Task, view.Item)
		if err != nil {
			return false, err
		}
		changes = append(changes, change)
	} else {
		for _, attempt := range attempts.Attempts {
			owned, err := owner.PrepareAttemptRetirement(view, attempt)
			changes = append(changes, owned)
			if err != nil {
				return false, err
			}
			change, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, attempt)
			if err != nil {
				return false, err
			}
			for _, key := range []string{executiondiagnostics.Key(attempt.Attempt.ID), executiondiagnostics.LossKey(attempt.Attempt.ID), executiondiagnostics.EventsKey(attempt.Attempt.ID)} {
				change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
			}
			changes = append(changes, change)
		}
	}
	_, err = collector.queue.Commit(ctx, changes...)
	return false, err
}
