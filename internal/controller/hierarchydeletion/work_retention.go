package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Retention struct {
	records *etcd.HierarchyDeletionRepository
}

func NewRetention(records *etcd.HierarchyDeletionRepository) (*Retention, error) {
	if records == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy retention requires its captured records")
	}
	return &Retention{records: records}, nil
}

func (owner *Retention) RetireWork(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	if view.Item.Item.Placement.Executor == workexecution.Agent {
		if ids.Validate(ids.KindAttach, view.Item.Item.TargetID) == nil {
			return owner.records.PrepareHierarchyAttachRetention(ctx, view, at, final)
		}
		return owner.records.PrepareHierarchyEnvironmentRetention(ctx, view, at, final)
	}
	if !final {
		if view.Item.Item.Operation == workexecution.RemovalBackupObject {
			return true, workqueue.Change{}, nil
		}
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Controller removal unexpectedly owns host attempts",
		)
	}
	return owner.records.PrepareHierarchyControllerRetention(ctx, view, at)
}

func (owner *Retention) PrepareAttemptRetirement(
	view workqueue.View,
	snapshot workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	item, attempt := view.Item.Item, snapshot.Attempt
	hostRemoval := item.Operation == workexecution.Hierarchy && item.Placement.Executor == workexecution.Agent
	remoteRemoval := item.Operation == workexecution.RemovalBackupObject &&
		item.Placement.Executor == workexecution.Controller
	if (!hostRemoval && !remoteRemoval) || item.Result == nil || item.Input == nil ||
		view.Task.Task.CollectionStartedAt == nil || view.Task.Task.Outcome != workexecution.Succeeded || attempt.Validate() != nil ||
		attempt.FinishedAt == nil || attempt.Result == nil ||
		attempt.TaskID != view.Task.Task.ID || attempt.WorkItemID != item.ID || attempt.Input != *item.Input ||
		attempt.Number > item.AttemptNumber || attempt.Executor.HostID != item.Placement.HostID || snapshot.Revision <= 0 {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"host removal attempt lacks retained completion proof",
		)
	}
	if attempt.Number == item.AttemptNumber {
		if attempt.Outcome != workexecution.Succeeded || *attempt.Result != *item.Result {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"last removal attempt differs from the retained successful result",
			)
		}
	} else if attempt.Outcome != workexecution.Failed && !(attempt.Direction == workexecution.Recovery && attempt.Outcome == workexecution.Succeeded) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "previous removal attempt was not an explicitly retried failure")
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(view.Task.Task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: snapshot.Revision},
	}}
	if remoteRemoval {
		change.Mutations = []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: etcd.HierarchyBackupResultKey(attempt.ID)},
		}
		return change, nil
	}
	if ids.Validate(ids.KindAttach, item.TargetID) == nil {
		for sequence := uint64(1); sequence <= attachexecution.MaximumRemovalReports; sequence++ {
			change.Mutations = append(
				change.Mutations,
				keyvalue.Mutation{
					Type: keyvalue.MutationDelete,
					Key:  attachremoval.WorkReportKey(attempt.ID, sequence),
				},
			)
		}
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: attachremoval.WorkFailureKey(attempt.ID)},
		)
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: attachremoval.WorkInspectionKey(attempt.ID)},
		)
		return change, nil
	}
	for sequence := uint64(1); sequence <= 4; sequence++ {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
			Key: hierarchydeletion.EnvironmentReportKey(attempt.ID, sequence)})
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
		Key: hierarchydeletion.EnvironmentFailureKey(attempt.ID)})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
		Key: hierarchydeletion.EnvironmentRecoveryResultKey(attempt.ID)})
	return change, nil
}
