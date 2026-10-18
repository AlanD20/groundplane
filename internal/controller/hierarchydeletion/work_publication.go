package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	hierarchyrecords "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type WorkPublication struct {
	service       *Service
	records       *etcd.HierarchyDeletionRepository
	queue         *workqueue.Repository
	attachPlanner *attachplan.RemovalPlanner
	retention     time.Duration
}

func NewWorkPublication(service *Service, records *etcd.HierarchyDeletionRepository, queue *workqueue.Repository,
	attachPlanner *attachplan.RemovalPlanner, retention time.Duration,
) (*WorkPublication, error) {
	if service == nil || records == nil || queue == nil || attachPlanner == nil || retention <= 0 {
		return nil, errs.New(errs.KindInternal, "hierarchy publication requires planning and captured storage")
	}
	return &WorkPublication{service: service, records: records, queue: queue, attachPlanner: attachPlanner, retention: retention}, nil
}

func (publication *WorkPublication) PreparePublication(ctx context.Context, task workqueue.TaskSnapshot, revision int64, at time.Time) (workdispatch.PublicationBatch, error) {
	if task.Revision <= 0 || revision < task.Revision || at.Before(task.Task.UpdatedAt) {
		return workdispatch.PublicationBatch{}, errs.New(errs.KindStateConflict, "hierarchy publication snapshot is invalid")
	}
	if task.Task.ExpectedItems == 0 && (task.Task.Stop != nil || !at.Before(task.Task.Deadline)) {
		op, err := publication.records.OperationByTaskAtRevision(ctx, task.Task.ID, revision)
		if err != nil {
			return workdispatch.PublicationBatch{}, err
		}
		if op.Tombstone.Phase == hierarchyrecords.HierarchyDeletionPlanning {
			reason := hierarchyrecords.PlanningDeadline
			if task.Task.Stop != nil {
				reason = hierarchyrecords.PlanningStopped
			}
			return publication.rejectPlanning(ctx, task, revision, reason, at)
		}
	}
	ready, err := publication.service.PreparePlan(ctx, task.Task.ID)
	if err != nil {
		if task.Task.ExpectedItems == 0 && task.Task.Stop == nil {
			if reason := planningRejectionReason(err); reason != "" {
				return publication.rejectPlanning(ctx, task, revision, reason, at)
			}
		}
		return workdispatch.PublicationBatch{}, err
	}
	if !ready {
		return workdispatch.PublicationBatch{Planning: true}, nil
	}
	graph, authority, err := publication.records.LoadWorkGraph(ctx, task, revision)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	// Stage membership first. Then capture one hook input set per pass while
	// the execution barrier is still closed. Stopped/expired work needs no
	// capture: its ordinary no-execution settlement abandons preparations.
	if task.Task.Progress.Total() == uint64(task.Task.ExpectedItems) && task.Task.Stop == nil && at.Before(task.Task.Deadline) {
		for _, item := range graph {
			if item.Operation != workexecution.Hierarchy || item.Placement.Executor != workexecution.Agent ||
				ids.Validate(ids.KindAttach, item.TargetID) != nil {
				continue
			}
			view, err := publication.queue.ReadWork(ctx, task.Task.ID, item.ID, revision)
			if err != nil {
				return workdispatch.PublicationBatch{}, err
			}
			change, err := publication.prepareAttachCapture(ctx, view)
			if err != nil {
				return workdispatch.PublicationBatch{}, err
			}
			if len(change.Mutations) == 0 {
				continue
			}
			result, err := publication.queue.Commit(ctx, authority, change)
			keyvalue.ClearMutationValues(change.Mutations)
			keyvalue.ClearValues(result.FailureReads)
			if err != nil {
				return workdispatch.PublicationBatch{}, err
			}
			if !result.Succeeded {
				return workdispatch.PublicationBatch{}, errs.New(errs.KindStateConflict, "hierarchy capture ownership changed before publication")
			}
			return workdispatch.PublicationBatch{Planning: true}, nil
		}
	}
	return workdispatch.PublicationBatch{Graph: graph, Authority: authority}, nil
}
