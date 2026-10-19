package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	hierarchyrecords "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (publication *WorkPublication) rejectPlanning(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	revision int64,
	reason hierarchyrecords.PlanningRejectionReason,
	at time.Time,
) (workdispatch.PublicationBatch, error) {
	authority, evidence, err := publication.records.PrepareHierarchyPlanningRejection(ctx, task, revision, reason, at)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	defer keyvalue.ClearMutationValues(authority.Mutations)
	outcome := workexecution.Failed
	if task.Task.Stop != nil {
		outcome = task.Task.Stop.Outcome
	}
	finish, err := publication.queue.PreparePlanningRejection(
		ctx,
		task,
		outcome,
		evidence,
		at,
		at.Add(publication.retention),
	)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	defer keyvalue.ClearMutationValues(finish.Mutations)
	result, err := publication.queue.Commit(ctx, authority, finish)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	if !result.Succeeded {
		return workdispatch.PublicationBatch{}, errs.New(
			errs.KindStateConflict,
			"removal planning changed before failure settlement",
		)
	}
	return workdispatch.PublicationBatch{Planning: true}, nil
}

// Classify only explicit permanent planning decisions. Storage failures,
// cancellation and concurrent-state conflicts are not evidence of bad intent.
func planningRejectionReason(err error) hierarchyrecords.PlanningRejectionReason {
	kind, known := errs.KindOf(err)
	if !known {
		return ""
	}
	switch kind {
	case errs.KindValidationFailed:
		return hierarchyrecords.PlanningInvalidInput
	case errs.KindResourceInUse:
		return hierarchyrecords.PlanningDependencyConflict
	default:
		return ""
	}
}
