package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) PrepareHierarchyAttachInspection(
	ctx context.Context,
	view workqueue.ResumeView,
	report attachexecution.RemovalInspection,
	at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	return repository.prepareHierarchyAttachInspection(ctx, view, report, at, false)
}

func (repository *HierarchyDeletionRepository) prepareHierarchyAttachInspection(
	ctx context.Context,
	view workqueue.ResumeView,
	report attachexecution.RemovalInspection,
	at time.Time,
	interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	attempt := view.Attempt.Attempt
	if !view.ActiveAttempt || view.CurrentExecutor == interrupted || view.ExecutorRevision <= 0 ||
		attempt.FinishedAt != nil ||
		attempt.Direction != workexecution.Recovery ||
		view.Work.Item.Item.Operation != workexecution.Hierarchy ||
		report.ObservedAt.After(at) ||
		interrupted && report.State != attachexecution.InspectionInterrupted {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"Attach inspection no longer owns its recovery attempt",
		)
	}
	source, err := repository.LoadHierarchyAttachRecovery(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	raw, evidence, err := attachexecution.EncodeRemovalInspection(
		attachexecution.RecoveryInput{Attempt: attempt, Source: source.Source},
		report,
	)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	authority, err := repository.HierarchyAttachAdmission(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	key := attachremoval.WorkInspectionKey(attempt.ID)
	authority.Conditions = append(authority.Conditions,
		keyvalue.Condition{Key: key},
		keyvalue.Condition{Key: attachremoval.WorkRecoveryKey(attempt.WorkItemID), ModRevision: source.Revision},
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	authority.Mutations = append(
		authority.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: raw},
	)
	outcome := workexecution.Failed
	if report.ExecutionStopped() {
		outcome = workexecution.Succeeded
	}
	return authority, workexecution.Completion{
		Outcome:        outcome,
		Evidence:       evidence,
		EffectsSettled: report.ExecutionStopped(),
	}, nil
}

func (repository *HierarchyDeletionRepository) PrepareInterruptedAttachInspection(
	ctx context.Context,
	view workqueue.ResumeView,
	at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	source, err := repository.LoadHierarchyAttachRecovery(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	report := attachexecution.RemovalInspection{AttemptID: view.Attempt.Attempt.ID, ForwardID: source.Source.Forward.ID,
		Failure: source.Source.Evidence, CommandOrdinal: uint32(source.Source.Failure.Checkpoint.Sequence / 2),
		State: attachexecution.InspectionInterrupted, ObservedAt: at}
	return repository.prepareHierarchyAttachInspection(ctx, view, report, at, true)
}

func (repository *HierarchyDeletionRepository) LoadHierarchyAttachInspection(
	ctx context.Context,
	view workqueue.ResumeView,
) ([]byte, error) {
	if view.Work.Item.Item.Operation != workexecution.Hierarchy {
		return nil, errs.New(errs.KindStateConflict, "hierarchy Attach inspection belongs to another operation")
	}
	source, err := repository.LoadHierarchyAttachRecovery(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	return attachremoval.LoadRemovalInspection(ctx, repository.store, view,
		attachexecution.RecoveryInput{Attempt: view.Attempt.Attempt, Source: source.Source})
}
