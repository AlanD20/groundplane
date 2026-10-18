package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The inspection result permits another explicit captured execution; it does
// not replace the original failed removal or claim that SQL/hook effects were
// rolled back. The Retry transaction compares both retained evidence revisions.
func (repository *HierarchyDeletionRepository) hierarchyAttachRetryBasis(ctx context.Context, view workqueue.View,
	attempt workexecution.Attempt,
) (workexecution.Attempt, []keyvalue.Condition, error) {
	if attempt.Direction != workexecution.Recovery || attempt.FinishedAt == nil || attempt.Result == nil || attempt.Outcome != workexecution.Succeeded {
		return workexecution.Attempt{}, nil, errs.New(errs.KindTaskNotRetryable, "Attach Retry requires a completed command inspection")
	}
	source, err := repository.LoadHierarchyAttachRecovery(ctx, view)
	if err != nil {
		return workexecution.Attempt{}, nil, err
	}
	key := attachremoval.WorkInspectionKey(attempt.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return workexecution.Attempt{}, nil, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 {
		return workexecution.Attempt{}, nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	report, err := attachexecution.DecodeRemovalInspection(attachexecution.RecoveryInput{Attempt: attempt, Source: source.Source},
		read.Values[0].Value, *attempt.Result)
	if err != nil {
		return workexecution.Attempt{}, nil, err
	}
	if !report.ExecutionStopped() || report.ObservedAt.After(*attempt.FinishedAt) {
		return workexecution.Attempt{}, nil, errs.New(errs.KindTaskNotRetryable, "previous Attach command is not confirmed stopped")
	}
	return source.Source.Forward, []keyvalue.Condition{
		{Key: attachremoval.WorkRecoveryKey(view.Item.Item.ID), ModRevision: source.Revision},
		{Key: key, ModRevision: read.Values[0].ModRevision},
	}, nil
}
