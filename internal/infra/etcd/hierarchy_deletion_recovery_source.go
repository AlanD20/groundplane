package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *HierarchyDeletionRepository) LoadHierarchyEnvironmentRecovery(
	ctx context.Context,
	view workqueue.View,
) (hierarchydeletion.EnvironmentRecoverySnapshot, error) {
	return hierarchydeletion.LoadEnvironmentRecovery(ctx, repository.store, view)
}

func (repository *HierarchyDeletionRepository) hierarchyEnvironmentRecoveryConditions(
	ctx context.Context,
	view workqueue.ResumeView,
	input hierarchyexecution.EnvironmentInput,
) ([]keyvalue.Condition, error) {
	if input.Attempt.Direction == workexecution.Forward {
		if input.Recovery != nil {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		return nil, nil
	}
	if input.Recovery == nil || input.Recovery.ValidateFor(view.Attempt.Attempt) != nil {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	snapshot, err := repository.LoadHierarchyEnvironmentRecovery(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	expected, err := hierarchyexecution.EncodeEnvironmentRecovery(snapshot.Source)
	if err != nil {
		return nil, err
	}
	provided, err := hierarchyexecution.EncodeEnvironmentRecovery(*input.Recovery)
	if err != nil || !bytes.Equal(expected, provided) {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return []keyvalue.Condition{
		{Key: hierarchydeletion.EnvironmentRecoveryKey(view.Work.Item.Item.ID), ModRevision: snapshot.Revision},
	}, nil
}
