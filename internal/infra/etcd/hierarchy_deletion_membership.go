package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) FreezeMembership(
	ctx context.Context,
	operation HierarchyDeletionOperation,
) (hierarchydeletionplanning.HierarchyDeletionFrozenMembership, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return hierarchydeletionplanning.HierarchyDeletionFrozenMembership{}, err
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return hierarchydeletionplanning.HierarchyDeletionFrozenMembership{}, err
	}
	if current.Tombstone.Phase != hierarchydeletion.HierarchyDeletionPlanning || current.Tombstone.SnapshotRevision <= 0 ||
		current.Tombstone.TargetRevision <= 0 || current.Tombstone.PlanCount != nil || current.Tombstone.PlanDigest != nil {
		return hierarchydeletionplanning.HierarchyDeletionFrozenMembership{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion membership is not freezable",
		)
	}
	return hierarchydeletionplanning.NewPlanner(repository.store).FreezeCapturedMembership(ctx, current.Tombstone)
}
