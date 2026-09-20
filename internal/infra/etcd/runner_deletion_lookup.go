package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) GetRunnerDeletionTombstone(
	ctx context.Context,
	runnerID string,
) (etcdstore.Versioned[DeletionTombstoneRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[DeletionTombstoneRecord]{}, false, err
	}
	if err := validateDeletionTarget(DeletionTargetRunner, runnerID); err != nil {
		return etcdstore.Versioned[DeletionTombstoneRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, deletionTombstoneKey(string(DeletionTargetRunner), runnerID))
	if err != nil {
		return etcdstore.Versioned[DeletionTombstoneRecord]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[DeletionTombstoneRecord]{}, false, errs.New(
			errs.KindInternal,
			"Runner deletion tombstone read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[DeletionTombstoneRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := decodeDeletionTombstone(result.Entry.Value)
	if err != nil || record.TargetKind != DeletionTargetRunner || record.TargetID != runnerID {
		return etcdstore.Versioned[DeletionTombstoneRecord]{}, false, corruptDeletionTombstone()
	}
	return etcdstore.Versioned[DeletionTombstoneRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
