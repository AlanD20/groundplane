package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) GetRunnerDeletionTombstone(
	ctx context.Context,
	runnerID string,
) (Versioned[DeletionTombstoneRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[DeletionTombstoneRecord]{}, false, err
	}
	if err := validateDeletionTarget(DeletionTargetRunner, runnerID); err != nil {
		return Versioned[DeletionTombstoneRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, deletionTombstoneKey(string(DeletionTargetRunner), runnerID))
	if err != nil {
		return Versioned[DeletionTombstoneRecord]{}, false, err
	}
	if result == nil {
		return Versioned[DeletionTombstoneRecord]{}, false, errs.New(
			errs.KindInternal,
			"Runner deletion tombstone read is empty",
		)
	}
	if result.Entry == nil {
		return Versioned[DeletionTombstoneRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := decodeDeletionTombstone(result.Entry.Value)
	if err != nil || record.TargetKind != DeletionTargetRunner || record.TargetID != runnerID {
		return Versioned[DeletionTombstoneRecord]{}, false, corruptDeletionTombstone()
	}
	return Versioned[DeletionTombstoneRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
