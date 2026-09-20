package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) GetRunnerDeletionTombstone(
	ctx context.Context,
	runnerID string,
) (etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{}, false, err
	}
	if err := deletionrecord.ValidateDeletionTarget(deletionrecord.DeletionTargetRunner, runnerID); err != nil {
		return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), runnerID))
	if err != nil {
		return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{}, false, errs.New(
			errs.KindInternal,
			"Runner deletion tombstone read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := deletionrecord.DecodeDeletionTombstone(result.Entry.Value)
	if err != nil || record.TargetKind != deletionrecord.DeletionTargetRunner || record.TargetID != runnerID {
		return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{}, false, deletionrecord.CorruptDeletionTombstone()
	}
	return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
