package backupconfiguration

import (
	"context"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// ReadConfigRestoreGeneration returns the immutable complete-set seal under the
// same native assignment and source guard used for receiving its records.
func (repository *ConfigTransferRepository) ReadConfigRestoreGeneration(ctx context.Context,
	owner ConfigRestoreTransferOwner, revision int64,
) (etcdstore.Versioned[ConfigRestoreGenerationRecord], bool, error) {
	var zero etcdstore.Versioned[ConfigRestoreGenerationRecord]
	if etcdstore.ValidateContext(ctx) != nil || repository == nil || repository.store == nil ||
		repository.guard == nil || repository.restoreGuard == nil ||
		ValidateConfigRestoreTransferOwner(owner) != nil || revision < 0 {
		return zero, false, captureSnapshotConflict()
	}
	key := ConfigRestoreGenerationKey(owner)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return zero, false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 ||
		(revision > 0 && read.ReadRevision != revision) {
		return zero, false, captureSnapshotConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	zero.ReadRevision = read.ReadRevision
	if _, err := repository.guard(ctx, owner.Transfer, read.ReadRevision); err != nil {
		return zero, false, err
	}
	if _, err := repository.restoreGuard(ctx, owner, read.ReadRevision); err != nil {
		return zero, false, err
	}
	value := read.Values[0]
	if value == nil {
		return zero, false, nil
	}
	record, err := DecodeConfigRestoreGeneration(value.Value)
	if err != nil || value.Key != key || value.Version != 1 || value.ModRevision <= 0 ||
		value.ModRevision > read.ReadRevision || record.Owner != owner {
		return zero, false, captureSnapshotConflict()
	}
	return etcdstore.Versioned[ConfigRestoreGenerationRecord]{Record: record, Revision: value.ModRevision,
		ReadRevision: read.ReadRevision}, true, nil
}
