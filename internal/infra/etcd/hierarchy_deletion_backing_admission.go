package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Check consumers before publishing deletion and compare their continued absence
// in the same transaction that fences the Backing Project against new mutations.
func (repository *HierarchyDeletionRepository) protectBackingDeletion(
	ctx context.Context,
	targetKind hierarchydeletion.HierarchyDeletionTargetKind,
	targetID string,
	revision int64,
) ([]keyvalue.Condition, error) {
	if targetKind != hierarchydeletion.HierarchyDeletionTargetBacking {
		return nil, nil
	}
	prefix := attachments.AttachBackingProjectPrefix(targetID)
	read, err := repository.store.Range(ctx, keyvalue.RangeRequest{Prefix: prefix, Limit: 1, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearRangeValues(read.Values)
	if len(read.Values) != 0 {
		return nil, errs.New(errs.KindResourceInUse,
			"Backing Service has consumer connections. Disconnect every consumer before deleting it")
	}
	return []keyvalue.Condition{{Key: prefix, Prefix: true}}, nil
}
