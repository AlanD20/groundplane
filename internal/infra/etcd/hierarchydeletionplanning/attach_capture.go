package hierarchydeletionplanning

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// captureAttachInput preserves each completed source read before traversing the
// next Attach. Re-entering planning reuses that immutable source, rather than
// rebuilding its encrypted facts and backing configuration from historical MVCC.
// Membership and dependency checks still use the operation's captured revision;
// this record alone neither seals the graph nor permits execution.
func (repository *Planner) captureAttachInput(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionTombstone, attachID string, attachRevision int64,
) (attachremoval.Input, error) {
	key := attachremoval.FrozenKey(operation.OperationID, attachID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return attachremoval.Input{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 {
		return attachremoval.Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	var input attachremoval.Input
	if value := read.Values[0]; value != nil {
		if value.Key != key || value.ModRevision <= 0 {
			return attachremoval.Input{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		input, err = attachremoval.Decode(value.Value)
	} else {
		input, err = attachremoval.ReadCaptured(ctx, repository.store, operation.SnapshotRevision, attachID)
	}
	if err != nil {
		return attachremoval.Input{}, err
	}
	if input.Attach.ID != attachID || input.AttachRevision != attachRevision || input.SnapshotRevision != operation.SnapshotRevision ||
		input.HookInputs != nil || input.HookInputSet != nil || input.HookOperationID != "" {
		attachremoval.Clear(&input)
		return attachremoval.Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if read.Values[0] == nil {
		if err := repository.persistAttachInput(ctx, operation, input); err != nil {
			attachremoval.Clear(&input)
			return attachremoval.Input{}, err
		}
	}
	return input, nil
}
