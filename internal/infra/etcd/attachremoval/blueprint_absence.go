package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Actual absence joins the ledger's positive birth proof. A missing binding
// alone cannot authorize retirement of possibly executed credential work.
func (repository *InputRepository) BlueprintAbsenceConditions(ctx context.Context,
	view workqueue.View, frozen BlueprintInput,
) ([]keyvalue.Condition, error) {
	if frozen.Snapshot != nil || frozen.AttachID != view.Item.Item.TargetID || frozen.TaskID != view.Task.Task.ID {
		return nil, checkpointConflict("Detach absence requires its declared-only admission capture")
	}
	keys := []string{attachments.AttachKey(frozen.AttachID), attachments.AttachFactsKey(frozen.AttachID),
		attachments.AttachNameKey(
			frozen.EnvironmentID,
			frozen.AttachName,
		), attachments.RetiredDetachKey(frozen.AttachID)}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	guards := make([]keyvalue.Condition, 0, len(keys)+2)
	for index, key := range keys {
		if read.Values[index] != nil {
			return nil, checkpointConflict("Declared-only Attach acquired state before cleanup")
		}
		guards = append(guards, keyvalue.Condition{Key: key})
	}
	for _, prefix := range []string{attachments.AttachCredentialByPrefix(frozen.AttachID), attachments.AttachGrantedByPrefix(frozen.AttachID)} {
		read, err := repository.store.Range(
			ctx,
			keyvalue.RangeRequest{Prefix: prefix, Limit: 1, Revision: view.ReadRevision},
		)
		if err != nil {
			return nil, err
		}
		if read == nil || read.ReadRevision != view.ReadRevision {
			return nil, checkpointConflict("Attach absence dependency snapshot is unavailable")
		}
		present := len(read.Values) != 0 || read.More
		keyvalue.ClearRangeValues(read.Values)
		if present {
			return nil, checkpointConflict("Declared-only Attach still has credential or grant consumers")
		}
		guards = append(guards, keyvalue.Condition{Key: prefix, Prefix: true})
	}
	return guards, nil
}
