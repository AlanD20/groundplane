package serviceremovalwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// DeletionGuard can release an unexecuted removal even when newer desired state
// exists. It never permits host effects: those additionally require runtime and
// execution ownership guards.
func (repository *Repository) DeletionGuard(
	ctx context.Context,
	input Input,
	revision int64,
) (keyvalue.Condition, error) {
	if err := input.Validate(); err != nil {
		return keyvalue.Condition{}, err
	}
	if revision <= 0 {
		return keyvalue.Condition{}, conflict("Service removal deletion snapshot is invalid")
	}
	key := deletions.TombstoneKey(string(deletions.DeletionTargetService), input.ServiceID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if read == nil {
		return keyvalue.Condition{}, conflict("Service removal deletion fence is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > revision {
		return keyvalue.Condition{}, conflict("Service removal deletion fence is unavailable")
	}
	if err := validateDeletionFence(input, read.Values[0].Value); err != nil {
		return keyvalue.Condition{}, err
	}
	return keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}

func validateDeletionFence(input Input, value []byte) error {
	tombstone, err := deletions.DecodeDeletionTombstone(value)
	if err != nil {
		return err
	}
	if tombstone.TargetKind != deletions.DeletionTargetService || tombstone.TargetID != input.ServiceID ||
		tombstone.TargetRevision != input.ServiceRevision || tombstone.TaskID != input.TaskID ||
		tombstone.Phase != deletions.DeletionPhaseHostEffects {
		return conflict("Service removal no longer owns its deletion fence")
	}
	return nil
}
