package attachments

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BackingZoneCreationKey records the last transaction that added a consumer.
// Its MVCC revision lets cascade admission compare one key rather than one
// comparison per Attach. Removal still validates each captured binding itself.
func BackingZoneCreationKey(zoneID string) string {
	return "/v1/attach-zone-creations/" + zoneID
}

// BindBackingZoneCreation must join every new binding's publication. Writing
// the same value still advances etcd's modification revision. A batch adding
// several consumers in one Zone writes this key only once.
func BindBackingZoneCreation(zoneID string, conditions []keyvalue.Condition, mutations []keyvalue.Mutation) ([]keyvalue.Condition, []keyvalue.Mutation) {
	fence := keyvalue.Condition{Key: deletions.TombstoneKey(string(deletions.DeletionTargetZone), zoneID)}
	if !slices.Contains(conditions, fence) {
		conditions = append(conditions, fence)
	}
	key := BackingZoneCreationKey(zoneID)
	if !slices.ContainsFunc(mutations, func(mutation keyvalue.Mutation) bool { return mutation.Key == key }) {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: []byte(zoneID)})
	}
	return conditions, mutations
}

func BackingZoneCreationCondition(ctx context.Context, store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}, zoneID string, revision int64) (keyvalue.Condition, error) {
	if ids.Validate(ids.KindNetwork, zoneID) != nil || revision <= 0 {
		return keyvalue.Condition{}, errs.New(errs.KindValidationFailed, "Backing Zone admission requires its captured revision")
	}
	key := BackingZoneCreationKey(zoneID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Backing Zone consumer creation snapshot is unavailable")
	}
	value := read.Values[0]
	if value != nil && (value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision || string(value.Value) != zoneID) {
		return keyvalue.Condition{}, CorruptAttachRecord()
	}
	return keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(value)}, nil
}
