package entries

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareProjectedEntryReplacement protects identity and lookup ownership while
// definitions remain in the caller's sealed predecessor projection. The caller
// must fence that projection and its Environment operation in the transaction.
func PrepareProjectedEntryReplacement(ctx context.Context, store interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}, environmentID string, revision int64, record, previous Record, existed bool,
) ([]etcdstore.Condition, error) {
	conflict := func() error { return errs.New(errs.KindStateConflict, "projected Entry replacement authority changed") }
	if ctx == nil || store == nil || revision <= 0 || ValidateRecord(record) != nil ||
		record.EnvironmentID != environmentID {
		return nil, conflict()
	}
	keys := []string{RecordKey(record.Entry.ID), EntryOwnerKey(record.EnvironmentID, record.Entry.ID),
		BlueprintEntryEnvironmentPrefix + record.Entry.ID,
		deletions.TombstoneKey(string(deletions.DeletionTargetEntry), record.Entry.ID)}
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, conflict()
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0) {
			return nil, conflict()
		}
	}
	// Standalone definitions would shadow the desired projection in readers.
	if read.Values[0] != nil || read.Values[1] != nil || read.Values[3] != nil {
		return nil, conflict()
	}
	if existed {
		if previous.Entry.ID != record.Entry.ID || previous.EnvironmentID != environmentID || read.Values[2] == nil {
			return nil, conflict()
		}
	} else if read.Values[2] != nil {
		return nil, conflict()
	}
	if read.Values[2] != nil && string(read.Values[2].Value) != environmentID {
		return nil, conflict()
	}
	conditions := make([]etcdstore.Condition, len(keys))
	for index, key := range keys {
		conditions[index].Key = key
		if read.Values[index] != nil {
			conditions[index].ModRevision = read.Values[index].ModRevision
		}
	}
	return conditions, nil
}
