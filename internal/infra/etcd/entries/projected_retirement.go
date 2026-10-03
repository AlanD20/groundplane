package entries

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareProjectedEntryRetirement removes only the selected Entry's derived
// lookup and values. The caller must prove and fence the desired head that no
// longer contains it, and commit these mutations with its durable progress.
func PrepareProjectedEntryRetirement(ctx context.Context, store interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}, record Record, revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if ctx == nil || store == nil || revision <= 0 || ValidateRecord(record) != nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "projected Entry retirement is invalid")
	}
	key := BlueprintEntryEnvironmentPrefix + record.Entry.ID
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, nil, errs.New(errs.KindStateConflict, "projected Entry retirement read changed")
	}
	defer etcdstore.ClearValues(read.Values)
	value := read.Values[0]
	var bindingRevision int64
	if value != nil {
		if value.Key != key || value.ModRevision <= 0 || string(value.Value) != record.EnvironmentID {
			return nil, nil, errs.New(errs.KindStateConflict, "projected Entry retirement owner changed")
		}
		bindingRevision = value.ModRevision
	}
	return []etcdstore.Condition{{Key: key, ModRevision: bindingRevision}}, []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: key},
		{Type: etcdstore.MutationDelete, Key: entryvalues.PlainPrefix + record.Entry.ID + "/", Prefix: true},
		{Type: etcdstore.MutationDelete, Key: entryvalues.SecretPrefix + record.Entry.ID + "/", Prefix: true},
	}, nil
}
