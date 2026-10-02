package backupplanning

import (
	"context"
	"sort"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// restoreSnapshot retains compares for exactly the keys used by admission.
// Range membership is not used here: latest-Point selection finishes first,
// then its selected Point and complete companion set are read through this owner.
type restoreSnapshot struct {
	fixedSnapshotStore
	conditions map[string]int64
}

func (snapshot *restoreSnapshot) Get(ctx context.Context, key string) (*etcdstore.GetResult, error) {
	read, err := snapshot.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return nil, err
	}
	return &etcdstore.GetResult{Entry: read.Values[0], ReadRevision: read.ReadRevision}, nil
}

func (snapshot *restoreSnapshot) GetMany(ctx context.Context,
	request etcdstore.GetManyRequest,
) (*etcdstore.GetManyResult, error) {
	read, err := snapshot.fixedSnapshotStore.GetMany(ctx, request)
	if err != nil {
		return nil, err
	}
	for index, key := range request.Keys {
		var revision int64
		if value := read.Values[index]; value != nil {
			if value.Key != key || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
				etcdstore.ClearValues(read.Values)
				return nil, errs.New(errs.KindInternal, "restore snapshot evidence is corrupt")
			}
			revision = value.ModRevision
		}
		if previous, exists := snapshot.conditions[key]; exists && previous != revision {
			etcdstore.ClearValues(read.Values)
			return nil, errs.New(errs.KindInternal, "restore snapshot evidence changed at its fixed view")
		}
		snapshot.conditions[key] = revision
	}
	return read, nil
}

func (snapshot *restoreSnapshot) compares() []etcdstore.Condition {
	keys := make([]string, 0, len(snapshot.conditions))
	for key := range snapshot.conditions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	conditions := make([]etcdstore.Condition, 0, len(keys))
	for _, key := range keys {
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: snapshot.conditions[key]})
	}
	return conditions
}
