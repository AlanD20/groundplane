package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EntryRemovalConfiguration struct {
	Files         runtimeconfiguration.Snapshot
	HeadRevision  int64
	EpochRevision int64
}

func (repository *HierarchyRepository) ReadEntryRemovalConfiguration(
	ctx context.Context,
	environmentID string,
	revision int64,
) (EntryRemovalConfiguration, error) {
	files, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return EntryRemovalConfiguration{}, err
	}
	snapshot, head, err := files.ReadAcknowledged(ctx, environmentID, revision)
	if err != nil {
		return EntryRemovalConfiguration{}, err
	}
	key := hierarchy.EnvironmentMutationEpochKey(environmentID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return EntryRemovalConfiguration{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > revision {
		return EntryRemovalConfiguration{}, errs.New(
			errs.KindStateConflict,
			"Entry removal Environment epoch is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	return EntryRemovalConfiguration{
		Files:         snapshot,
		HeadRevision:  head,
		EpochRevision: read.Values[0].ModRevision,
	}, nil
}
