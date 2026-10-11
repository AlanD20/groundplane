package desiredrevision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// A durable round-robin cursor lets held/current revisions coexist with expired
// ones without pinning every cleanup pass to the first descriptor page.
func (repository *Repository) readCleanupCursor(ctx context.Context) (string, int64, error) {
	read, err := repository.store.Get(ctx, blueprints.EnvironmentBlueprintCleanupCursorKey)
	if err != nil {
		return "", 0, err
	}
	if read == nil || read.ReadRevision <= 0 {
		return "", 0, blueprints.CorruptEnvironmentBlueprintStage()
	}
	if read.Entry == nil {
		return "", 0, nil
	}
	defer clear(read.Entry.Value)
	if read.Entry.ModRevision <= 0 {
		return "", 0, blueprints.CorruptEnvironmentBlueprintStage()
	}
	key := string(read.Entry.Value)
	if _, err := parseEnvironmentBlueprintDescriptorKey(key); err != nil {
		return "", 0, err
	}
	return key, read.Entry.ModRevision, nil
}

func (repository *Repository) advanceCleanupCursor(
	ctx context.Context, page *keyvalue.RangeResult, cursorRevision int64,
) error {
	mutation := keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: blueprints.EnvironmentBlueprintCleanupCursorKey}
	if page.More {
		if len(page.Values) == 0 {
			return blueprints.CorruptEnvironmentBlueprintStage()
		}
		mutation.Type = keyvalue.MutationPut
		mutation.Value = []byte(page.Values[len(page.Values)-1].Key)
		defer clear(mutation.Value)
	}
	conditions := []keyvalue.Condition{{Key: mutation.Key, ModRevision: cursorRevision}}
	mutations := []keyvalue.Mutation{mutation}
	if err := etcd.ValidateBlueprintTransaction(repository.store, conditions, mutations, 2,
		blueprints.EnvironmentBlueprintGCTransactionBytes); err != nil {
		return err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	keyvalue.ClearValues(result.FailureReads)
	// A concurrent leader pass may already have advanced the cursor. Its
	// revision-owned deletions remain valid; the losing pass never overwrites it.
	return nil
}
