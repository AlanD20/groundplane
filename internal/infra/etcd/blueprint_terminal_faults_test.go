package etcd

import (
	"context"
	"testing"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintTerminalFaultStore struct {
	*memoryHierarchyStore
	t                 *testing.T
	epochKey          string
	calls             int
	committedRevision int64
	fault             string
	raceRevision      int64
}

func (store *blueprintTerminalFaultStore) TransactBlueprintTaskTerminal(
	ctx context.Context, envelope BlueprintTaskTerminalTransaction,
) (testkeyvalue.TransactionResult, error) {
	store.calls++
	conditions, mutations, err := envelope.Operations()
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	defer testkeyvalue.ClearMutationValues(mutations)
	if store.calls != 1 {
		store.t.Fatal("terminal persistence repeated after authority loss or uncertain commit")
	}
	if store.fault == "compare-loss" {
		value := store.valueAt(store.epochKey, store.revision)
		if value == nil {
			store.t.Fatal("terminal fault fixture has no Environment epoch")
		}
		// Same bytes, new ModRevision: force the actual old compare to fail.
		if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: store.epochKey, Value: value.Value}}); err != nil {
			store.t.Fatal(err)
		}
		before := store.revision
		store.raceRevision = before
		result, err := store.Transact(ctx, conditions, mutations)
		if err != nil || result.Succeeded || store.revision != before {
			store.t.Fatalf("lost terminal compare performed partial writes: %v", err)
		}
		return result, err
	}
	result, err := store.Transact(ctx, conditions, mutations)
	if err != nil || !result.Succeeded {
		store.t.Fatalf("terminal composition did not commit: %v", err)
	}
	store.committedRevision = result.Revision
	for _, mutation := range mutations {
		if mutation.Prefix {
			continue // Prefix semantics are covered by the production store tests.
		}
		value := store.valueAt(mutation.Key, store.revision)
		if mutation.Type == testkeyvalue.MutationPut && (value == nil || value.ModRevision != result.Revision) ||
			mutation.Type == testkeyvalue.MutationDelete && value != nil {
			store.t.Fatalf("terminal mutation did not share the atomic commit: %s", mutation.Key)
		}
	}
	return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal, "injected lost terminal commit response")
}
