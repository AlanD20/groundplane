package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// The persistence fake consumes the same closed envelope and local validation
// as the real store; its underlying transaction preserves one atomic revision.
func (store *memoryHierarchyStore) TransactBlueprintTaskTerminal(
	ctx context.Context,
	envelope etcd.BlueprintTaskTerminalTransaction,
) (testkeyvalue.TransactionResult, error) {
	conditions, mutations, err := envelope.Operations()
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	defer testkeyvalue.ClearMutationValues(mutations)
	return store.Transact(ctx, conditions, mutations)
}

func (store *memoryHierarchyStore) ValidateBlueprintTaskTerminal(
	_ context.Context,
	envelope etcd.BlueprintTaskTerminalTransaction,
) error {
	return envelope.ValidateBudget("")
}
