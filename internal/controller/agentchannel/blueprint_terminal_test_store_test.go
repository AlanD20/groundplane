package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (store *channelMemoryStore) ValidateBlueprintTaskTerminal(
	_ context.Context,
	envelope etcd.BlueprintTaskTerminalTransaction,
) error {
	return envelope.ValidateBudget("")
}

func (store *fakeTaskStore) ReconnectAgentAssignment(
	_ context.Context, assignment etcd.TaskAssignment,
) (etcd.TaskAssignment, error) {
	return assignment, nil
}

func (store *environmentAcknowledgementStore) ReconnectAgentAssignment(
	_ context.Context, assignment etcd.TaskAssignment,
) (etcd.TaskAssignment, error) {
	return assignment, nil
}

func (store *blockingClaimTaskStore) ReconnectAgentAssignment(
	ctx context.Context, assignment etcd.TaskAssignment,
) (etcd.TaskAssignment, error) {
	return store.repository.ReconnectAgentAssignment(ctx, assignment)
}

func (store *channelMemoryStore) TransactBlueprintTaskTerminal(
	ctx context.Context, envelope etcd.BlueprintTaskTerminalTransaction,
) (testkeyvalue.TransactionResult, error) {
	conditions, mutations, err := envelope.Operations()
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	defer func() {
		for _, mutation := range mutations {
			clear(mutation.Value)
		}
	}()
	return store.Transact(ctx, conditions, mutations)
}
