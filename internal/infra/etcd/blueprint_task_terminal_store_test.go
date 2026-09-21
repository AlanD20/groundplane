package etcd

import (
	"context"
	"fmt"
	"testing"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// The persistence fake consumes the same closed envelope and local validation
// as the real store; its underlying transaction preserves one atomic revision.
func (store *memoryHierarchyStore) TransactBlueprintTaskTerminal(
	ctx context.Context,
	envelope BlueprintTaskTerminalTransaction,
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
	envelope BlueprintTaskTerminalTransaction,
) error {
	return envelope.ValidateBudget("")
}

// Rationale: the completion envelope has exact per-arm and physical-byte
// ceilings without exposing a general larger-transaction escape hatch.
func TestBlueprintTerminalStoreKeepsBoundedArmsAndOrdinaryLimit(t *testing.T) {
	conditions := make([]testkeyvalue.Condition, 256)
	mutations := make([]testkeyvalue.Mutation, 256)
	for index := range conditions {
		conditions[index] = testkeyvalue.Condition{Key: fmt.Sprintf("/terminal/guard/%03d", index)}
		mutations[index] = testkeyvalue.Mutation{
			Type: testkeyvalue.MutationDelete,
			Key:  fmt.Sprintf("/terminal/value/%03d", index),
		}
	}
	backend := &fakeClient{transactionResponse: &clientv3.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 33}, Succeeded: true,
	}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatal(err)
	}
	envelope := BlueprintTaskTerminalTransaction{taskID: "test", conditions: conditions, mutations: mutations}
	if _, err := store.TransactBlueprintTaskTerminal(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if len(backend.transaction.conditions) != 256 || len(backend.transaction.operations) != 256 ||
		len(backend.transaction.otherwise) != 256 {
		t.Fatal("complete terminal request arms were not retained")
	}
	for _, variation := range []string{"comparison", "mutation", "bytes", "empty"} {
		t.Run(variation, func(t *testing.T) {
			candidate := envelope
			switch variation {
			case "comparison":
				candidate.conditions = append(
					append(
						[]testkeyvalue.Condition(nil),
						conditions...),
					testkeyvalue.Condition{Key: "/terminal/extra"},
				)
			case "mutation":
				candidate.mutations = append(
					append(
						[]testkeyvalue.Mutation(nil),
						mutations...),
					testkeyvalue.Mutation{Type: testkeyvalue.MutationDelete, Key: "/terminal/extra"},
				)
			case "bytes":
				candidate.mutations = []testkeyvalue.Mutation{
					{
						Type:  testkeyvalue.MutationPut,
						Key:   "/terminal/large",
						Value: make([]byte, testkeyvalue.MaximumBytes),
					},
				}
			case "empty":
				candidate = BlueprintTaskTerminalTransaction{}
			}
			backend.transaction = nil
			if _, err := store.TransactBlueprintTaskTerminal(context.Background(), candidate); err == nil {
				t.Fatal("invalid terminal envelope accepted")
			}
			if backend.transaction != nil {
				t.Fatal("invalid terminal envelope reached etcd")
			}
		})
	}
	backend.transaction = nil
	if _, err := store.Transact(context.Background(), conditions[:49], mutations[:48]); !isKind(
		err,
		errs.KindValidationFailed,
	) {
		t.Fatalf("ordinary 97-operation transaction: %v", err)
	}
	if backend.transaction != nil {
		t.Fatal("ordinary oversized transaction reached etcd")
	}
}
