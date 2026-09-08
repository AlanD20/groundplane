package etcd

import (
	"context"
	"math"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: budget proof must include physical namespace bytes and reserve the
// full positive ModRevision width before bounded release changes those keys.
func TestBlueprintTerminalPhysicalBudgetAndNonExecutableProjection(t *testing.T) {
	const prefix = "/groundplane"
	key := "/v1/records/tasks/budget"
	envelope := BlueprintTaskTerminalTransaction{
		taskID: "budget", projectionOnly: true,
		conditions: []Condition{{Key: key, ModRevision: math.MaxInt64}},
		mutations:  []Mutation{{Type: MutationPut, Key: key, Value: make([]byte, maximumTransactionBytes)}},
	}
	physicalKeys := []string{prefix + key}
	size := transactionRequest(envelope.conditions, envelope.mutations, physicalKeys, physicalKeys).Size()
	envelope.mutations[0].Value = envelope.mutations[0].Value[:maximumTransactionBytes-(size-maximumTransactionBytes)]
	if got := transactionRequest(envelope.conditions, envelope.mutations, physicalKeys, physicalKeys).Size(); got != maximumTransactionBytes {
		t.Fatalf("physical boundary fixture size = %d", got)
	}
	s := &store{root: prefix} // Validation and projection rejection need no client.
	if err := s.ValidateBlueprintTaskTerminal(context.Background(), envelope); err != nil {
		t.Fatalf("exact physical 1 MiB boundary: %v", err)
	}
	for _, revision := range []int64{1, 127, 128, math.MaxInt64} {
		envelope.conditions[0].ModRevision = revision
		if err := s.ValidateBlueprintTaskTerminal(context.Background(), envelope); err != nil {
			t.Fatalf("reserved revision width did not cover %d: %v", revision, err)
		}
	}
	if _, err := s.TransactBlueprintTaskTerminal(context.Background(), envelope); !isKind(err, errs.KindInternal) {
		t.Fatalf("budget-only projection was executable: %v", err)
	}
	if _, _, err := envelope.Operations(); !isKind(err, errs.KindInternal) {
		t.Fatalf("budget-only projection exposed executable operations: %v", err)
	}
	envelope.mutations[0].Value = append(envelope.mutations[0].Value, 0)
	if err := envelope.validate(); err != nil {
		t.Fatalf("logical boundary unexpectedly exceeded: %v", err)
	}
	if err := s.ValidateBlueprintTaskTerminal(context.Background(), envelope); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("physical 1 MiB plus one byte accepted: %v", err)
	}
}
