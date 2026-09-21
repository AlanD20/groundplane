package etcd

import (
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"testing"
)

func TestZoneRemovalTransactionsRemainStrictlyBelowEtcdOperationLimit(t *testing.T) {
	t.Parallel()
	conditions := make([]testkeyvalue.Condition, 47)
	mutations := make([]testkeyvalue.Mutation, 48)
	for _, phase := range []zoneRemovalTransactionPhase{
		zoneRemovalTransactionBegin,
		zoneRemovalTransactionRetry,
		zoneRemovalTransactionCompletedAcknowledgement,
		zoneRemovalTransactionFailedAcknowledgement,
	} {
		phase := phase
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			if operations := len(conditions) + len(mutations); operations != 95 {
				t.Fatalf("Zone removal %s transaction operations = %d, want 95", phase, operations)
			}
			if err := validateZoneRemovalTransactionBudget(phase, conditions, mutations); err != nil {
				t.Fatalf("validateZoneRemovalTransactionBudget(%s, 95) error = %v", phase, err)
			}
			atLimit := append(append([]testkeyvalue.Mutation(nil), mutations...), testkeyvalue.Mutation{})
			if err := validateZoneRemovalTransactionBudget(phase, conditions, atLimit); err == nil {
				t.Fatalf("validateZoneRemovalTransactionBudget(%s, 96) error = nil", phase)
			}
		})
	}
}
