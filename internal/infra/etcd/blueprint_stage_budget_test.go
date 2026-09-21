package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	strings "strings"
	testing "testing"
)

func TestEnvironmentBlueprintStageTransactionBudgetIsExact(t *testing.T) {
	t.Parallel()
	conditions := make([]testkeyvalue.Condition, 13)
	mutations := make([]testkeyvalue.Mutation, 13)
	for index := range conditions {
		conditions[index] = testkeyvalue.Condition{
			Key: strings.Repeat(string(rune('a'+index)), blueprints.EnvironmentBlueprintKeyMaxBytes),
		}
	}
	for index := range mutations {
		valueBytes := 97 + blueprints.EnvironmentBlueprintChunkBytes
		if index == len(mutations)-1 {
			valueBytes = 4 * 1024
		}
		mutations[index] = testkeyvalue.Mutation{
			Type:  testkeyvalue.MutationPut,
			Key:   strings.Repeat(string(rune('n'+index)), blueprints.EnvironmentBlueprintKeyMaxBytes),
			Value: make([]byte, valueBytes),
		}
	}
	if err := ValidateBlueprintTransaction(
		newMemoryHierarchyStore(), conditions, mutations, 26, blueprints.EnvironmentBlueprintStageTransactionBytes,
	); err != nil {
		t.Fatalf("validateBlueprintTransaction(exact) error = %v", err)
	}
	mutations[len(mutations)-1].Value = append(mutations[len(mutations)-1].Value, 0)
	if err := ValidateBlueprintTransaction(
		newMemoryHierarchyStore(), conditions, mutations, 26, blueprints.EnvironmentBlueprintStageTransactionBytes,
	); !isKind(err, errs.KindInternal) {
		t.Fatalf("validateBlueprintTransaction(above) error = %v", err)
	}
}
