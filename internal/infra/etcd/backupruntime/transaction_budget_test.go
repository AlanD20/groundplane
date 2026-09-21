package backupruntime

import (
	errors "errors"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	testing "testing"
)

// Rationale: transaction preflight accepts the exact byte boundary; exceeding
// a locked internal composition ceiling is an implementation defect, not bad input.
func TestBackupRuntimeRepositoryTransactionBounds(t *testing.T) {
	t.Parallel()
	conditions := make([]testkeyvalue.Condition, testkeyvalue.MaximumOperations)
	if err := ValidateBackupRuntimeTransactionBounds(conditions, nil); err != nil {
		t.Fatalf("exact operation boundary error = %v", err)
	}
	conditions = make([]testkeyvalue.Condition, testkeyvalue.MaximumOperations+1)
	if err := ValidateBackupRuntimeTransactionBounds(conditions, nil); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("operation overflow error = %v", err)
	}
	key := "/v1/runtime/boundary"
	value := make([]byte, maximumBackupRuntimeTransactionBytes-len(key)-64)
	if err := ValidateBackupRuntimeTransactionBounds(
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
	); err != nil {
		t.Fatalf("exact byte boundary error = %v", err)
	}
	value = append(value, 0)
	if err := ValidateBackupRuntimeTransactionBounds(
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("byte overflow error = %v", err)
	}
}
