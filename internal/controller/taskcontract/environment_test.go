package taskcontract

import "testing"

func TestBlueprintReleaseProcedureStepCountIncludesRecoveryPair(t *testing.T) {
	t.Parallel()
	if got, valid := BlueprintReleaseProcedureStepCount(3, 2); !valid || got != 14 {
		t.Fatalf("BlueprintReleaseProcedureStepCount(3, 2) = %d, %t, want 14, true", got, valid)
	}
}
