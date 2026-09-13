package taskcontract

import "testing"

// QA: BP-11; local plan-budget arithmetic.
// Rationale: candidate step counts must reserve both recovery steps per member;
// omitting them can admit a plan whose actual procedure exceeds its bound.
func TestBlueprintReleaseProcedureStepCountIncludesRecoveryPair(t *testing.T) {
	t.Parallel()
	if got, valid := BlueprintReleaseProcedureStepCount(3, 2); !valid || got != 14 {
		t.Fatalf("BlueprintReleaseProcedureStepCount(3, 2) = %d, %t, want 14, true", got, valid)
	}
}
