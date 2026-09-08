package executionplan

import (
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: snapshot and projection must agree semantically even if all
// nested hashes are refreshed; historical runners have the same rule as new ones.
func TestSealRejectsRehashedScriptProjectionImageMismatch(t *testing.T) {
	for _, test := range []struct {
		name  string
		build func(*testing.T) *agentpb.ExecutionPlan
	}{{"historical", validManualScriptPlan}, {"candidate", validBlueprintScriptReconcilePlan}} {
		t.Run(test.name, func(t *testing.T) {
			plan := test.build(t)
			if _, err := Seal(plan); err != nil {
				t.Fatalf("invalid baseline: %v", err)
			}
			plan.ScriptRunnerProjections[0].Image = "sha256:" + strings.Repeat("b", 64)
			refreshScriptProjectionImageDigestForTest(t, plan)
			if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Seal(mismatched runner image) = %v", err)
			}
		})
	}
}

// Rationale: agreement between a runner's own documents cannot substitute for
// matching the exact candidate workload already authorized by ComposeApply.
func TestSealRejectsRehashedRunnerImageDifferentFromCandidate(t *testing.T) {
	plan := validBlueprintScriptReconcilePlan(t)
	if _, err := Seal(plan); err != nil {
		t.Fatalf("invalid baseline: %v", err)
	}
	plan.ScriptRunnerSnapshots[0].LocalImageId = "sha256:" + strings.Repeat("b", 64)
	plan.ScriptRunnerProjections[0].Image = plan.ScriptRunnerSnapshots[0].LocalImageId
	refreshScriptProjectionImageDigestForTest(t, plan)
	if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Seal(runner differs from candidate) = %v", err)
	}
}

func refreshScriptProjectionImageDigestForTest(t *testing.T, plan *agentpb.ExecutionPlan) {
	t.Helper()
	digest, err := scriptMessageDigest(plan.ScriptRunnerProjections[0])
	if err != nil {
		t.Fatal(err)
	}
	plan.ScriptRunnerSnapshots[0].RunnerProjectionSha256 = digest
	refreshBlueprintSnapshotDigestForTest(t, plan)
}
