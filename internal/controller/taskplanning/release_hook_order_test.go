package taskplanning

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: every frozen phase must honor numeric order before slug, without
// consulting mutable Script metadata or breaking the cleanup prerequisite chain.
func TestBuildReleaseHookPlanUsesFrozenNumericOrderInEveryPhase(t *testing.T) {
	for _, operation := range []domain.OperationKind{domain.OperationDeploy, domain.OperationRollback} {
		t.Run(string(operation), func(t *testing.T) {
			pre, post := core.ScriptPreDeploy, core.ScriptPostDeploy
			if operation == domain.OperationRollback {
				pre, post = core.ScriptPreRollback, core.ScriptPostRollback
			}
			for _, phase := range []core.ScriptHook{pre, post, core.ScriptOnFailure} {
				t.Run(string(phase), func(t *testing.T) {
					const releaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
					hooks := []testreleaserender.ReleaseHookRenderInput{
						releaseHookInput(t, releaseID, "a-later", phase,
							"01ARZ3NDEKTSV4RRFFQ69G5FAA", "01ARZ3NDEKTSV4RRFFQ69G5FAB"),
						releaseHookInput(t, releaseID, "z-first", phase,
							"01ARZ3NDEKTSV4RRFFQ69G5FAC", "01ARZ3NDEKTSV4RRFFQ69G5FAD"),
						releaseHookInput(t, releaseID, "b-later", phase,
							"01ARZ3NDEKTSV4RRFFQ69G5FAE", "01ARZ3NDEKTSV4RRFFQ69G5FAF"),
					}
					hooks[0].Order, hooks[1].Order, hooks[2].Order = 65535, 0, 65535
					stepIDs := []string{"step-first", "step-second", "step-third"}
					input := ReleaseHookPlanInput{
						Operation: operation, CandidateReleaseID: releaseID, FailureReleaseID: releaseID,
						PostHookAnchorStepID: "start", CompensationStepID: "compensate", Hooks: hooks,
					}
					switch phase {
					case pre:
						input.PreStepIDs = stepIDs
					case post:
						input.PostStepIDs = stepIDs
					default:
						input.FailureStepIDs = stepIDs
					}
					plan, err := BuildReleaseHookPlan(input)
					if err != nil {
						t.Fatal(err)
					}
					var steps []*agentpb.ExecutionStep
					steps = append(steps, plan.PreSteps...)
					steps = append(steps, plan.PostSteps...)
					steps = append(steps, plan.FailureSteps...)
					for index, original := range []int{1, 0, 2} {
						if steps[index].GetRunScript().ScriptExecutionId != hooks[original].ScriptExecutionID {
							t.Fatalf("step %d did not select numeric order then slug", index)
						}
						if index > 0 && steps[index].PrerequisiteStepId != stepIDs[index-1] {
							t.Fatalf("step %d lost the preceding cleanup prerequisite", index)
						}
					}
				})
			}
		})
	}
}
