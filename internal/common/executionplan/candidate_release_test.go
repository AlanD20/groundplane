package executionplan

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: the immutable plan owns one candidate Release procedure even
// when several Services are selected, and a first application must authorize
// exact candidate absence without fabricating a predecessor.
func TestBuildCandidateReleaseProcedureBlueprintFirstApplication(t *testing.T) {
	t.Parallel()

	procedure, err := BuildCandidateReleaseProcedure(CandidateReleaseProcedureInput{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		Members: []CandidateReleaseMemberInput{{
			ServiceID:           "svc_01K4A1B2C3D4E5F6G7H8J9K0MN",
			CandidateReleaseID:  "dep_01K4A1B2C3D4E5F6G7H8J9K0MN",
			CandidateArtifactID: "cfg_01K4A1B2C3D4E5F6G7H8J9K0MN",
			ForwardStepIDs: []string{
				"step_01K4A1B2C3D4E5F6G7H8J9K0MN",
				"step_01K4A1B2C3D4E5F6G7H8J9K0MP",
			},
			ServingPredecessor: &ServingPredecessorInput{
				ProbeStepID: "step_01K4A1B2C3D4E5F6G7H8J9K0MQ", CompensateStepID: "step_01K4A1B2C3D4E5F6G7H8J9K0MR",
			},
			CandidateAbsence: &CandidateAbsenceInput{
				ComposeProjectName: "gp-env_01K4A1B2C3D4E5F6G7H8J9K0MN",
				ProbeStepID:        "step_01K4A1B2C3D4E5F6G7H8J9K0MQ", CompensateStepID: "step_01K4A1B2C3D4E5F6G7H8J9K0MR",
				Services: []CandidateServiceIdentity{{
					ServiceID: "svc_01K4A1B2C3D4E5F6G7H8J9K0MN",
					ReleaseID: "dep_01K4A1B2C3D4E5F6G7H8J9K0MN",
				}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("build first-application candidate procedure: %v", err)
	}
	if len(procedure.GetMembers()) != 1 {
		t.Fatalf("candidate procedure members = %d, want one", len(procedure.GetMembers()))
	}
	member := procedure.GetMembers()[0]
	if member.GetCandidateAbsence() == nil || member.GetServingPredecessor() == nil {
		t.Fatal("Blueprint first application omitted a lawful restoration alternative")
	}
	if member.GetCandidateArtifactId() == "baseline" || member.GetCandidateReleaseId() == "baseline" {
		t.Fatal("first application fabricated baseline authority")
	}
}

// Rationale: mutation-bearing plans must not omit their one hash-covered
// candidate Release procedure.
func TestValidateCandidateReleasePlanRequiresProcedure(t *testing.T) {
	t.Parallel()

	plan := validPlan()
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	plan.Steps[0].Payload = &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
		ArtifactId:    plan.Artifacts[0].ArtifactId,
		ServiceIds:    []string{"svc_01K4A1B2C3D4E5F6G7H8J9K0MN"},
		ForceRecreate: true, NoDependencies: true,
	}}
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal() accepted candidate mutation without a candidate Release procedure")
	}
}

func TestCandidateReleaseDescriptorRejectsAnotherSealedPlan(t *testing.T) {
	t.Parallel()

	plan, err := Seal(validBlueprintScriptReconcilePlan(t))
	if err != nil {
		t.Fatalf("seal candidate plan: %v", err)
	}
	descriptor, err := DescribeCandidateRelease(plan)
	if err != nil {
		t.Fatalf("describe candidate release: %v", err)
	}

	other := proto.Clone(plan).(*agentpb.ExecutionPlan)
	other.Steps[0].TimeoutSeconds++
	other.PlanHash = nil
	other, err = Seal(other)
	if err != nil {
		t.Fatalf("seal other plan: %v", err)
	}

	if err := CandidateReleaseDescriptorMatchesPlan(descriptor, other); err == nil {
		t.Fatal("expected descriptor from another sealed plan to be rejected")
	}
}
