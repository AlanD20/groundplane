package executionplan

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: SVC-09 recovery may observe the failed candidate beside its exact
// predecessor, but only the sealed member/pair can authorize that distinction.
func TestPlanRestorationObservationBindsCandidateAndPredecessor(t *testing.T) {
	plan := candidateRuntimeBlueGreenPlan(t, true)
	before := proto.CloneOf(plan)
	member := plan.CandidateReleaseProcedure.Members[0]
	for _, stepID := range []string{member.ServingPredecessor.ProbeStepId, member.ServingPredecessor.CompensateStepId} {
		for _, artifactID := range []string{member.ServingPredecessor.PriorArtifactId, member.CandidateArtifactId} {
			observation, err := NewPlanRestorationObservation(plan, stepID, artifactID)
			if err != nil {
				t.Fatal(err)
			}
			var expected *agentpb.ComposeArtifact
			for _, artifact := range plan.Artifacts {
				if artifact.ArtifactId == artifactID {
					expected = artifact
				}
			}
			if !proto.Equal(observation.Artifact(), expected) {
				t.Fatal("historical artifact changed")
			}
			workloads := observation.CandidateWorkloads()
			if len(workloads) != 1 || workloads[0].Slot != "blue" ||
				workloads[0].ServiceId != member.ServiceId || workloads[0].Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
				t.Fatalf("candidate scope = %v", workloads)
			}
			workloads[0].ExpectedLabels[0].Value = "foreign"
			if proto.Equal(workloads[0], observation.CandidateWorkloads()[0]) {
				t.Fatal("mutable observation authority")
			}
		}
	}
	if !proto.Equal(plan, before) {
		t.Fatal("observation rewrote plan")
	}
	for _, test := range []struct{ name, stepID, artifactID string }{
		{"forward step", member.ForwardStepIds[0], member.CandidateArtifactId},
		{"unknown artifact", member.ServingPredecessor.ProbeStepId, "unknown"},
		{"unknown step", "unknown", member.ServingPredecessor.PriorArtifactId},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewPlanRestorationObservation(plan, test.stepID, test.artifactID); err == nil {
				t.Fatal("unbound observation accepted")
			}
		})
	}
	plan.PlanHash[0] ^= 1
	if _, err := NewPlanRestorationObservation(plan, member.ServingPredecessor.ProbeStepId, member.ServingPredecessor.PriorArtifactId); err == nil {
		t.Fatal("changed plan accepted")
	}
}
