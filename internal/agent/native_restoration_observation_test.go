package agent

import (
	"bytes"
	"testing"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: restoration and independent observation must use the same captured
// Service Release, even when the applied Environment artifact has another identity.
func TestNativeRestorationObservationUsesCapturedRelease(t *testing.T) {
	for _, target := range []string{"singleton", "blue"} {
		t.Run(target, func(t *testing.T) {
			assignment, probe := sealedRecreateProbeAssignment(t, 1, target)
			authority := assignment.RestorationAuthority
			applied := &agentpb.ComposeArtifact{}
			if err := proto.Unmarshal(authority.AppliedPredecessor.ComposeArtifact, applied); err != nil {
				t.Fatal(err)
			}
			applied.ArtifactId = nativeAssignmentRetainedArtifact
			sealAssignmentWitness(authority, marshalNativeAssignmentArtifact(t, applied))
			planBefore, authorityBefore := proto.CloneOf(assignment.Plan), proto.CloneOf(authority)
			member := assignment.Plan.CandidateReleaseProcedure.Members[0]
			for _, stepID := range []string{probe.StepId, member.ServingPredecessor.CompensateStepId} {
				observation, err := executionplan.NewRestorationObservation(assignment.Plan, authority, stepID)
				if err != nil {
					t.Fatal(err)
				}
				artifact := observation.Artifact()
				encoded := marshalNativeAssignmentArtifact(t, artifact)
				if !bytes.Equal(encoded, authority.NativePredecessors[0].CurrentArtifact) ||
					bytes.Equal(encoded, authority.AppliedPredecessor.ComposeArtifact) {
					t.Fatal("observation did not select the exact native predecessor")
				}
				artifact.OwnerId = "mutated-copy"
				if observation.Artifact().OwnerId == artifact.OwnerId {
					t.Fatal("observation exposed mutable authority")
				}
			}
			if !proto.Equal(planBefore, assignment.Plan) || !proto.Equal(authorityBefore, authority) {
				t.Fatal("observation changed the sealed plan or authority")
			}
			authority.AppliedPredecessor = nil
			if _, err := executionplan.NewRestorationObservation(assignment.Plan, authority, probe.StepId); err != nil {
				t.Fatalf("native observation required an applied Environment artifact: %v", err)
			}
		})
	}

}

// Rationale: the read-only descriptor is an authority boundary, not a fallback
// reader; absent, substituted or unbound native bytes must never reach observation.
func TestNativeRestorationObservationRejectsChangedAuthority(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testtaskassignment.Assignment)
	}{
		{"missing native", func(a *testtaskassignment.Assignment) { a.RestorationAuthority.NativePredecessors = nil }},
		{"missing current", func(a *testtaskassignment.Assignment) {
			a.RestorationAuthority.NativePredecessors[0].CurrentArtifact = nil
		}},
		{"foreign service", func(a *testtaskassignment.Assignment) {
			a.RestorationAuthority.NativePredecessors[0].ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		}},
		{"changed bytes", func(a *testtaskassignment.Assignment) {
			a.RestorationAuthority.NativePredecessors[0].CurrentArtifact[0] ^= 0xff
		}},
		{"same identity different content", func(a *testtaskassignment.Assignment) {
			current := &agentpb.ComposeArtifact{}
			if err := proto.Unmarshal(a.RestorationAuthority.NativePredecessors[0].CurrentArtifact, current); err != nil {
				t.Fatal(err)
			}
			current.Services[0].ExpectedReplicas++
			a.RestorationAuthority.NativePredecessors[0].CurrentArtifact = marshalNativeAssignmentArtifact(t, current)
		}},
		{"unbound retained", func(a *testtaskassignment.Assignment) {
			a.RestorationAuthority.NativePredecessors[0].RetainedPriorArtifact =
				bytes.Clone(a.RestorationAuthority.NativePredecessors[0].CurrentArtifact)
		}},
		{"changed selection", func(a *testtaskassignment.Assignment) {
			a.RestorationAuthority.Candidates[0].Target = agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE
		}},
		{"changed applied digest", func(a *testtaskassignment.Assignment) {
			a.RestorationAuthority.AppliedPredecessor.ComposeArtifactSha256[0] ^= 1
		}},
		{"changed plan hash", func(a *testtaskassignment.Assignment) { a.Plan.PlanHash[0] ^= 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			assignment, probe := sealedRecreateProbeAssignment(t, 1, "singleton")
			test.mutate(&assignment)
			if _, err := executionplan.NewRestorationObservation(assignment.Plan, assignment.RestorationAuthority, probe.StepId); err == nil {
				t.Fatal("observation accepted changed recovery authority")
			}
		})
	}
	assignment, probe := sealedRecreateProbeAssignment(t, 1, "singleton")
	assignment.RestorationAuthority.NativePredecessors = nil
	if _, err := executionplan.NewRestorationObservation(
		assignment.Plan,
		assignment.RestorationAuthority,
		probe.StepId,
	); err == nil {
		t.Fatal("recovery fell back to the applied Environment artifact")
	}
}
