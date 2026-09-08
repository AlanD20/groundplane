package agent

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: ordinary Deploy/Rollback carry the same immutable per-Service
// witness as Blueprint. Matching IDs alone cannot authorize changed bytes.
func TestOrdinaryAssignmentBindsNativePredecessorBytes(t *testing.T) {
	for _, operation := range []agentpb.PlanOperation{
		agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK,
	} {
		t.Run(operation.String(), func(t *testing.T) {
			assignment, current, _ := nativeServingAssignment(t)
			assignment.Plan.Operation = operation
			assignment.Plan.CandidateReleaseProcedure.Members[0].CandidateAbsence = nil
			if err := validateCandidateReleaseAssignmentAuthority(assignment, assignment.Plan); err != nil {
				t.Fatalf("ordinary per-Service predecessor rejected: %v", err)
			}
			changed := proto.CloneOf(current)
			changed.CanonicalYaml = []byte("changed historical configuration")
			sealNativeAssignmentArtifact(t, assignment.RestorationAuthority.NativePredecessors[0], changed, nil)
			if err := validateCandidateReleaseAssignmentAuthority(assignment, assignment.Plan); err == nil {
				t.Fatal("same-ID predecessor with different bytes accepted")
			}
		})
	}
}
