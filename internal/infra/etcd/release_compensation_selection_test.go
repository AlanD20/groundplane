package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: a damaged member map must reject before indexing by procedure
// ordinal, even when a completed forward step makes compensation applicable.
func TestApplicableCompensationRejectsMissingMemberAuthority(t *testing.T) {
	procedure := &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{{
		ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", CandidateReleaseId: candidateSelectionReleaseID,
		ForwardStepIds:   []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		CandidateAbsence: &agentpb.CandidateAbsenceRestoration{CompensateStepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"},
	}}}
	evidence := []releaseRecoveryMutationEvidence{
		{StepID: procedure.Members[0].ForwardStepIds[0], Running: true, Completed: true},
	}
	if _, err := releaseApplicableCompensationStepIDs(procedure, nil, evidence); err == nil {
		t.Fatal("missing member selection accepted")
	}
}

const candidateSelectionReleaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
