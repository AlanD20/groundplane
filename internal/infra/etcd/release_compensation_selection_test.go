package etcd

import (
	"slices"
	"testing"

	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
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
	evidence := []testtaskassignments.ReleaseRecoveryMutationEvidence{
		{StepID: procedure.Members[0].ForwardStepIds[0], Running: true, Completed: true},
	}
	if _, err := releaseApplicableCompensationStepIDs(procedure, nil, evidence); err == nil {
		t.Fatal("missing member selection accepted")
	}
}

const candidateSelectionReleaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"

// SVC-15: a failed native apply may have replaced the inactive slot after its
// durable Running barrier. Recovery must retain that obligation without a
// Completed event; an untouched member still cannot authorize compensation.
func TestApplicableCompensationIncludesFailedNativeMutation(t *testing.T) {
	for _, target := range []testtaskassignments.ReleaseRestorationTarget{
		testtaskassignments.ReleaseRestorationServingPredecessor,
		testtaskassignments.ReleaseRestorationCandidateAbsence,
	} {
		member := &agentpb.CandidateReleaseMember{
			ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", CandidateReleaseId: candidateSelectionReleaseID,
			ForwardStepIds: []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			ServingPredecessor: &agentpb.ServingPredecessorRestoration{
				CompensateStepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			},
			CandidateAbsence: &agentpb.CandidateAbsenceRestoration{
				CompensateStepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAX",
			},
		}
		procedure := &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{member}}
		candidates := []testtaskassignments.ReleaseRestorationCandidate{{
			ServiceID: member.ServiceId, ReleaseID: member.CandidateReleaseId, Target: target,
		}}
		if target == testtaskassignments.ReleaseRestorationServingPredecessor {
			member.CandidateAbsence = nil
		} else {
			member.ServingPredecessor = nil
		}
		want := executionCompensationStep(member)
		for _, completed := range []bool{false, true} {
			actual, err := releaseApplicableCompensationStepIDs(procedure, candidates,
				[]testtaskassignments.ReleaseRecoveryMutationEvidence{{
					StepID: member.ForwardStepIds[0], Running: true, Completed: completed,
				}})
			if err != nil || !slices.Equal(actual, []string{want}) {
				t.Fatalf("target=%s completed=%t compensation=%v error=%v", target, completed, actual, err)
			}
		}
		actual, err := releaseApplicableCompensationStepIDs(procedure, candidates, nil)
		if err != nil || len(actual) != 0 {
			t.Fatalf("untouched target=%s compensation=%v error=%v", target, actual, err)
		}
	}
}

func executionCompensationStep(member *agentpb.CandidateReleaseMember) string {
	if member.GetServingPredecessor() != nil {
		return member.GetServingPredecessor().GetCompensateStepId()
	}
	return member.GetCandidateAbsence().GetCompensateStepId()
}
