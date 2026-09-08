package agent

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: the last member's successful compensation must not erase another
// member's proof or conceal its still-unproven absence.
func TestReleaseAbsenceAggregationPreservesEachMember(t *testing.T) {
	_, _, first := exactCandidateAbsenceCompensation()
	second := proto.CloneOf(first)
	second.Candidates = []*agentpb.CandidateReleaseService{{ServiceId: "worker", ReleaseId: "release-worker"}}
	second.AbsenceProven = false
	state := &releaseExecutionState{}
	if err := state.recordAbsenceEvidence(second); err != nil {
		t.Fatal(err)
	}
	if err := state.recordAbsenceEvidence(first); err != nil {
		t.Fatal(err)
	}
	got := state.aggregateAbsenceEvidence()
	if got.GetAbsenceProven() || len(got.GetCandidates()) != 2 || got.Candidates[0].ServiceId != "api" ||
		got.Candidates[1].ServiceId != "worker" {
		t.Fatalf("partial aggregate = %v", got)
	}
	second.AbsenceProven = true
	if err := state.recordAbsenceEvidence(second); err != nil {
		t.Fatal(err)
	}
	if !state.aggregateAbsenceEvidence().GetAbsenceProven() {
		t.Fatal("completed member proof did not advance aggregate")
	}
	second.Candidates[0].ReleaseId = "wrong-release"
	if err := state.recordAbsenceEvidence(second); err == nil {
		t.Fatal("member identity changed within one recovery")
	}
}

// Rationale: per-member evidence from different assignment authorities cannot
// be combined into a seemingly complete terminal proof.
func TestReleaseAbsenceAggregationRejectsDifferentAuthority(t *testing.T) {
	_, _, first := exactCandidateAbsenceCompensation()
	state := &releaseExecutionState{}
	if err := state.recordAbsenceEvidence(first); err != nil {
		t.Fatal(err)
	}
	changed := proto.CloneOf(first)
	changed.AuthoritySha256[0] ^= 1
	if err := state.recordAbsenceEvidence(changed); err == nil {
		t.Fatal("different authority was merged")
	}
	if !proto.Equal(first, state.aggregateAbsenceEvidence()) {
		t.Fatal("failed merge changed retained proof")
	}
}
