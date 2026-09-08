package composehelper

import (
	"bytes"
	"context"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: a negative restoration observation is neither failed observation
// nor successful restoration. It cannot carry contradictory success evidence.
func TestRestorationRequiredResponseHasNoSuccessEvidence(t *testing.T) {
	valid := &agentpb.ComposeHelperResponse{Schema: SchemaVersion,
		Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_RESTORATION_REQUIRED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE}
	if err := validateResponse(valid); err != nil {
		t.Fatalf("negative observation rejected: %v", err)
	}
	var frame bytes.Buffer
	if err := WriteResponse(context.Background(), &frame, valid); err != nil {
		t.Fatal(err)
	}
	decoded, err := ReadResponse(context.Background(), &frame)
	if err != nil || !proto.Equal(valid, decoded) {
		t.Fatalf("negative observation wire roundtrip: %v", err)
	}
	for _, mutate := range []func(*agentpb.ComposeHelperResponse){
		func(r *agentpb.ComposeHelperResponse) { r.ExitCode = 1 },
		func(r *agentpb.ComposeHelperResponse) {
			r.Diagnostic = agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED
		},
		func(r *agentpb.ComposeHelperResponse) { r.ProxyEvidence = &agentpb.ServiceProxyEvidence{} },
		func(r *agentpb.ComposeHelperResponse) { r.RecreateEvidence = &agentpb.ServiceRecreateEvidence{} },
		func(r *agentpb.ComposeHelperResponse) {
			r.CandidateAbsenceEvidence = &agentpb.CandidateAbsenceEvidence{}
		},
	} {
		changed := proto.CloneOf(valid)
		mutate(changed)
		if err := validateResponse(changed); err == nil {
			t.Fatal("contradictory negative observation accepted")
		}
	}
}
