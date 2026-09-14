package executionplan

import (
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: SVC-15/JOURNEY-02 reconnect must replay file probes and
// compensations in their sealed order around the existing native recovery.
func TestRecoveryStepIDsUsesCanonicalFileAndNativeOrder(t *testing.T) {
	t.Parallel()
	procedure := &agentpb.CandidateReleaseProcedure{
		ConfigurationRestoration: &agentpb.ConfigurationRestoration{Files: []*agentpb.ConfigurationFileRestoration{
			{ProbeStepId: "file-probe-a", CompensateStepId: "file-compensate-a"},
			{ProbeStepId: "file-probe-b", CompensateStepId: "file-compensate-b"},
		}},
		Members: []*agentpb.CandidateReleaseMember{
			{ServingPredecessor: &agentpb.ServingPredecessorRestoration{
				ProbeStepId: "native-probe-a", CompensateStepId: "native-compensate-a",
			}},
			{CandidateAbsence: &agentpb.CandidateAbsenceRestoration{
				ProbeStepId: "native-probe-b", CompensateStepId: "native-compensate-b",
			}},
		},
	}
	want := []string{
		"file-probe-a", "file-probe-b", "native-probe-a", "native-probe-b",
		"file-compensate-a", "file-compensate-b", "native-compensate-b", "native-compensate-a",
	}
	if got := RecoveryStepIDs(procedure); !slices.Equal(got, want) {
		t.Fatalf("RecoveryStepIDs() = %v, want %v", got, want)
	}
	if got := RecoveryStepIDs(nil); got != nil {
		t.Fatalf("RecoveryStepIDs(nil) = %v, want nil", got)
	}
}
