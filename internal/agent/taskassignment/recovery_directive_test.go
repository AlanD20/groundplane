package taskassignment

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: recovery must probe captured files before applying compensation;
// reordered instructions cannot reuse otherwise valid Task authority.
func TestRecoveryDirectiveRejectsReorderedConfigurationSteps(t *testing.T) {
	procedure := &agentpb.CandidateReleaseProcedure{
		ConfigurationRestoration: &agentpb.ConfigurationRestoration{
			Files: []*agentpb.ConfigurationFileRestoration{{
				ProbeStepId: "file-probe", CompensateStepId: "file-restore",
			}},
		},
	}
	directive := &agentpb.ReleaseRecoveryDirective{
		StepIds:                       executionplan.RecoveryStepIDs(procedure),
		Phase:                         agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE,
		ApplicableCompensationStepIds: []string{"file-restore"},
	}
	if err := validateReleaseRecoveryDirective(procedure, directive); err != nil {
		t.Fatalf("canonical directive: %v", err)
	}
	directive.StepIds[0], directive.StepIds[1] = directive.StepIds[1], directive.StepIds[0]
	if err := validateReleaseRecoveryDirective(procedure, directive); err == nil {
		t.Fatal("noncanonical recovery directive was accepted")
	}
}
