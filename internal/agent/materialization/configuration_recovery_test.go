package materialization

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// SVC-15/JOURNEY-02: probe and compensation must share one retained source,
// rather than retaining two independently owned copies of secret configuration.
// Worker restoration and re-probe behavior remains in the Agent integration test.
func TestConfigurationRecoverySharesOneRetainedBuffer(t *testing.T) {
	fixture := newConfigurationRecoveryFixture(t)
	recovery := recoveryAssignment(fixture, agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE)
	inbox := NewInbox()
	if err := inbox.Register(recovery); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { inbox.Release(recovery.TaskID) })
	for index, step := range recovery.Plan.Steps {
		if step.StepId != fixture.probe.StepId && step.StepId != fixture.compensate.StepId {
			continue
		}
		for _, transfer := range materializationTransfersForStep(recovery, index, fixture.prior, 7) {
			if err := inbox.Accept(t.Context(), transfer); err != nil {
				t.Fatal(err)
			}
		}
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	probe := inbox.tasks[recovery.TaskID].steps[fixture.probe.StepId]
	compensate := inbox.tasks[recovery.TaskID].steps[fixture.compensate.StepId]
	if probe.shared == nil || probe.shared != compensate.shared || probe.shared.refs != 2 ||
		probe.content != nil || compensate.content != nil {
		t.Fatal("probe and compensation did not share one owned retained buffer")
	}
}
