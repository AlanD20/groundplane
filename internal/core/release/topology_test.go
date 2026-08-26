package release

import "testing"

func TestWorkloadTopologyKeepsRecreateOutsideSlotState(t *testing.T) {
	t.Parallel()
	target, err := TargetFor(StrategyRecreate, "")
	if err != nil || target != WorkloadSingleton || target.Slot() != "" {
		t.Fatalf("recreate target = %q, slot = %q, error = %v", target, target.Slot(), err)
	}
	if _, err := TargetFor(StrategyRecreate, SlotBlue); err == nil {
		t.Fatal("recreate accepted durable blue slot")
	}
}

func TestWorkloadComposeNamesSeparateProxyAndEveryTopology(t *testing.T) {
	t.Parallel()
	want := map[WorkloadTarget]string{
		WorkloadSingleton: "api--singleton",
		WorkloadBlue:      "api--blue",
		WorkloadGreen:     "api--green",
	}
	for target, expected := range want {
		actual, err := WorkloadComposeName("api", target)
		if err != nil || actual != expected {
			t.Fatalf("WorkloadComposeName(%q) = %q, %v", target, actual, err)
		}
	}
}

func TestBlueGreenToRecreateTransitionIsCrashReplayStable(t *testing.T) {
	t.Parallel()
	for attempt := 0; attempt < 3; attempt++ {
		transition, err := NewTopologyTransition(StrategyRecreate, "", StrategyBlueGreen, SlotBlue)
		if err != nil {
			t.Fatal(err)
		}
		if transition.CandidateTarget != WorkloadSingleton || !transition.RequiresPriorArtifact() ||
			!transition.RemovePriorBeforeApply() || transition.RemovePriorAfterSwitch() ||
			!transition.RestorePriorDuringCompensation() {
			t.Fatalf("attempt %d transition = %#v", attempt, transition)
		}
	}
}

func TestRecreateToBlueGreenTransitionIsCrashReplayStable(t *testing.T) {
	t.Parallel()
	for attempt := 0; attempt < 3; attempt++ {
		transition, err := NewTopologyTransition(StrategyBlueGreen, SlotBlue, StrategyRecreate, "")
		if err != nil {
			t.Fatal(err)
		}
		if transition.CandidateTarget != WorkloadBlue || !transition.RequiresPriorArtifact() ||
			transition.RemovePriorBeforeApply() || !transition.RemovePriorAfterSwitch() ||
			!transition.RestorePriorDuringCompensation() {
			t.Fatalf("attempt %d transition = %#v", attempt, transition)
		}
	}
}
