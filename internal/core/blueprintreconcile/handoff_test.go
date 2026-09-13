package blueprintreconcile

import (
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: an accounted superseded write may be repaired by the latest input
// without restoring the predecessor or claiming the partial write was applied.
func TestSupersededHandoffRepairsForwardOnlyAfterStopAndAccounting(t *testing.T) {
	config := testUnit(testKey(ids.KindEnvEntry, 100), "working-a")
	obsolete := testUnit(config.Target, "partial-b")
	latest := testUnit(config.Target, "latest-c")
	consumer := testUnit(testKey(ids.KindService, 101), "consumer-c")
	consumer.Reads, consumer.After = []ResourceKey{config.Target}, []ResourceKey{config.Target}
	route := testUnit(testKey(ids.KindRoute, 102), "independent")
	execution := testExecution(obsolete, Draining, 103)
	input := Snapshot{Desired: []Unit{latest, consumer, route}, Applied: []AppliedUnit{
		testApplied(config), {Target: consumer.Target, State: Absent}, {Target: route.Target, State: Absent},
	}, Executions: []Execution{execution}}
	report := HandoffReport{PlanID: execution.PlanID, TaskID: execution.TaskID, Epoch: execution.Epoch,
		Executor: ExecutorUnconfirmed, Writes: []WriteEffect{{Resource: config.Target, Outcome: WriteChanged}}}
	for _, step := range []struct {
		executor ExecutorStopState
		outcome  WriteOutcome
	}{
		{ExecutorUnconfirmed, WriteChanged},
		{ExecutorStopped, WriteUnknown},
	} {
		report.Executor, report.Writes[0].Outcome = step.executor, step.outcome
		decision, err := ResolveHandoff(execution, report)
		if err != nil || decision.Settled || len(decision.ChangedWrites) != 0 {
			t.Fatalf("incomplete proof allowed handoff: %#v, %v", decision, err)
		}
		selected := mustSelect(t, input)
		assertKeys(t, "unrelated progress", selected.Ready, route.Target)
		if !slices.Contains(selected.Waiting, config.Target) || !slices.Contains(selected.Waiting, consumer.Target) {
			t.Fatal("conflicting work escaped the draining claim")
		}
	}
	report.Executor, report.Writes[0].Outcome = ExecutorStopped, WriteChanged
	decision, err := ResolveHandoff(execution, report)
	if err != nil || !decision.Settled {
		t.Fatalf("complete handoff proof: %#v, %v", decision, err)
	}
	assertKeys(t, "changed effects", decision.ChangedWrites, config.Target)
	// The persistence owner must publish this effect record and release the exact
	// claim together under the snapshot/epoch fence. No predecessor restore occurs.
	input.Executions = nil
	input.Applied[0].State, input.Applied[0].AffectedWrites = Diverged, decision.ChangedWrites
	for _, desired := range []Unit{latest, config} {
		input.Desired[0] = desired
		selected := mustSelect(t, input)
		if !slices.Contains(selected.Ready, config.Target) || len(selected.Satisfied) != 0 ||
			!slices.Contains(selected.Waiting, consumer.Target) {
			t.Fatal("accounted divergence was skipped or consumed before repair")
		}
		if input.Applied[0].Fingerprint != config.Fingerprint {
			t.Fatal("handoff discarded the last successful input")
		}
	}
	input.Desired[0], input.Applied[0] = latest, testApplied(latest)
	if !slices.Contains(mustSelect(t, input).Ready, consumer.Target) {
		t.Fatal("consumer did not become ready after the latest configuration succeeded")
	}
}

// Rationale: a proven no-effect cancellation must preserve a matching applied
// result; cancellation itself does not force an unnecessary restore or restart.
func TestSupersededHandoffPreservesUntouchedInputs(t *testing.T) {
	working := testUnit(testKey(ids.KindService, 110), "working")
	execution := testExecution(testUnit(working.Target, "obsolete"), Draining, 111)
	decision, err := ResolveHandoff(execution, HandoffReport{
		PlanID: execution.PlanID, TaskID: execution.TaskID, Epoch: execution.Epoch, Executor: ExecutorStopped,
		Writes: []WriteEffect{{Resource: working.Target, Outcome: WriteUntouched}},
	})
	if err != nil || !decision.Settled || len(decision.ChangedWrites) != 0 {
		t.Fatalf("untouched handoff: %#v, %v", decision, err)
	}
	input := Snapshot{Desired: []Unit{working}, Applied: []AppliedUnit{testApplied(working)}}
	assertKeys(t, "still applied", mustSelect(t, input).Satisfied, working.Target)
}

// Rationale: late reports, incomplete write sets and non-draining executions
// cannot release a newer claim or assert effects outside the sealed unit.
func TestHandoffRequiresExactDrainingExecutionAndCompleteWriteScope(t *testing.T) {
	unit := testUnit(testKey(ids.KindService, 120), "obsolete")
	shared := testKey(ids.KindEnvEntry, 121)
	unit.Writes = append(unit.Writes, shared)
	execution := testExecution(unit, Draining, 122)
	for _, name := range []string{"running", "epoch", "task", "plan", "missing", "duplicate", "foreign", "executor", "outcome"} {
		t.Run(name, func(t *testing.T) {
			current := execution
			report := HandoffReport{
				PlanID: execution.PlanID, TaskID: execution.TaskID, Epoch: execution.Epoch, Executor: ExecutorStopped,
				Writes: []WriteEffect{
					{Resource: unit.Target, Outcome: WriteUntouched},
					{Resource: shared, Outcome: WriteChanged},
				},
			}
			switch name {
			case "running":
				current.State = Running
			case "epoch":
				report.Epoch++
			case "task":
				report.TaskID = testKey(ids.KindTask, 123).ID
			case "plan":
				report.PlanID = testKey(ids.KindPlan, 124).ID
			case "missing":
				report.Writes = report.Writes[:1]
			case "duplicate":
				report.Writes[1].Resource = unit.Target
			case "foreign":
				report.Writes[1].Resource = testKey(ids.KindEnvEntry, 125)
			case "executor":
				report.Executor = ""
			case "outcome":
				report.Writes[0].Outcome = ""
			}
			decision, err := ResolveHandoff(current, report)
			if err == nil || decision.Settled || len(decision.ChangedWrites) != 0 {
				t.Fatalf("invalid %s report produced a handoff: %#v, %v", name, decision, err)
			}
		})
	}
	report := HandoffReport{
		PlanID: execution.PlanID, TaskID: execution.TaskID, Epoch: execution.Epoch, Executor: ExecutorStopped,
		Writes: []WriteEffect{
			{Resource: unit.Target, Outcome: WriteUntouched},
			{Resource: shared, Outcome: WriteChanged},
		},
	}
	writesBefore := slices.Clone(report.Writes)
	scopeBefore := slices.Clone(execution.Unit.Writes)
	decision, err := ResolveHandoff(execution, report)
	if err != nil || !decision.Settled {
		t.Fatalf("complete unordered report: %#v, %v", decision, err)
	}
	assertKeys(t, "changed shared input", decision.ChangedWrites, shared)
	if !slices.Equal(report.Writes, writesBefore) || !slices.Equal(execution.Unit.Writes, scopeBefore) {
		t.Fatal("handoff mutated its caller's immutable execution or report")
	}
}

// Rationale: probing unknown effects is independent of desired execution order;
// waiting for a new prerequisite before accounting old writes can deadlock repair.
func TestUnknownEffectAccountingDoesNotWaitForDesiredPrerequisites(t *testing.T) {
	prerequisite := testUnit(testKey(ids.KindEnvEntry, 130), "new")
	unit := testUnit(testKey(ids.KindService, 131), "latest")
	unit.After = []ResourceKey{prerequisite.Target}
	got := mustSelect(t, Snapshot{Desired: []Unit{unit, prerequisite}, Applied: []AppliedUnit{
		{Target: unit.Target, State: Uncertain, AffectedWrites: []ResourceKey{unit.Target, prerequisite.Target}},
		{Target: prerequisite.Target, State: Absent},
	}})
	assertKeys(t, "accounting", got.ResolveEffects, unit.Target)
	assertKeys(t, "conflicting prerequisite", got.Waiting, prerequisite.Target)
}

// Rationale: repairing a logical owner alone cannot clear another physical
// resource's divergence or admit consumers of that still-unrepaired resource.
func TestAccountedSharedWritesRemainSeparateUntilTheirRepairSucceeds(t *testing.T) {
	shared := testKey(ids.KindEnvEntry, 140)
	writer := testUnit(testKey(ids.KindService, 141), "writer")
	reader := testUnit(testKey(ids.KindService, 142), "reader")
	writer.Writes = append(writer.Writes, shared)
	reader.Reads = []ResourceKey{shared}
	input := Snapshot{Desired: []Unit{reader, writer}, Applied: []AppliedUnit{
		testApplied(
			writer,
		), testApplied(reader), {Target: shared, State: Diverged, AffectedWrites: []ResourceKey{shared}},
	}}
	got := mustSelect(t, input)
	assertKeys(t, "repair writer", got.Ready, writer.Target)
	assertKeys(t, "blocked reader", got.Waiting, reader.Target)
	if len(got.Satisfied) != 0 {
		t.Fatal("matching historical inputs hid a diverged shared file")
	}
	input.Applied[2] = testApplied(testUnit(shared, "repaired"))
	got = mustSelect(t, input)
	if len(got.Ready)+len(got.Waiting) != 0 || len(got.Satisfied) != 2 {
		t.Fatal("accounted repair did not preserve already-correct consumers")
	}
	input.Applied[2] = AppliedUnit{
		Target:         shared,
		State:          Diverged,
		AffectedWrites: []ResourceKey{shared, writer.Target},
	}
	if _, err := Select(input); err == nil {
		t.Fatal("multiple diverged resources could be cleared through one owner")
	}
}
