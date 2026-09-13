package blueprintreconcile

import (
	"crypto/sha256"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: newer input must skip queued B, drain running A, and preserve
// independent routing work instead of cancelling the whole Environment.
func TestLatestInputSkipsPendingWorkAndWaitsForConflictingExecution(t *testing.T) {
	api, route := testKey(ids.KindService, 1), testKey(ids.KindRoute, 2)
	a, b, c := testUnit(api, "a"), testUnit(api, "b"), testUnit(api, "c")
	routing := testUnit(route, "routing")
	running, pending := testExecution(a, Running, 3), testExecution(b, Pending, 4)
	input := Snapshot{
		Desired:    []Unit{c, routing},
		Applied:    []AppliedUnit{testApplied(a), {Target: route, State: Absent}},
		Executions: []Execution{running, pending},
	}
	got := mustSelect(t, input)
	assertKeys(t, "ready", got.Ready, route)
	assertKeys(t, "waiting", got.Waiting, api)
	assertIDs(t, "pending cancellation", got.CancelPending, pending.PlanID)
	assertIDs(t, "running cancellation", got.CancelRunning, running.PlanID)

	input.Executions = nil // Durable cancellation/cleanup has now proved A stopped.
	got = mustSelect(t, input)
	if !slices.Contains(got.Ready, api) {
		t.Fatal("latest API input did not become eligible after handoff")
	}
}

// Rationale: rejecting invalid input must not produce cancellation instructions.
func TestInvalidLatestInputCannotCancelRunningWork(t *testing.T) {
	api := testKey(ids.KindService, 10)
	unit := testUnit(api, "working")
	invalid := unit
	invalid.Fingerprint = Fingerprint{}
	got, err := Select(Snapshot{
		Desired: []Unit{invalid}, Applied: []AppliedUnit{testApplied(unit)},
		Executions: []Execution{testExecution(unit, Running, 11)},
	})
	if err == nil || len(got.CancelRunning) != 0 || len(got.CancelPending) != 0 {
		t.Fatal("invalid input produced a selection or cancellation")
	}
}

// Rationale: changing the latest Apply identity does not obsolete equal effective
// inputs; cancellation that already began must not be reversed, however.
func TestMatchingRunningWorkContinuesButDrainingWorkCannotRestart(t *testing.T) {
	unit := testUnit(testKey(ids.KindService, 20), "same")
	execution := testExecution(unit, Running, 21)
	input := Snapshot{
		Desired:    []Unit{unit},
		Applied:    []AppliedUnit{testApplied(unit)},
		Executions: []Execution{execution},
	}
	got := mustSelect(t, input)
	assertIDs(t, "continuing", got.ContinueRunning, execution.PlanID)
	if len(got.Ready)+len(got.CancelRunning)+len(got.Satisfied) != 0 {
		t.Fatal("still-required execution was replaced or prematurely acknowledged")
	}
	input.Executions[0].State = Draining
	got = mustSelect(t, input)
	assertKeys(t, "waiting", got.Waiting, unit.Target)
	if len(got.Ready)+len(got.ContinueRunning)+len(got.CancelRunning) != 0 {
		t.Fatal("draining execution was revived or cancelled repeatedly")
	}
}

// Rationale: equal desired input after a failed or uncertain attempt is not
// successful application; missing authority is not inferred to mean absence.
func TestSelectionUsesAppliedResultsAndRequiresKnownEffects(t *testing.T) {
	unit := testUnit(testKey(ids.KindService, 30), "requested")
	input := Snapshot{Desired: []Unit{unit}, Applied: []AppliedUnit{{Target: unit.Target, State: Absent}}}
	assertKeys(t, "unapplied", mustSelect(t, input).Ready, unit.Target)
	input.Applied[0] = testApplied(unit)
	assertKeys(t, "applied", mustSelect(t, input).Satisfied, unit.Target)
	input.Applied[0] = AppliedUnit{Target: unit.Target, State: Uncertain, UncertainWrites: unit.Writes}
	got := mustSelect(t, input)
	assertKeys(t, "unknown effects", got.ResolveEffects, unit.Target)
	if len(got.Ready)+len(got.Satisfied) != 0 {
		t.Fatal("unknown effects were treated as applied or absent")
	}
	input.Applied = nil
	if _, err := Select(input); err == nil {
		t.Fatal("missing applied authority was treated as first application")
	}
}

// Rationale: unresolved shared-file effects fence every consumer, including one
// ordered before the uncertain owner and one whose own inputs otherwise match.
func TestUncertainEffectsBlockSharedResourcesButNotUnrelatedWork(t *testing.T) {
	shared := testKey(ids.KindEnvEntry, 90)
	reader := testUnit(testKey(ids.KindService, 91), "reader")
	writer := testUnit(testKey(ids.KindService, 92), "writer")
	owner := testUnit(testKey(ids.KindService, 93), "latest-owner")
	route := testUnit(testKey(ids.KindRoute, 94), "route")
	reader.Reads = []ResourceKey{shared}
	writer.Writes = append(writer.Writes, shared)
	// The latest owner no longer touches shared. Its earlier effects still do.
	input := Snapshot{Desired: []Unit{reader, writer, owner, route}, Applied: []AppliedUnit{
		testApplied(reader), {Target: writer.Target, State: Absent},
		{Target: owner.Target, State: Uncertain, UncertainWrites: []ResourceKey{owner.Target, shared}},
		{Target: route.Target, State: Absent},
	}}
	got := mustSelect(t, input)
	assertKeys(t, "independent work", got.Ready, route.Target)
	assertKeys(t, "uncertain owner", got.ResolveEffects, owner.Target)
	if len(got.Satisfied) != 0 || len(got.Waiting) != 2 ||
		!slices.Contains(got.Waiting, reader.Target) || !slices.Contains(got.Waiting, writer.Target) {
		t.Fatal("unknown shared effects did not fence readers and writers")
	}
	input.Applied[2].UncertainWrites = nil
	if _, err := Select(input); err == nil {
		t.Fatal("unknown effects without an explicit resource scope were accepted")
	}
}

// Rationale: an old in-flight mutation can invalidate an otherwise matching
// applied fingerprint; no-op selection must wait for that mutation to settle.
func TestObsoleteMutationBlocksMatchingAppliedInput(t *testing.T) {
	unit := testUnit(testKey(ids.KindService, 40), "current")
	old := testExecution(testUnit(unit.Target, "obsolete"), Running, 41)
	got := mustSelect(
		t,
		Snapshot{Desired: []Unit{unit}, Applied: []AppliedUnit{testApplied(unit)}, Executions: []Execution{old}},
	)
	assertKeys(t, "waiting", got.Waiting, unit.Target)
	if len(got.Satisfied) != 0 {
		t.Fatal("in-flight conflicting effects were ignored")
	}
}

// Rationale: shared reads do not conflict, but changing shared configuration
// must wait for readers and block new consumers until its result is applied.
func TestResourceAccessAndDependencyReadiness(t *testing.T) {
	entry := testUnit(testKey(ids.KindEnvEntry, 50), "new-value")
	api := testUnit(testKey(ids.KindService, 51), "api")
	worker := testUnit(testKey(ids.KindService, 52), "worker")
	api.Reads, worker.Reads = []ResourceKey{entry.Target}, []ResourceKey{entry.Target}
	api.After, worker.After = []ResourceKey{entry.Target}, []ResourceKey{entry.Target}
	input := Snapshot{Desired: []Unit{entry, api, worker}, Applied: []AppliedUnit{
		{
			Target: entry.Target,
			State:  Absent,
		}, {Target: api.Target, State: Absent}, {Target: worker.Target, State: Absent},
	}}
	got := mustSelect(t, input)
	assertKeys(t, "entry first", got.Ready, entry.Target)
	if !slices.Contains(got.Waiting, api.Target) || !slices.Contains(got.Waiting, worker.Target) {
		t.Fatal("consumers ran before configuration was applied")
	}
	input.Applied[0] = testApplied(entry)
	got = mustSelect(t, input)
	if len(got.Ready) != 2 || !slices.Contains(got.Ready, api.Target) || !slices.Contains(got.Ready, worker.Target) {
		t.Fatal("read-only sharing unnecessarily serialized consumers")
	}
	input.Executions = []Execution{testExecution(api, Running, 53)}
	input.Applied[0] = AppliedUnit{Target: entry.Target, State: Absent}
	got = mustSelect(t, input)
	if slices.Contains(got.Ready, entry.Target) {
		t.Fatal("shared configuration write overlapped a running reader")
	}
}

// Rationale: concurrent selected writes must be serialized even when their
// different logical targets would otherwise appear independent.
func TestReadySelectionReservesSharedWrites(t *testing.T) {
	shared := testKey(ids.KindEnvEntry, 60)
	first := testUnit(testKey(ids.KindService, 61), "first")
	second := testUnit(testKey(ids.KindService, 62), "second")
	first.Writes = append(first.Writes, shared)
	second.Writes = append(second.Writes, shared)
	input := Snapshot{Desired: []Unit{second, first}, Applied: []AppliedUnit{
		{Target: first.Target, State: Absent}, {Target: second.Target, State: Absent},
	}}
	got := mustSelect(t, input)
	if len(got.Ready) != 1 || len(got.Waiting) != 1 || got.Ready[0] == got.Waiting[0] {
		t.Fatal("shared writes were not serialized")
	}
	slices.Reverse(input.Desired)
	again := mustSelect(t, input)
	if !slices.Equal(got.Ready, again.Ready) || !slices.Equal(got.Waiting, again.Waiting) {
		t.Fatal("selection depends on caller input order")
	}
}

// Rationale: access-set order is not a configuration change, but replacing an
// accessed resource must obsolete the old plan even if a caller reuses its hash.
func TestScopeComparisonIsOrderIndependentAndDoesNotMutateInputs(t *testing.T) {
	unit := testUnit(testKey(ids.KindService, 80), "same")
	unit.Reads = []ResourceKey{testKey(ids.KindEnvEntry, 81), testKey(ids.KindNetwork, 82)}
	execution := testExecution(unit, Running, 83)
	execution.Unit.Reads = slices.Clone(unit.Reads)
	slices.Reverse(execution.Unit.Reads)
	before := slices.Clone(execution.Unit.Reads)
	input := Snapshot{
		Desired:    []Unit{unit},
		Applied:    []AppliedUnit{testApplied(unit)},
		Executions: []Execution{execution},
	}
	got := mustSelect(t, input)
	assertIDs(t, "same access set", got.ContinueRunning, execution.PlanID)
	if !slices.Equal(input.Executions[0].Unit.Reads, before) {
		t.Fatal("selection mutated its caller's immutable source")
	}
	input.Desired[0].Reads = []ResourceKey{testKey(ids.KindEnvEntry, 84)}
	got = mustSelect(t, input)
	assertIDs(t, "changed access set", got.CancelRunning, execution.PlanID)
}

// Rationale: malformed ownership and cyclic dependencies must fail closed
// before producing effects or cancellation requests.
func TestRejectsAmbiguousOrIncompleteSnapshot(t *testing.T) {
	api := testUnit(testKey(ids.KindService, 70), "api")
	worker := testUnit(testKey(ids.KindService, 71), "worker")
	for _, name := range []string{"cycle", "missing dependency", "duplicate target", "duplicate plan", "conflicting owners", "invalid state"} {
		t.Run(name, func(t *testing.T) {
			input := Snapshot{
				Desired: []Unit{api, worker},
				Applied: []AppliedUnit{testApplied(api), testApplied(worker)},
			}
			switch name {
			case "cycle":
				input.Desired[0].After, input.Desired[1].After = []ResourceKey{worker.Target}, []ResourceKey{api.Target}
			case "missing dependency":
				input.Desired[0].After = []ResourceKey{testKey(ids.KindEnvEntry, 72)}
			case "duplicate target":
				input.Desired = append(input.Desired, api)
			case "duplicate plan":
				execution := testExecution(api, Pending, 73)
				input.Executions = []Execution{execution, execution}
			case "conflicting owners":
				input.Executions = []Execution{testExecution(api, Running, 74), testExecution(api, Draining, 75)}
			case "invalid state":
				input.Applied[0].State = ""
			}
			if _, err := Select(input); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
}

func testKey(kind ids.Kind, seed int64) ResourceKey {
	return ResourceKey{Kind: kind, ID: ids.NewAt(kind, time.Unix(1_700_000_000, 0), seed)}
}

func testUnit(target ResourceKey, value string) Unit {
	return Unit{Target: target, Fingerprint: Fingerprint(sha256.Sum256([]byte(value))), Writes: []ResourceKey{target}}
}

func testApplied(unit Unit) AppliedUnit {
	return AppliedUnit{Target: unit.Target, State: Applied, Fingerprint: unit.Fingerprint}
}

func testExecution(unit Unit, state ExecutionState, seed int64) Execution {
	return Execution{
		PlanID: testKey(ids.KindPlan, seed).ID,
		TaskID: testKey(ids.KindTask, seed).ID,
		Unit:   unit,
		State:  state,
	}
}

func mustSelect(t *testing.T, snapshot Snapshot) Selection {
	t.Helper()
	selection, err := Select(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return selection
}

func assertKeys(t *testing.T, label string, actual []ResourceKey, expected ...ResourceKey) {
	t.Helper()
	if !slices.Equal(actual, expected) {
		t.Fatalf("%s: got %v, want %v", label, actual, expected)
	}
}

func assertIDs(t *testing.T, label string, actual []string, expected ...string) {
	t.Helper()
	if !slices.Equal(actual, expected) {
		t.Fatalf("%s: got %v, want %v", label, actual, expected)
	}
}
