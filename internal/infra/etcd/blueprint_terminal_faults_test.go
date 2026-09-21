package etcd

import (
	"context"
	"testing"

	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintTerminalFaultStore struct {
	*memoryHierarchyStore
	t                 *testing.T
	epochKey          string
	calls             int
	committedRevision int64
	fault             string
	raceRevision      int64
}

func (store *blueprintTerminalFaultStore) TransactBlueprintTaskTerminal(
	ctx context.Context, envelope BlueprintTaskTerminalTransaction,
) (testkeyvalue.TransactionResult, error) {
	store.calls++
	conditions, mutations, err := envelope.Operations()
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	defer testkeyvalue.ClearMutationValues(mutations)
	if store.calls != 1 {
		store.t.Fatal("terminal persistence repeated after authority loss or uncertain commit")
	}
	if store.fault == "compare-loss" {
		value := store.valueAt(store.epochKey, store.revision)
		if value == nil {
			store.t.Fatal("terminal fault fixture has no Environment epoch")
		}
		// Same bytes, new ModRevision: force the actual old compare to fail.
		if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: store.epochKey, Value: value.Value}}); err != nil {
			store.t.Fatal(err)
		}
		before := store.revision
		store.raceRevision = before
		result, err := store.Transact(ctx, conditions, mutations)
		if err != nil || result.Succeeded || store.revision != before {
			store.t.Fatalf("lost terminal compare performed partial writes: %v", err)
		}
		return result, err
	}
	result, err := store.Transact(ctx, conditions, mutations)
	if err != nil || !result.Succeeded {
		store.t.Fatalf("terminal composition did not commit: %v", err)
	}
	store.committedRevision = result.Revision
	for _, mutation := range mutations {
		if mutation.Prefix {
			continue // Prefix semantics are covered by the production store tests.
		}
		value := store.valueAt(mutation.Key, store.revision)
		if mutation.Type == testkeyvalue.MutationPut && (value == nil || value.ModRevision != result.Revision) ||
			mutation.Type == testkeyvalue.MutationDelete && value != nil {
			store.t.Fatalf("terminal mutation did not share the atomic commit: %s", mutation.Key)
		}
	}
	return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal, "injected lost terminal commit response")
}

func (fixture *ExecutedArtifactFixture) proveClosingTerminalFaults(t *testing.T, current TaskAssignment) {
	t.Helper()
	ctx := context.Background()
	report, value, err := fixture.Tasks.readScriptClosingReport(ctx, current)
	if err != nil || value == nil {
		t.Fatalf("fault proof requires an original closing report: %v", err)
	}
	faults := &blueprintTerminalFaultStore{memoryHierarchyStore: fixture.store.memoryHierarchyStore,
		t: t, epochKey: testhierarchy.EnvironmentMutationEpochKey(current.Task.Record.Owner.EnvironmentID), fault: fixture.TerminalCommitFault}
	repository, err := newTaskRepository(faults)
	if err != nil {
		t.Fatal(err)
	}
	repository.blueprintTerminalStore = faults
	_, reconnectErr := repository.ReconnectAgentAssignment(ctx, current)
	if fixture.TerminalCommitFault == "compare-loss" {
		after, err := fixture.Tasks.GetTaskAssignment(ctx, current.Task.Record.ID)
		if !isKind(reconnectErr, errs.KindStateConflict) || err != nil || faults.calls != 1 ||
			faults.raceRevision == 0 || fixture.store.revision != faults.raceRevision ||
			after.Task.Revision != current.Task.Revision || after.Assignment.Revision != current.Assignment.Revision {
			t.Fatalf("terminal compare loss changed Task/assignment authority: reconnect=%v read=%v", reconnectErr, err)
		}
		retained, retainedValue, err := fixture.Tasks.readScriptClosingReport(ctx, after)
		if err != nil || retainedValue == nil || retainedValue.ModRevision != value.ModRevision ||
			!retained.matches(report.Status, report.Result) {
			t.Fatalf("terminal compare loss discarded original continuation: %v", err)
		}
		return
	}
	if !isKind(reconnectErr, errs.KindInternal) || faults.calls != 1 || faults.committedRevision == 0 {
		t.Fatalf("expected uncertain commit: calls=%d error=%v", faults.calls, reconnectErr)
	}
	committed, err := fixture.Tasks.GetTask(ctx, current.Task.Record.ID)
	if err != nil || committed.Revision != faults.committedRevision || committed.Record.Status != report.Status {
		t.Fatalf("uncertain commit did not leave the complete terminal Task: %v", err)
	}
	restarted, err := newTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	restarted.blueprintTerminalStore = fixture.store
	before := fixture.store.revision
	replayed, err := restarted.AcknowledgeTask(ctx, report.AgentID, report.AgentGeneration,
		report.TaskID, report.AssignmentID, report.Status, report.Result, report.ObservedAt)
	if err != nil || replayed.Revision != committed.Revision || fixture.store.revision != before {
		t.Fatalf("uncertain terminal replay was not exact read-only success: %v", err)
	}
	if replayed.Record.FinishedAt == nil || !replayed.Record.FinishedAt.Equal(report.ObservedAt) {
		t.Fatal("uncertain terminal replay lost the original completion time")
	}
	for _, key := range []string{testscriptsourceevidence.ScriptSourceRootKey(current.Task.Record.OperationID), blueprintClosingReportKey(report.TaskID)} {
		if fixture.store.valueAt(key, fixture.store.revision) != nil {
			t.Fatalf("uncertain commit retained terminal continuation: %s", key)
		}
	}
}
