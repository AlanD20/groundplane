package etcd

import (
	"context"
	"testing"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a crashed publication cannot run its deferred abandonment. Startup
// must release the actual Script primary count from durable preparation state,
// without the original request's source list or a fabricated terminal Task.
func TestManualScriptCrashedPreparationRecoveryReleasesReferences(t *testing.T) {
	ctx := context.Background()
	store, sources, execution, task, marker := manualScriptLifecycleFixture(t)
	crashed := &manualPreparationCrashStore{memoryHierarchyStore: store, failAt: 4}
	_, err := (composeScriptRepository(crashed)).PublishExecutionWithTask(ctx, sources, execution, task, marker)
	if err == nil {
		t.Fatal("simulated crash did not interrupt publication")
	}
	if store.valueAt(testtaskjournal.TaskStorageKey(task.ID), store.revision) != nil ||
		store.valueAt(testscriptsourceevidence.ScriptSourceRootKey(task.OperationID), store.revision) != nil {
		t.Fatal("crashed preparation published visible authority")
	}
	prepared := store.valueAt(testscriptsourcereference.PreparationKey(task.OperationID), store.revision)
	if prepared == nil {
		t.Fatal("crash did not retain preparation evidence")
	}
	scripts := composeScriptRepository(store)
	before, err := scripts.GetScript(ctx, execution.ScriptID)
	if err != nil || before.Record.ActiveReferences != 1 {
		t.Fatalf("crash body reference = %v", err)
	}
	restarted, err := testscriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.RecoverPreparations(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := scripts.GetScript(ctx, execution.ScriptID)
	if err != nil || after.Record.ActiveReferences != 0 {
		t.Fatalf("recovery retained body reference: %v", err)
	}
	if store.valueAt(testscriptsourcereference.PreparationKey(task.OperationID), store.revision) != nil ||
		store.valueAt(testtaskjournal.TaskStorageKey(task.ID), store.revision) != nil {
		t.Fatal("recovery retained private descriptor or manufactured a Task")
	}
	revision := store.revision
	if err := restarted.RecoverPreparations(ctx); err != nil || store.revision != revision {
		t.Fatalf("recovery replay wrote state: %v", err)
	}
}

type manualPreparationCrashStore struct {
	*memoryHierarchyStore
	failAt int
	calls  int
}

func (store *manualPreparationCrashStore) Transact(
	ctx context.Context, conditions []testkeyvalue.Condition, mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.calls++
	if store.calls >= store.failAt {
		return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal, "simulated process loss")
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
