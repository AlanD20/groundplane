package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: retrying publication must resolve the durable idempotency marker
// before preparing sources again, including after the execution has closed.
func TestManualScriptPublicationReplayDoesNotReserveSourcesAgain(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		name := "pending"
		if terminal {
			name = "terminal"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store, sources, execution, task, marker := manualScriptLifecycleFixture(t)
			scripts := &ScriptRepository{store: store}
			published, err := scripts.PublishExecutionWithTask(ctx, sources, execution, task, marker)
			if err != nil || published.kind != idempotencyTransactionApplied {
				t.Fatalf("initial publication = %v", err)
			}
			if terminal {
				tasks, err := newTaskRepository(store)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tasks.AbortPendingTask(ctx, task.ID, task.CreatedAt.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			revision := store.revision
			replayed, err := scripts.PublishExecutionWithTask(ctx, sources, execution, task, marker)
			if err != nil || replayed.kind != idempotencyTransactionExisting || store.revision != revision {
				t.Fatalf("publication replay changed source authority: kind=%v revisions=%d/%d error=%v",
					replayed.kind, revision, store.revision, err)
			}
		})
	}
}

// Rationale: an unknown publication commit cannot be treated as a failed
// preparation. The active root retains its counts and exact replay resolves
// the committed marker without another source reservation.
func TestManualScriptPublicationUnknownCommitReplaysWithoutWrites(t *testing.T) {
	ctx := context.Background()
	store, sources, execution, task, marker := manualScriptLifecycleFixture(t)
	faults := &manualScriptPublicationLostResponseStore{memoryHierarchyStore: store, taskKey: taskKey(task.ID)}
	scripts := &ScriptRepository{store: faults}
	_, err := scripts.PublishExecutionWithTask(ctx, sources, execution, task, marker)
	if !isKind(err, errs.KindInternal) || !faults.lost {
		t.Fatalf("unknown publication outcome = %v", err)
	}
	taskValue := store.valueAt(taskKey(task.ID), store.revision)
	rootValue := store.valueAt(scriptSourceRootKey(task.OperationID), store.revision)
	if taskValue == nil || rootValue == nil || taskValue.ModRevision != rootValue.ModRevision {
		t.Fatal("unknown publication did not retain its atomic Task/root authority")
	}
	script, err := scripts.GetScript(ctx, task.Target)
	if err != nil || script.Record.ActiveReferences != 1 {
		t.Fatalf("unknown publication abandoned active source authority: %v", err)
	}
	revision := store.revision
	restarted := &ScriptRepository{store: store}
	replay, err := restarted.PublishExecutionWithTask(ctx, sources, execution, task, marker)
	if err != nil || replay.kind != idempotencyTransactionExisting || store.revision != revision {
		t.Fatalf("unknown publication replay wrote state: %v", err)
	}
}

type manualScriptPublicationLostResponseStore struct {
	*memoryHierarchyStore
	taskKey string
	lost    bool
}

func (store *manualScriptPublicationLostResponseStore) Transact(
	ctx context.Context, conditions []Condition, mutations []Mutation,
) (TransactionResult, error) {
	result, err := store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
	if err != nil || !result.Succeeded || store.lost {
		return result, err
	}
	for _, mutation := range mutations {
		if mutation.Type == MutationPut && mutation.Key == store.taskKey {
			store.lost = true
			return TransactionResult{}, errs.New(errs.KindInternal, "injected lost manual publication response")
		}
	}
	return result, nil
}
