package scriptsourcereference

import (
	"context"
	"fmt"
	"testing"
)

// Rationale: process memory is unavailable after a crash. Recovery must use
// the descriptor's exact committed cursor, including an unsealed prefix,
// without reconstructing or refreshing the caller's unreserved source list.
func TestRecoverPreparationsReleasesOnlyCommittedPrefix(t *testing.T) {
	ctx := context.Background()
	store := newReleaseTestStore()
	repository, err := NewRepository(store, releaseTestScriptCodec{})
	if err != nil {
		t.Fatal(err)
	}
	operationID := "op_crashed_preparation"
	members, digest, _, err := canonicalMembers(operationID, releaseServiceMembers(store, operationID, 35))
	if err != nil {
		t.Fatal(err)
	}
	descriptor, revision, err := repository.ensurePreparation(ctx, Preparation{
		OperationID: operationID, MembershipCount: uint64(len(members)), MembershipSHA256: digest,
		Phase: PreparationPreparing,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = repository.prepareBatch(ctx, descriptor, revision, members[:preparationBatchSize]); err != nil {
		t.Fatal(err)
	}
	// An unrelated visible operation retains its memberships throughout recovery.
	active := releaseServiceMembers(store, "op_visible", 2)
	activateReleaseMembers(t, ctx, store, repository, "op_visible", active)
	restarted, err := NewRepository(store, releaseTestScriptCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.RecoverPreparations(ctx); err != nil {
		t.Fatal(err)
	}
	if store.values[PreparationKey(operationID)] != nil {
		t.Fatal("orphan descriptor retained")
	}
	activeSources := make(map[string]bool, len(active))
	for _, member := range active {
		activeSources[CountKey(member.Reference.Source)] = true
	}
	for _, member := range members {
		if store.values[ForwardKey(member.Reference)] != nil || store.values[ReverseKey(member.Reference)] != nil {
			t.Fatal("orphan membership retained")
		}
		countKey := CountKey(member.Reference.Source)
		value := store.values[countKey]
		if !activeSources[countKey] {
			if value != nil {
				t.Fatal("orphan-only source count retained")
			}
			continue
		}
		if value == nil {
			t.Fatal("published operation lost shared source count")
		}
		count, err := decodeCount(value.Value)
		if err != nil || count.ReferencedExecutionCount != 1 {
			t.Fatalf("shared source count = %#v, %v", count, err)
		}
	}
	for _, member := range active {
		if store.values[ForwardKey(member.Reference)] == nil || store.values[ReverseKey(member.Reference)] == nil {
			t.Fatal("recovery removed a published operation's membership")
		}
	}
	if store.values[RootKey("op_visible")] == nil {
		t.Fatal("recovery removed active root")
	}
	revision = store.revision
	if err := restarted.RecoverPreparations(ctx); err != nil || store.revision != revision {
		t.Fatalf("recovery replay wrote state: %v", err)
	}
}

// Rationale: sealed and already-abandoning descriptors are equally private;
// a lost committed response must resume without a second count decrement.
func TestRecoverPreparationsResumesUnknownCommittedAbandonment(t *testing.T) {
	for _, failAt := range []int{1, 2, 4, 6} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			ctx := context.Background()
			store := newReleaseTestStore()
			repository, err := NewRepository(store, releaseTestScriptCodec{})
			if err != nil {
				t.Fatal(err)
			}
			members := releaseServiceMembers(store, "op_crash_abandon", 35)
			if _, err := repository.Prepare(ctx, "op_crash_abandon", members); err != nil {
				t.Fatal(err)
			}
			interrupted, err := NewRepository(
				&preparationUnknownCommitStore{releaseTestStore: store, failAt: failAt},
				releaseTestScriptCodec{},
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := interrupted.RecoverPreparations(ctx); err == nil {
				t.Fatal("lost commit response did not fail")
			}
			restarted, err := NewRepository(store, releaseTestScriptCodec{})
			if err != nil {
				t.Fatal(err)
			}
			if err := restarted.RecoverPreparations(ctx); err != nil {
				t.Fatal(err)
			}
			assertReleaseMembersAbsent(t, store, members)
			if store.values[PreparationKey("op_crash_abandon")] != nil {
				t.Fatal("abandonment descriptor retained")
			}
			if store.maximumRangeLimit > normalReleaseWindowSize || store.maximumTransactionOperations > 96 {
				t.Fatalf(
					"recovery exceeded physical bounds: range=%d operations=%d",
					store.maximumRangeLimit,
					store.maximumTransactionOperations,
				)
			}
		})
	}
}

type preparationUnknownCommitStore struct {
	*releaseTestStore
	failAt int
	calls  int
}

func (store *preparationUnknownCommitStore) Transact(
	ctx context.Context, conditions []Condition, mutations []Mutation,
) (TransactionResult, error) {
	store.calls++
	if store.calls == store.failAt {
		store.commitThenError = true
	}
	return store.releaseTestStore.Transact(ctx, conditions, mutations)
}

// Rationale: durable corruption is not cleanup permission, even when symmetric
// memberships exist. Recovery must leave counts untouched on invalid cursors
// or a root that makes the operation visible.
func TestRecoverPreparationsRejectsCorruptOrPublishedAuthority(t *testing.T) {
	for _, change := range []string{"cursor", "phase", "digest", "root"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			store := newReleaseTestStore()
			repository, err := NewRepository(store, releaseTestScriptCodec{})
			if err != nil {
				t.Fatal(err)
			}
			operationID := "op_corrupt_preparation"
			members := releaseServiceMembers(store, operationID, 2)
			if _, err := repository.Prepare(ctx, operationID, members); err != nil {
				t.Fatal(err)
			}
			value := store.values[PreparationKey(operationID)]
			descriptor, err := decodePreparation(value.Value)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "cursor":
				descriptor.ReleaseCursor = descriptor.PreparationCursor + 1
			case "phase":
				descriptor.Phase = "unknown"
			case "digest":
				descriptor.MembershipSHA256 = "invalid"
			case "root":
				store.put(RootKey(operationID), []byte("occupied root"))
			}
			encoded, err := encodePreparation(descriptor)
			if err != nil {
				t.Fatal(err)
			}
			store.put(value.Key, encoded)
			revision := store.revision
			if err := repository.RecoverPreparations(ctx); err == nil || store.revision != revision {
				t.Fatalf("corrupt recovery changed state: %v", err)
			}
		})
	}
}
