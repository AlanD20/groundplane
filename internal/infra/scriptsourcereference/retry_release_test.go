package scriptsourcereference

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// Rationale: expiry shares physical batch limits with normal completion, but
// retains terminal Task/index bytes and resumes only its recorded release path
// after an unknown committed batch outcome.
func TestRetryExpiryDrainsBoundedPagesAcrossUnknownCommit(t *testing.T) {
	ctx := context.Background()
	store := newReleaseTestStore()
	repository, err := NewRepository(store, releaseTestScriptCodec{})
	if err != nil {
		t.Fatal(err)
	}
	operationID := "op_retry_expiry_restart"
	members := releaseServiceMembers(store, operationID, 35)
	activateReleaseMembers(t, ctx, store, repository, operationID, members)
	task := store.put("/tasks/terminal", []byte("original terminal Task"))
	retention := store.put("/retention/terminal", []byte("original retention index"))
	guards := []Condition{
		{Key: task.Key, ModRevision: task.ModRevision},
		{Key: retention.Key, ModRevision: retention.ModRevision},
	}
	deadline := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	available, err := repository.PrepareRetryAvailable(
		ctx,
		operationID,
		store.values[RootKey(operationID)].ModRevision,
		deadline,
	)
	if err != nil {
		t.Fatal(err)
	}
	applyRetryReleaseFragment(t, ctx, store, guards, available)
	expiry, err := repository.PrepareRetryExpiry(
		ctx,
		operationID,
		store.values[RootKey(operationID)].ModRevision,
		deadline,
	)
	if err != nil {
		t.Fatal(err)
	}
	applyRetryReleaseFragment(t, ctx, store, guards, expiry)
	store.commitThenError = true
	if _, _, err := repository.ReleaseRetryExpiryNext(ctx, operationID, guards); err == nil {
		t.Fatal("expiry batch did not expose its unknown commit outcome")
	}
	restarted, err := NewRepository(store, releaseTestScriptCodec{})
	if err != nil {
		t.Fatal(err)
	}
	for {
		processed, drained, err := restarted.ReleaseRetryExpiryNext(ctx, operationID, guards)
		if err != nil {
			t.Fatal(err)
		}
		if drained {
			break
		}
		if !processed {
			t.Fatal("expiry made no progress before drain")
		}
	}
	root, err := decodeRoot(store.values[RootKey(operationID)].Value)
	if err != nil || root.ReleasePath != sourceReleasePathRetryExpiry ||
		root.RetryDisposition != RetryDispositionExpired ||
		root.RetryExpiresAt == nil ||
		!root.RetryExpiresAt.Equal(deadline) {
		t.Fatalf("expiry restart changed its recorded path/deadline: %v", err)
	}
	wrong, err := restarted.PrepareReleaseFinalization(ctx, operationID)
	wrong.Clear()
	if err == nil {
		t.Fatal("normal finalization accepted retry expiry")
	}
	final, err := restarted.PrepareRetryExpiryFinalization(ctx, operationID)
	if err != nil {
		t.Fatal(err)
	}
	applyRetryReleaseFragment(t, ctx, store, guards, final)
	assertReleaseMembersAbsent(t, store, members)
	if store.values[RootKey(operationID)] != nil || store.maximumRangeLimit != normalReleaseWindowSize ||
		store.maximumTransactionOperations > releaseTransactionOperationLimit {
		t.Fatal("expiry did not finalize within the ordinary physical bounds")
	}
	for _, original := range []*KeyValue{task, retention} {
		current := store.values[original.Key]
		if current == nil || current.ModRevision != original.ModRevision ||
			!bytes.Equal(current.Value, original.Value) {
			t.Fatal("expiry changed terminal Task or retention authority")
		}
	}
}

func applyRetryReleaseFragment(
	t *testing.T,
	ctx context.Context,
	store *releaseTestStore,
	guards []Condition,
	fragment ReleaseFragment,
) {
	t.Helper()
	defer fragment.Clear()
	result, err := store.Transact(
		ctx,
		append(append([]Condition(nil), guards...), fragment.Conditions...),
		fragment.Mutations,
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("commit retry release fragment: %v", err)
	}
}
