package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: SEC-07 requires every Secret deletion path to reject an exact
// recovery pin, preserving the predecessor configuration needed by SVC-15 and
// JOURNEY-02 without copying ciphertext into the membership record.
func TestSecretRecoveryPinBlocksDeletionAdmissionDirectFinalizationAndHierarchy(t *testing.T) {
	ctx := context.Background()
	store, project, secrets, current, value := secretRecoveryPinFixture(t)
	operationID := ids.NewAt(
		ids.KindOperation,
		current.Record.Secret.UpdatedAt.Add(time.Second),
		301,
	)
	seedSecretRecoveryPin(t, store, tasksecretpinrecord.Record{
		OperationID: operationID, SecretID: current.Record.Secret.ID,
		MetadataRevision: current.Revision, CiphertextSHA256: value.CiphertextSHA256,
	})

	task, marker, tombstone := secretDeletionTestTask(
		t, current, project, current.Record.Secret.UpdatedAt.Add(2*time.Second), 302,
	)
	admission, err := newSecretRepository(&connectorReferenceRaceStore{
		memoryHierarchyStore: store, injected: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := store.revision
	result, err := admission.BeginSecretDeletionWithTask(
		ctx, ProjectSecretOwner(project), current, tombstone, task, marker,
	)
	if err != nil || !isKind(result.conflict, errs.KindResourceInUse) || store.revision != before {
		t.Fatalf("pinned Secret deletion admission = %#v/%v", result, err)
	}
	if store.valueAt(taskKey(task.ID), store.revision) != nil {
		t.Fatal("rejected pinned Secret deletion published a Task")
	}
	if _, err := secrets.DeleteSecret(ctx, ProjectSecretOwner(project), current); !isKind(
		err,
		errs.KindResourceInUse,
	) || store.revision != before {
		t.Fatalf("pinned Secret direct finalization = %v", err)
	}
	hierarchy, err := newHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hierarchy.prepareHierarchyDeletionSecretFinalizer(ctx, HierarchyDeletionAction{
		TargetID: current.Record.Secret.ID, TargetRevision: current.Revision,
	}); !isKind(err, errs.KindResourceInUse) || store.revision != before {
		t.Fatalf("pinned Secret hierarchy finalization = %v", err)
	}
}

// Rationale: SEC-07 requires Retry to reacquire the same deletion fence and
// lose to a recovery pin created while a failed deletion is visible; this keeps
// the exact SVC-15/JOURNEY-02 predecessor source available for recovery.
func TestSecretRecoveryPinBlocksDeletionRetry(t *testing.T) {
	ctx := context.Background()
	store, project, secrets, current, value := secretRecoveryPinFixture(t)
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	at := current.Record.Secret.UpdatedAt.Add(time.Second)
	task, marker, tombstone := secretDeletionTestTask(t, current, project, at, 311)
	result, err := secrets.BeginSecretDeletionWithTask(
		ctx, ProjectSecretOwner(project), current, tombstone, task, marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("unpinned initial Secret deletion = %#v/%v", result, err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, at.Add(time.Second)); err != nil ||
		!found {
		t.Fatalf("claim initial Secret deletion = %t/%v", found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, task.ID, TaskStatusFailed, at.Add(2*time.Second)); err != nil {
		t.Fatalf("fail initial Secret deletion = %v", err)
	}
	seedSecretRecoveryPin(t, store, tasksecretpinrecord.Record{
		OperationID: ids.NewAt(ids.KindOperation, at.Add(3*time.Second), 312),
		SecretID:    current.Record.Secret.ID, MetadataRevision: current.Revision,
		CiphertextSHA256: value.CiphertextSHA256,
	})
	retryID := ids.NewAt(ids.KindTask, at.Add(4*time.Second), 313)
	retryMarker := pendingRetryMarker(
		task,
		retryID,
		at.Add(4*time.Second),
		"secret-recovery-pin-retry",
	)
	before := store.revision
	if _, err := tasks.RetryTask(ctx, task.ID, retryID, TaskActorOperator, retryMarker); !isKind(
		err,
		errs.KindResourceInUse,
	) || store.revision != before {
		t.Fatalf("pinned Secret deletion Retry = %v", err)
	}
	if store.valueAt(taskKey(retryID), store.revision) != nil {
		t.Fatal("rejected pinned Secret Retry published a Task")
	}
}

// Rationale: SEC-07 must fail closed when persisted pin membership is not the
// validated Secret-only operation/revision/digest record; corrupt authority
// cannot permit deletion of SVC-15/JOURNEY-02 recovery input.
func TestMalformedSecretRecoveryPinsFailClosed(t *testing.T) {
	for _, test := range []struct {
		name  string
		key   func(tasksecretpinrecord.Record) string
		value func(*testing.T, tasksecretpinrecord.Record) []byte
	}{
		{
			name: "invalid-json",
			key: func(pin tasksecretpinrecord.Record) string {
				return tasksecretpinrecord.Key(pin.SecretID, pin.OperationID)
			},
			value: func(*testing.T, tasksecretpinrecord.Record) []byte { return []byte("not-json") },
		},
		{
			name: "unknown-field",
			key: func(pin tasksecretpinrecord.Record) string {
				return tasksecretpinrecord.Key(pin.SecretID, pin.OperationID)
			},
			value: func(_ *testing.T, pin tasksecretpinrecord.Record) []byte {
				return []byte(`{"operation_id":"` + pin.OperationID + `","secret_id":"` + pin.SecretID +
					`","metadata_revision":1,"ciphertext_sha256":"` + pin.CiphertextSHA256 +
					`","ciphertext":"forbidden"}`)
			},
		},
		{
			name: "duplicate-field",
			key: func(pin tasksecretpinrecord.Record) string {
				return tasksecretpinrecord.Key(pin.SecretID, pin.OperationID)
			},
			value: func(_ *testing.T, pin tasksecretpinrecord.Record) []byte {
				return []byte(`{"operation_id":"` + pin.OperationID + `","operation_id":"` +
					ids.New(ids.KindOperation) + `","secret_id":"` + pin.SecretID +
					`","metadata_revision":1,"ciphertext_sha256":"` + pin.CiphertextSHA256 + `"}`)
			},
		},
		{
			name: "key-mismatch",
			key: func(pin tasksecretpinrecord.Record) string {
				return tasksecretpinrecord.SecretPrefix(pin.SecretID) + ids.New(ids.KindOperation)
			},
			value: func(t *testing.T, pin tasksecretpinrecord.Record) []byte {
				t.Helper()
				encoded, err := tasksecretpinrecord.Encode(pin)
				if err != nil {
					t.Fatal(err)
				}
				return encoded
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, project, secrets, current, encrypted := secretRecoveryPinFixture(t)
			pin := tasksecretpinrecord.Record{
				OperationID: ids.New(ids.KindOperation), SecretID: current.Record.Secret.ID,
				MetadataRevision: current.Revision, CiphertextSHA256: encrypted.CiphertextSHA256,
			}
			if _, err := store.Transact(ctx, nil, []Mutation{{
				Type: MutationPut, Key: test.key(pin), Value: test.value(t, pin),
			}}); err != nil {
				t.Fatal(err)
			}
			before := store.revision
			if _, err := secrets.DeleteSecret(ctx, ProjectSecretOwner(project), current); !isKind(
				err,
				errs.KindInternal,
			) || store.revision != before {
				t.Fatalf("malformed recovery pin authorized direct deletion: %v", err)
			}
			task, marker, tombstone := secretDeletionTestTask(
				t, current, project, current.Record.Secret.UpdatedAt.Add(time.Second), 321,
			)
			admission, err := newSecretRepository(&connectorReferenceRaceStore{
				memoryHierarchyStore: store, injected: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := admission.BeginSecretDeletionWithTask(
				ctx, ProjectSecretOwner(project), current, tombstone, task, marker,
			)
			if err != nil || !isKind(result.conflict, errs.KindInternal) ||
				store.revision != before {
				t.Fatalf("malformed recovery pin admission = %#v/%v", result, err)
			}
		})
	}
}

func secretRecoveryPinFixture(
	t *testing.T,
) (*memoryHierarchyStore, Versioned[ProjectRecord], *SecretRepository, Versioned[SecretRecord], SecretEncryptedValue) {
	t.Helper()
	ctx := context.Background()
	store, project := secretDeletionTestStore(t)
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 14, 14, 0, 0, 0, time.UTC)
	secretID := ids.NewAt(ids.KindSecret, at, 300)
	record, err := NewProjectSecretRecord(
		secretID,
		project.Record.ID,
		"RECOVERY_TOKEN",
		core.SecretKindEnvVar,
		"",
		at,
	)
	if err != nil {
		t.Fatal(err)
	}
	value := testSecretEncryptedValue(secretID, "opaque-recovery-ciphertext")
	current, err := secrets.CreateSecret(ctx, ProjectSecretOwner(project), record, value)
	if err != nil {
		t.Fatal(err)
	}
	return store, project, secrets, current, value
}

func seedSecretRecoveryPin(
	t *testing.T,
	store *memoryHierarchyStore,
	pin tasksecretpinrecord.Record,
) {
	t.Helper()
	value, err := tasksecretpinrecord.Encode(pin)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: tasksecretpinrecord.Key(pin.SecretID, pin.OperationID), Value: value,
	}})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Secret recovery pin = %#v/%v", result, err)
	}
}
