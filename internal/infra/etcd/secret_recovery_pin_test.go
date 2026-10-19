package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testhierarchydeletionfinalization "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: SEC-07 requires every Secret deletion path to reject an exact
// recovery pin, preserving the predecessor configuration needed by SVC-15 and
// JOURNEY-02 without copying ciphertext into the membership record.
func TestSecretRecoveryPinBlocksDirectAndHierarchyFinalization(t *testing.T) {
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
	before := store.revision

	if _, err := secrets.DeleteSecret(ctx, testsecrets.ProjectOwner(project), current); !isKind(
		err,
		errs.KindResourceInUse,
	) || store.revision != before {
		t.Fatalf("pinned Secret direct finalization = %v", err)
	}
	if _, err := testhierarchydeletionfinalization.NewPreparer(store).Prepare(
		ctx, testhierarchydeletion.HierarchyDeletionOperation{}, testhierarchydeletion.HierarchyDeletionAction{
			ActionKind: testhierarchydeletion.HierarchyDeletionProjectSecretRemove,
			TargetID:   current.Record.Secret.ID, TargetRevision: current.Revision,
		},
	); !isKind(err, errs.KindResourceInUse) || store.revision != before {
		t.Fatalf("pinned Secret hierarchy finalization = %v", err)
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
			if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
				Type: testkeyvalue.MutationPut, Key: test.key(pin), Value: test.value(t, pin),
			}}); err != nil {
				t.Fatal(err)
			}
			before := store.revision
			if _, err := secrets.DeleteSecret(ctx, testsecrets.ProjectOwner(project), current); !isKind(
				err,
				errs.KindInternal,
			) || store.revision != before {
				t.Fatalf("malformed recovery pin authorized direct deletion: %v", err)
			}

		})
	}
}

func secretRecoveryPinFixture(
	t *testing.T,
) (*memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.ProjectRecord], *SecretRepository, testkeyvalue.Versioned[testsecrets.Record], testsecrets.EncryptedValue) {
	t.Helper()
	ctx := context.Background()
	store, project := secretMetadataStore(t)
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 14, 14, 0, 0, 0, time.UTC)
	secretID := ids.NewAt(ids.KindSecret, at, 300)
	record, err := testsecrets.NewProjectRecord(
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
	current, err := secrets.CreateSecret(ctx, testsecrets.ProjectOwner(project), record, value)
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
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: tasksecretpinrecord.Key(pin.SecretID, pin.OperationID), Value: value,
	}})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Secret recovery pin = %#v/%v", result, err)
	}
}
