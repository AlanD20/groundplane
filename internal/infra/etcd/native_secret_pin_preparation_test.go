package etcd

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SEC-07/SVC-15: actual Secret pin storage must refuse a foreign Project or
// changed ciphertext. Startup can reclaim unpublished preparation, and its old
// activation cannot recreate protection afterward. No execution is simulated.
func TestNativeSecretPinPreparationRejectsWrongSourcesAndAbandonsUnpublishedWork(t *testing.T) {
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, _ := createEnvironmentBlueprintOwners(t, hierarchy)
	repository, err := newSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record, err := secrets.NewProjectRecord(ids.New(ids.KindSecret), project.Record.ID,
		"RECOVERY_TOKEN", core.SecretKindEnvVar, "", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	encrypted := testSecretEncryptedValue(record.Secret.ID, "opaque-recovery-ciphertext")
	current, err := repository.CreateSecret(t.Context(), secrets.ProjectOwner(project), record, encrypted)
	if err != nil {
		t.Fatal(err)
	}
	for _, mismatch := range []string{"foreign Project", "changed ciphertext"} {
		t.Run(mismatch, func(t *testing.T) {
			projectID := project.Record.ID
			pin := tasksecretpinrecord.Record{
				OperationID: ids.New(ids.KindOperation), SecretID: record.Secret.ID,
				MetadataRevision: current.Revision, CiphertextSHA256: encrypted.CiphertextSHA256,
			}
			if mismatch == "foreign Project" {
				projectID = ids.New(ids.KindProject)
			} else {
				pin.CiphertextSHA256 = strings.Repeat("0", 64)
			}
			pins, err := tasksecretpins.NewEtcdRepository(store, projectID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pins.Prepare(t.Context(), pin.OperationID, ids.New(ids.KindTask), ids.New(ids.KindWorkItem),
				[]tasksecretpinrecord.Record{pin}); !isKind(err, errs.KindStateConflict) {
				t.Fatalf("invalid Secret source was accepted: %v", err)
			}
		})
	}
	pins, err := tasksecretpins.NewEtcdRepository(store, project.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	operationID := ids.New(ids.KindOperation)
	prepared, err := pins.Prepare(t.Context(), operationID, ids.New(ids.KindTask), ids.New(ids.KindWorkItem),
		[]tasksecretpinrecord.Record{{
			OperationID: operationID, SecretID: current.Record.Secret.ID,
			MetadataRevision: current.Revision, CiphertextSHA256: encrypted.CiphertextSHA256,
		}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.DeleteSecret(t.Context(), secrets.ProjectOwner(project), current); !isKind(
		err,
		errs.KindResourceInUse,
	) {
		t.Fatalf("unpublished preparation did not protect the Secret: %v", err)
	}
	activation, err := tasksecretpins.Activation(prepared)
	if err != nil {
		t.Fatal(err)
	}
	defer activation.Clear()
	if err := RecoverTaskSecretPinSources(t.Context(), store); err != nil {
		t.Fatal(err)
	}
	conditions, writes, err := tasksecretpins.EtcdFragment(activation)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(t.Context(), conditions, writes)
	if err != nil || result.Succeeded || store.valueAt(tasksecretpins.RootKey(operationID), store.revision) != nil {
		t.Fatalf("stale activation revived abandoned Secret pins: %v", err)
	}
	if _, err := repository.DeleteSecret(t.Context(), secrets.ProjectOwner(project), current); err != nil {
		t.Fatalf("startup did not release unpublished Secret protection: %v", err)
	}
	if store.valueAt(secrets.RecordKey(current.Record.Secret.ID), store.revision) != nil ||
		store.valueAt(secrets.ValueKey(current.Record.Secret.ID), store.revision) != nil {
		t.Fatal("Secret removal retained metadata or ciphertext")
	}
}
