package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: source reservation and Secret retirement must be mutually
// exclusive, including the interval before a prepared Script Task is published.
func TestSecretDeletionAndScriptPreparationAreMutuallyExclusive(t *testing.T) {
	for _, deletionFirst := range []bool{false, true} {
		name := "preparation-first"
		if deletionFirst {
			name = "deletion-first"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store, project := secretDeletionTestStore(t)
			repository, err := newSecretRepository(&connectorReferenceRaceStore{
				memoryHierarchyStore: store, injected: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			at := time.Date(2026, 9, 8, 20, 0, 0, 0, time.UTC)
			secretID := ids.New(ids.KindSecret)
			record, err := NewProjectSecretRecord(secretID, project.Record.ID, "TOKEN", core.SecretKindEnvVar, "", at)
			if err != nil {
				t.Fatal(err)
			}
			value := testSecretEncryptedValue(secretID, "opaque-ciphertext")
			current, err := repository.CreateSecret(ctx, ProjectSecretOwner(project), record, value)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := newScriptSourceReferenceAuthority(store)
			if err != nil {
				t.Fatal(err)
			}
			operationID := ids.New(ids.KindOperation)
			source := ScriptSourceIdentity{
				Kind:              ScriptSourceSecretValue,
				SecretID:          secretID,
				ValueGenerationID: secretID,
			}
			member := ScriptSourcePreparationMember{
				Reference: ScriptSourceReference{
					OperationID: operationID, ScriptExecutionID: ids.NewULID(), Source: source,
					SourceOwnerID: project.Record.ID, SourceModRevision: current.Revision, SourceDigest: value.CiphertextSHA256,
				},
				Evidence: ScriptSourceEvidence{
					Existing: &ScriptExistingSourceEvidence{SourceKey: secretValueKey(secretID)},
				},
			}
			task, marker, tombstone := secretDeletionTestTask(t, current, project, at.Add(time.Second), 160)
			if deletionFirst {
				result, err := repository.BeginSecretDeletionWithTask(
					ctx,
					ProjectSecretOwner(project),
					current,
					tombstone,
					task,
					marker,
				)
				if err != nil || result.kind != idempotencyTransactionApplied {
					t.Fatalf("initial deletion = %v", err)
				}
				if _, err := authority.Prepare(ctx, operationID, []ScriptSourcePreparationMember{member}); err == nil {
					t.Fatal("reserved a Secret whose deletion already owns its ciphertext")
				}
				if store.valueAt(scriptSourceCountKey(source), store.revision) != nil ||
					store.valueAt(scriptSourceForwardReferenceKey(member.Reference), store.revision) != nil {
					t.Fatal("rejected reservation left Secret memberships")
				}
				return
			}
			if _, err := authority.Prepare(ctx, operationID, []ScriptSourcePreparationMember{member}); err != nil {
				t.Fatal(err)
			}
			before := store.revision
			result, err := repository.BeginSecretDeletionWithTask(
				ctx,
				ProjectSecretOwner(project),
				current,
				tombstone,
				task,
				marker,
			)
			if err != nil || !isKind(result.conflict, errs.KindResourceInUse) || store.revision != before {
				t.Fatalf("deleted prepared Secret before Script publication: %v / %v", err, result.conflict)
			}
			if _, err := repository.DeleteSecret(ctx, ProjectSecretOwner(project), current); !isKind(
				err,
				errs.KindResourceInUse,
			) ||
				store.revision != before {
				t.Fatalf("direct Secret finalizer ignored prepared references: %v", err)
			}
			hierarchy, err := newHierarchyDeletionRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := hierarchy.prepareHierarchyDeletionSecretFinalizer(ctx, HierarchyDeletionAction{
				TargetID: secretID, TargetRevision: current.Revision,
			}); !isKind(err, errs.KindResourceInUse) || store.revision != before {
				t.Fatalf("parent Secret finalizer ignored prepared references: %v", err)
			}
		})
	}
}
