package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testhierarchydeletionfinalization "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
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
			store, project := secretMetadataStore(t)
			repository, err := newSecretRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			at := time.Date(2026, 9, 8, 20, 0, 0, 0, time.UTC)
			secretID := ids.New(ids.KindSecret)
			record, err := testsecrets.NewProjectRecord(
				secretID,
				project.Record.ID,
				"TOKEN",
				core.SecretKindEnvVar,
				"",
				at,
			)
			if err != nil {
				t.Fatal(err)
			}
			value := testSecretEncryptedValue(secretID, "opaque-ciphertext")
			current, err := repository.CreateSecret(ctx, testsecrets.ProjectOwner(project), record, value)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := testscriptsourcepublication.NewAuthority(store)
			if err != nil {
				t.Fatal(err)
			}
			operationID := ids.New(ids.KindOperation)
			source := testscriptsourcereference.SourceIdentity{
				Kind:              testscriptsourcereference.SourceSecretValue,
				SecretID:          secretID,
				ValueGenerationID: secretID,
			}
			member := testscriptsourceevidence.ScriptSourcePreparationMember{
				Reference: testscriptsourcereference.Reference{
					OperationID: operationID, ScriptExecutionID: ids.NewULID(), Source: source,
					SourceOwnerID: project.Record.ID, SourceModRevision: current.Revision, SourceDigest: value.CiphertextSHA256,
				},
				Evidence: testscriptsourceevidence.ScriptSourceEvidence{
					Existing: &testscriptsourceevidence.ScriptExistingSourceEvidence{
						SourceKey: testsecrets.ValueKey(secretID),
					},
				},
			}
			if deletionFirst {
				if _, err := repository.DeleteSecret(ctx, testsecrets.ProjectOwner(project), current); err != nil {
					t.Fatal(err)
				}
				if _, err := authority.Prepare(ctx, operationID, []testscriptsourceevidence.ScriptSourcePreparationMember{member}); err == nil {
					t.Fatal("reserved a Secret whose deletion already owns its ciphertext")
				}
				if store.valueAt(testscriptsourceevidence.ScriptSourceCountKey(source), store.revision) != nil ||
					store.valueAt(
						testscriptsourceevidence.ScriptSourceForwardReferenceKey(member.Reference),
						store.revision,
					) != nil {
					t.Fatal("rejected reservation left Secret memberships")
				}
				return
			}
			if _, err := authority.Prepare(ctx, operationID, []testscriptsourceevidence.ScriptSourcePreparationMember{member}); err != nil {
				t.Fatal(err)
			}
			before := store.revision
			if _, err := repository.DeleteSecret(ctx, testsecrets.ProjectOwner(project), current); !isKind(
				err,
				errs.KindResourceInUse,
			) ||
				store.revision != before {
				t.Fatalf("direct Secret finalizer ignored prepared references: %v", err)
			}
			if _, err := testhierarchydeletionfinalization.NewPreparer(store).Prepare(
				ctx, testhierarchydeletion.HierarchyDeletionOperation{}, testhierarchydeletion.HierarchyDeletionAction{
					ActionKind: testhierarchydeletion.HierarchyDeletionProjectSecretRemove,
					TargetID:   secretID, TargetRevision: current.Revision,
				},
			); !isKind(err, errs.KindResourceInUse) || store.revision != before {
				t.Fatalf("parent Secret finalizer ignored prepared references: %v", err)
			}
			if err := authority.Abandon(ctx, operationID, []testscriptsourceevidence.ScriptSourcePreparationMember{member}); err != nil {
				t.Fatal(err)
			}
			if _, err := repository.DeleteSecret(ctx, testsecrets.ProjectOwner(project), current); err != nil {
				t.Fatalf("Secret removal remained blocked after source release: %v", err)
			}
			deleted, err := store.Get(ctx, testsecrets.ValueKey(secretID))
			if err != nil || deleted.Entry != nil {
				t.Fatalf("released Secret ciphertext remained after deletion: %#v, %v", deleted, err)
			}
		})
	}
}
