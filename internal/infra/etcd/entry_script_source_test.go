package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testentryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testhierarchydeletionfinalization "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: deleting an Entry deletes every generation, so a retained old
// generation must block removal even after a valid edit creates a newer value.
func TestEntryRemovalProtectsOlderScriptGenerationButAllowsEdit(t *testing.T) {
	ctx := context.Background()
	repository, store, environment, project, current, generations := entryMetadataFixture(t)
	member := entryScriptTestMember(t, store, current.Record, generations[0])
	authority, err := testscriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Prepare(ctx, member.Reference.OperationID, []testscriptsourceevidence.ScriptSourcePreparationMember{member}); err != nil {
		t.Fatal(err)
	}
	desired := current.Record.Entry
	desired.Source.Literal = "new-value"
	newID := ids.New(ids.KindConfig)
	value := testPlainGeneration(
		environment.Record.ID,
		desired.ID,
		newID,
		"new-value",
		serviceRecordTestTime().Add(5*time.Hour),
	)
	updated, err := repository.ReplaceEntry(
		ctx,
		environment,
		project,
		current,
		desired,
		newID, testentries.EntryValueGeneration{Plain: &value},
	)
	if err != nil || updated.Record.CurrentValueGenerationID != newID {
		t.Fatalf("retained generation blocked a new immutable value: %v", err)
	}
	before := store.revision
	if _, err := repository.DeleteEntry(ctx, environment, project, updated); !isKind(err, errs.KindResourceInUse) ||
		store.revision != before {
		t.Fatalf("direct Entry finalizer erased an older retained generation: %v", err)
	}
	if _, err := testhierarchydeletionfinalization.NewPreparer(store).Prepare(
		ctx, testhierarchydeletion.HierarchyDeletionOperation{}, testhierarchydeletion.HierarchyDeletionAction{
			ActionKind: testhierarchydeletion.HierarchyDeletionEntryRemove,
			TargetID:   updated.Record.Entry.ID, TargetRevision: updated.Revision,
		},
	); !isKind(err, errs.KindResourceInUse) || store.revision != before {
		t.Fatalf("parent Entry finalizer ignored retained generation: %v", err)
	}
	if err := authority.Abandon(ctx, member.Reference.OperationID, []testscriptsourceevidence.ScriptSourcePreparationMember{member}); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.DeleteEntry(ctx, environment, project, updated); err != nil {
		t.Fatalf("Entry metadata removal remained blocked after source release: %v", err)
	}
}

// Rationale: an Entry's values remain present until removal acknowledgement;
// their existence cannot authorize reservation after retirement has started.
func TestEntryScriptPreparationRejectsRetiringEntry(t *testing.T) {
	ctx := context.Background()
	_, store, _, _, current, generations := entryMetadataFixture(t)
	member := entryScriptTestMember(t, store, current.Record, generations[0])
	// Supply a retirement marker to exercise source admission, not Task acceptance.
	value, err := testdeletions.EncodeDeletionTombstone(testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetEntry, TargetID: current.Record.Entry.ID, TargetRevision: current.Revision,
		TaskID: ids.New(ids.KindTask), Phase: testdeletions.DeletionPhaseHostEffects,
		CreatedAt: serviceRecordTestTime(), UpdatedAt: serviceRecordTestTime(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut,
		Key: testdeletions.TombstoneKey(string(testdeletions.DeletionTargetEntry), current.Record.Entry.ID), Value: value}}); err != nil {
		t.Fatal(err)
	}
	authority, err := testscriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Prepare(ctx, member.Reference.OperationID, []testscriptsourceevidence.ScriptSourcePreparationMember{member}); err == nil {
		t.Fatal("reserved an Entry after its deletion started")
	}
	if store.valueAt(testscriptsourceevidence.ScriptSourceCountKey(member.Reference.Source), store.revision) != nil {
		t.Fatal("rejected Entry reservation left a count")
	}
}

func entryScriptTestMember(
	t *testing.T,
	store *memoryHierarchyStore,
	entry testentries.Record,
	generationID string,
) testscriptsourceevidence.ScriptSourcePreparationMember {
	t.Helper()
	key := testentryvalues.PlainKey(entry.Entry.ID, generationID)
	stored := store.valueAt(key, store.revision)
	if stored == nil {
		t.Fatal("Entry fixture generation is missing")
	}
	value, err := testentryvalues.DecodePlain(stored.Value)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value.Content)
	return testscriptsourceevidence.ScriptSourcePreparationMember{
		Reference: testscriptsourcereference.Reference{
			OperationID: ids.New(ids.KindOperation), ScriptExecutionID: ids.NewULID(),
			Source: testscriptsourcereference.SourceIdentity{
				Kind:              testscriptsourcereference.SourceEntryValue,
				EntryID:           entry.Entry.ID,
				ValueGenerationID: generationID,
			},
			SourceOwnerID: entry.EnvironmentID, SourceModRevision: stored.ModRevision, SourceDigest: value.PlaintextSHA256,
		},
		Evidence: testscriptsourceevidence.ScriptSourceEvidence{
			Existing: &testscriptsourceevidence.ScriptExistingSourceEvidence{SourceKey: key},
		},
	}
}
