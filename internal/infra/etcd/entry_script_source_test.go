package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: deleting an Entry deletes every generation, so a retained old
// generation must block removal even after a valid edit creates a newer value.
func TestEntryRemovalProtectsOlderScriptGenerationButAllowsEdit(t *testing.T) {
	ctx := context.Background()
	repository, store, environment, project, current, generations := entryDeletionTestState(t)
	member := entryScriptTestMember(t, store, current.Record, generations[0])
	authority, err := newScriptSourceReferenceAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Prepare(ctx, member.Reference.OperationID, []ScriptSourcePreparationMember{member}); err != nil {
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
		newID,
		EntryValueGeneration{Plain: &value},
	)
	if err != nil || updated.Record.CurrentValueGenerationID != newID {
		t.Fatalf("retained generation blocked a new immutable value: %v", err)
	}
	repository, err = newEntryRepository(&connectorReferenceRaceStore{memoryHierarchyStore: store, injected: true})
	if err != nil {
		t.Fatal(err)
	}
	task, marker, tombstone, intent := entryDeletionTestRecords(t, project, environment, updated, nil)
	before := store.revision
	result, err := repository.BeginEntryDeletionWithTask(
		ctx,
		environment,
		project,
		updated,
		nil,
		tombstone,
		intent,
		task,
		marker,
	)
	if !isKind(err, errs.KindResourceInUse) && !isKind(result.conflict, errs.KindResourceInUse) ||
		store.revision != before {
		t.Fatalf("Entry removal ignored an older retained generation: %v / %v", err, result.conflict)
	}
	if _, err := repository.DeleteEntry(ctx, environment, project, updated); !isKind(err, errs.KindResourceInUse) ||
		store.revision != before {
		t.Fatalf("direct Entry finalizer erased an older retained generation: %v", err)
	}
	hierarchy, err := newHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hierarchy.prepareHierarchyDeletionEntryFinalizer(ctx, HierarchyDeletionAction{
		TargetID: updated.Record.Entry.ID, TargetRevision: updated.Revision,
	}); !isKind(err, errs.KindResourceInUse) || store.revision != before {
		t.Fatalf("parent Entry finalizer ignored retained generation: %v", err)
	}
	if err := authority.Abandon(ctx, member.Reference.OperationID, []ScriptSourcePreparationMember{member}); err != nil {
		t.Fatal(err)
	}
	result, err = repository.BeginEntryDeletionWithTask(
		ctx,
		environment,
		project,
		updated,
		nil,
		tombstone,
		intent,
		task,
		marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("Entry removal remained blocked after source release: %v / %v", err, result.conflict)
	}
}

// Rationale: an Entry's values remain present until removal acknowledgement;
// their existence cannot authorize reservation after retirement has started.
func TestEntryScriptPreparationRejectsRetiringEntry(t *testing.T) {
	ctx := context.Background()
	repository, store, environment, project, current, generations := entryDeletionTestState(t)
	member := entryScriptTestMember(t, store, current.Record, generations[0])
	task, marker, tombstone, intent := entryDeletionTestRecords(t, project, environment, current, nil)
	result, err := repository.BeginEntryDeletionWithTask(
		ctx,
		environment,
		project,
		current,
		nil,
		tombstone,
		intent,
		task,
		marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("initial Entry removal = %v / %v", err, result.conflict)
	}
	authority, err := newScriptSourceReferenceAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Prepare(ctx, member.Reference.OperationID, []ScriptSourcePreparationMember{member}); err == nil {
		t.Fatal("reserved an Entry after its deletion started")
	}
	if store.valueAt(scriptSourceCountKey(member.Reference.Source), store.revision) != nil {
		t.Fatal("rejected Entry reservation left a count")
	}
}

func entryScriptTestMember(
	t *testing.T,
	store *memoryHierarchyStore,
	entry EntryRecord,
	generationID string,
) ScriptSourcePreparationMember {
	t.Helper()
	key := plainEntryValueGenerationKey(entry.Entry.ID, generationID)
	stored := store.valueAt(key, store.revision)
	if stored == nil {
		t.Fatal("Entry fixture generation is missing")
	}
	value, err := decodePlainEntryValueGeneration(stored.Value)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value.Content)
	return ScriptSourcePreparationMember{
		Reference: ScriptSourceReference{
			OperationID: ids.New(ids.KindOperation), ScriptExecutionID: ids.NewULID(),
			Source: ScriptSourceIdentity{
				Kind:              ScriptSourceEntryValue,
				EntryID:           entry.Entry.ID,
				ValueGenerationID: generationID,
			},
			SourceOwnerID: entry.EnvironmentID, SourceModRevision: stored.ModRevision, SourceDigest: value.PlaintextSHA256,
		},
		Evidence: ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{SourceKey: key}},
	}
}
