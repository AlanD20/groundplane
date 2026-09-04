package scriptsourcereference

import (
	"context"
	"testing"
)

// Rationale: a legal sixteen-member preparation with the maximum distinct
// body/count/Script-aggregate shape must abandon through the existing safe
// physical pages without exceeding the ordinary transaction ceiling or
// retaining partial preparation state.
func TestAbandonMaximumBodyPreparationUsesSafePhysicalPages(t *testing.T) {
	ctx := context.Background()
	store := newReleaseTestStore()
	repository, err := NewRepository(store, releaseCounterScriptCodec{})
	if err != nil {
		t.Fatal(err)
	}
	operationID := "op_abandon_body_window"
	members := make([]Member, normalReleaseWindowSize)
	for index := range members {
		id := releaseTestID(index)
		source := SourceIdentity{
			Kind: SourceBody, EnvironmentID: "env_abandon", ScriptSetGeneration: "generation",
			ScriptID: "script-" + id, BodyGeneration: 1,
		}
		sourceRecord := store.put("/sources/body/"+id, []byte("body"))
		store.put(ScriptPrimaryKey(source), []byte("0"))
		members[index] = Member{
			Reference: Reference{
				OperationID: operationID, ScriptExecutionID: "execution-" + id, Source: source,
				SourceOwnerID: "env_abandon", SourceModRevision: sourceRecord.ModRevision,
			},
			SourceKey: sourceRecord.Key, Mode: EvidenceExisting,
		}
	}
	if _, err := repository.Prepare(ctx, operationID, members); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if err := repository.Abandon(ctx, operationID, members); err != nil {
		t.Fatalf("Abandon() error = %v", err)
	}
	if store.maximumRangeLimit != releaseBatchSize {
		t.Fatalf("abandonment range limit = %d, want %d", store.maximumRangeLimit, releaseBatchSize)
	}
	if store.maximumTransactionOperations > releaseTransactionOperationLimit {
		t.Fatalf(
			"maximum transaction operations = %d, want <= %d",
			store.maximumTransactionOperations,
			releaseTransactionOperationLimit,
		)
	}
	if store.values[PreparationKey(operationID)] != nil || store.values[RootKey(operationID)] != nil {
		t.Fatal("abandonment retained preparation or root state")
	}
	assertReleaseMembersAbsent(t, store, members)
	for _, member := range members {
		if got := string(store.values[ScriptPrimaryKey(member.Reference.Source)].Value); got != "0" {
			t.Fatalf("released Script aggregate = %q, want 0", got)
		}
	}
}
