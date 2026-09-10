package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: prepared source membership must retain only the declared explicit
// Entry generations, not require or reserve unrelated exposed credentials.
func TestScriptContextManualEntrySourcesSelectOnlyGrantedEntries(t *testing.T) {
	for _, granted := range []bool{false, true} {
		store, sources, execution, _, _ := manualScriptLifecycleFixture(t)
		const entryID = "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		const generationID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		content := []byte("selected value")
		digest := sha256.Sum256(content)
		values, err := newEntryValueGenerationRepository(store)
		if err != nil {
			t.Fatal(err)
		}
		if err := values.CreatePlain(context.Background(), PlainEntryValueGeneration{
			EnvironmentID: sources.Environment.Record.ID, EntryID: entryID, GenerationID: generationID,
			Content: content, PlaintextSHA256: hex.EncodeToString(digest[:]), CreatedAt: execution.CreatedAt,
		}); err != nil {
			t.Fatal(err)
		}
		sources.Revision = store.revision
		sources.DesiredProjection.Record.Entries = []EntryRecord{
			{EnvironmentID: sources.Environment.Record.ID, CurrentValueGenerationID: generationID,
				Entry: core.EnvEntry{ID: entryID, Kind: core.EntryKindEnv, Key: "SELECTED", Exposure: []string{"all"},
					Source: core.EntrySource{Kind: core.SourceLiteral, Literal: string(content)}}},
			{EnvironmentID: sources.Environment.Record.ID, CurrentValueGenerationID: generationID,
				Entry: core.EnvEntry{ID: "ev_01ARZ3NDEKTSV4RRFFQ69G5FAW", Kind: core.EntryKindEnv,
					Key: "UNSELECTED", Secret: true, Exposure: []string{"all"},
					Source: core.EntrySource{Kind: core.SourceSecretRef, SecretRef: "unselected"}}},
		}
		sources.Script.Record.Desired.Execution = &core.ScriptExecution{
			Mode: core.ScriptExecutionExplicit, Image: "example/setup@sha256:" + strings.Repeat("b", 64), User: "0:0",
		}
		var bindings []*agentpb.ScriptRunnerEntryBinding
		if granted {
			sources.Script.Record.Desired.Execution.EntryIDs = []string{entryID}
			bindings = []*agentpb.ScriptRunnerEntryBinding{{EntryId: entryID, ValueGenerationId: generationID,
				Kind: agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV, EnvironmentKey: "SELECTED", Sha256: digest[:]}}
		}
		members, err := (&ScriptRepository{store: store}).manualScriptEntrySourceMembers(
			context.Background(), sources, ScriptSourceReference{SourceOwnerID: sources.Environment.Record.ID,
				OperationID: execution.OperationID, ScriptExecutionID: execution.ID}, bindings,
		)
		if err != nil || len(members) != len(bindings) {
			t.Fatalf("explicit Entry source selection (granted=%t): count=%d, %v", granted, len(members), err)
		}
		if granted && (members[0].Reference.Source.EntryID != entryID ||
			members[0].Reference.SourceDigest != hex.EncodeToString(digest[:])) {
			t.Fatal("selected Entry generation lost its exact source identity/digest")
		}
	}
}

// Rationale: an empty projection and binding list must not hide a missing
// explicitly declared Entry at the persistence boundary.
func TestScriptContextManualEntrySourcesRejectMissingGrant(t *testing.T) {
	store, sources, _, _, _ := manualScriptLifecycleFixture(t)
	sources.Script.Record.Desired.Execution = &core.ScriptExecution{
		Mode: core.ScriptExecutionExplicit, Image: "example/setup@sha256:" + strings.Repeat("b", 64), User: "0:0",
		EntryIDs: []string{"ev_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	}
	_, err := (&ScriptRepository{store: store}).manualScriptEntrySourceMembers(
		context.Background(), sources, ScriptSourceReference{SourceOwnerID: sources.Environment.Record.ID}, nil,
	)
	if !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("missing explicit Entry grant accepted: %v", err)
	}
}
