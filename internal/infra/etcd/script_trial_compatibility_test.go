package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

// This is the strict Script wire shape in the deployed pre-order/context
// predecessor (85a472476), not an alternative production decoder.
type predecessorScriptRecord struct {
	EnvironmentID       string `json:"environment_id"`
	ServiceID           string `json:"service_id"`
	Origin              string `json:"origin"`
	ReconciliationKey   string `json:"reconciliation_key,omitempty"`
	ActiveGeneration    uint64 `json:"active_generation"`
	ActiveReferences    uint64 `json:"active_references"`
	ScriptSetGeneration string `json:"script_set_generation"`
	Desired             struct {
		ID          string          `json:"id"`
		Slug        string          `json:"slug"`
		ServiceName string          `json:"service"`
		When        core.ScriptHook `json:"when"`
	} `json:"desired"`
}

// Rationale: startup's unpublished-source cleanup is permitted before trial
// qualification. Its count rewrite must not add forward-only Script metadata.
func TestScriptTrialCleanupPreservesPredecessorWireShape(t *testing.T) {
	store, sources, _, _, _ := manualScriptLifecycleFixture(t)
	record := sources.Script.Record
	key := scriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID)
	value := store.valueAt(key, store.revision)
	if value == nil {
		t.Fatal("missing actual Script primary")
	}
	source := ScriptSourceIdentity{
		Kind: ScriptSourceBody, EnvironmentID: record.EnvironmentID,
		ScriptID: record.Desired.ID, ScriptSetGeneration: record.ScriptSetGeneration,
	}
	adapter := &scriptSourceReferenceStore{}
	prepared, err := adapter.AdjustScriptPrimary(value.Value, source, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(prepared)
	cleaned, err := adapter.AdjustScriptPrimary(prepared, source, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(cleaned)
	for _, encoded := range [][]byte{value.Value, prepared, cleaned} {
		if _, err := decodeEnvelope[predecessorScriptRecord](encoded, "script"); err != nil {
			t.Fatalf("count-only cleanup introduced predecessor-incompatible fields: %v", err)
		}
	}
}

// Rationale: additive JSON fields are not automatically rollback-compatible:
// authoring nondefault metadata must remain outside an unfinished native trial.
func TestScriptTrialAuthoredMetadataRequiresQualifiedWriter(t *testing.T) {
	for _, explicitReset := range []bool{false, true} {
		_, sources, _, _, _ := manualScriptLifecycleFixture(t)
		record := sources.Script.Record
		if explicitReset {
			record.Desired.Execution = &core.ScriptExecution{Mode: core.ScriptExecutionInherited}
		} else {
			record.Desired.Order = 42
		}
		encoded, err := encodeScriptRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		defer clear(encoded)
		if _, err := decodeEnvelope[predecessorScriptRecord](encoded, "script"); err == nil {
			t.Fatal("strict predecessor unexpectedly accepted new metadata")
		}
		if _, err := decodeScriptRecord(encoded); err != nil {
			t.Fatal(err)
		}
	}
}
