package scriptsourcereference

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
)

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
func TestEtcdScriptPrimaryAdjustmentPreservesPredecessorWireShape(t *testing.T) {
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	serviceID := ids.NewAt(ids.KindService, at, 2)
	scriptID := ids.NewAt(ids.KindScript, at, 3)
	setGeneration := ids.NewAt(ids.KindTask, at, 4)
	record, err := scriptrecord.NewRecord(environmentID, serviceID, core.Script{
		ID: scriptID, Slug: "cleanup", ServiceName: "api", When: core.ScriptManual, Body: "exit 0",
	})
	if err != nil {
		t.Fatal(err)
	}
	record.ScriptSetGeneration = setGeneration
	value, err := scriptrecord.EncodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	source := SourceIdentity{
		Kind: SourceBody, EnvironmentID: environmentID,
		ScriptID: scriptID, ScriptSetGeneration: setGeneration,
	}
	adapter := &scriptSourceReferenceStore{}
	prepared, err := adapter.AdjustScriptPrimary(value, source, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(prepared)
	cleaned, err := adapter.AdjustScriptPrimary(prepared, source, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(cleaned)
	for _, encoded := range [][]byte{value, prepared, cleaned} {
		if _, err := recordcodec.Decode[predecessorScriptRecord](encoded, "script"); err != nil {
			t.Fatalf("count-only cleanup introduced predecessor-incompatible fields: %v", err)
		}
	}
}
