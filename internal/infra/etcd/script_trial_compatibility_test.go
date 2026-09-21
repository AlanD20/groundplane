package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
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
		encoded, err := testscripts.EncodeRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		defer clear(encoded)
		if _, err := testrecordcodec.Decode[predecessorScriptRecord](encoded, "script"); err == nil {
			t.Fatal("strict predecessor unexpectedly accepted new metadata")
		}
		if _, err := testscripts.DecodeRecord(encoded); err != nil {
			t.Fatal(err)
		}
	}
}
