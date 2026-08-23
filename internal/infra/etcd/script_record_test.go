package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestReplaceScriptDesiredPreservesStableReferences(t *testing.T) {
	// Rationale: an edit must never retarget or rename a Script behind its stable id and indexes.
	record, err := NewScriptRecord(ids.New(ids.KindEnvironment), ids.New(ids.KindService), core.Script{
		ID: ids.New(ids.KindScript), Name: "migrate", ServiceName: "api",
		Body: "first", When: core.ScriptHook("manual"),
	})
	if err != nil {
		t.Fatalf("NewScriptRecord(): %v", err)
	}
	desired := record.Desired
	desired.Body = "second"
	desired.When = core.ScriptHook("pre-deploy")
	replacement, err := ReplaceScriptDesired(record, desired)
	if err != nil {
		t.Fatalf("ReplaceScriptDesired(): %v", err)
	}
	if replacement.EnvironmentID != record.EnvironmentID || replacement.ServiceID != record.ServiceID ||
		replacement.Desired.ID != record.Desired.ID || replacement.Desired.Name != record.Desired.Name {
		t.Fatal("ReplaceScriptDesired() changed a stable reference")
	}
}

func TestReplaceScriptDesiredAllowsServiceLabelRefresh(t *testing.T) {
	// Rationale: Service names are renamable labels; refreshing the projection must preserve its stable Service id.
	record, err := NewScriptRecord(ids.New(ids.KindEnvironment), ids.New(ids.KindService), core.Script{
		ID: ids.New(ids.KindScript), Name: "migrate", ServiceName: "api",
		Body: "first", When: core.ScriptHook("manual"),
	})
	if err != nil {
		t.Fatalf("NewScriptRecord(): %v", err)
	}
	desired := record.Desired
	desired.ServiceName = "worker"
	replacement, err := ReplaceScriptDesired(record, desired)
	if err != nil {
		t.Fatalf("ReplaceScriptDesired(): %v", err)
	}
	if replacement.ServiceID != record.ServiceID || replacement.Desired.ServiceName != "worker" {
		t.Fatal("ReplaceScriptDesired() did not refresh the label projection safely")
	}
}
