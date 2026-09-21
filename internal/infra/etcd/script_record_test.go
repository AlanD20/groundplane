package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
)

func TestReplaceScriptDesiredPreservesStableReferencesAndAllowsSlugRename(t *testing.T) {
	// Rationale: an edit may rename the operator label but must never retarget a Script behind its stable id.
	record, err := testscripts.NewRecord(ids.New(ids.KindEnvironment), ids.New(ids.KindService), core.Script{
		ID: ids.New(ids.KindScript), Slug: "migrate", ServiceName: "api",
		Body: "first", When: core.ScriptHook("manual"),
	})
	if err != nil {
		t.Fatalf("NewScriptRecord(): %v", err)
	}
	desired := record.Desired
	desired.Slug = "migrate-database"
	desired.Body = "second"
	desired.When = core.ScriptHook("pre-deploy")
	replacement, err := testscripts.ReplaceDesired(record, desired)
	if err != nil {
		t.Fatalf("ReplaceScriptDesired(): %v", err)
	}
	if replacement.EnvironmentID != record.EnvironmentID || replacement.ServiceID != record.ServiceID ||
		replacement.Desired.ID != record.Desired.ID || replacement.Desired.Slug != "migrate-database" ||
		replacement.ActiveGeneration != 2 {
		t.Fatal("ReplaceScriptDesired() changed a stable reference")
	}
}

func TestReplaceScriptDesiredAllowsServiceLabelRefresh(t *testing.T) {
	// Rationale: Service names are renamable labels; refreshing the projection must preserve its stable Service id.
	record, err := testscripts.NewRecord(ids.New(ids.KindEnvironment), ids.New(ids.KindService), core.Script{
		ID: ids.New(ids.KindScript), Slug: "migrate", ServiceName: "api",
		Body: "first", When: core.ScriptHook("manual"),
	})
	if err != nil {
		t.Fatalf("NewScriptRecord(): %v", err)
	}
	desired := record.Desired
	desired.ServiceName = "worker"
	replacement, err := testscripts.ReplaceDesired(record, desired)
	if err != nil {
		t.Fatalf("ReplaceScriptDesired(): %v", err)
	}
	if replacement.ServiceID != record.ServiceID || replacement.Desired.ServiceName != "worker" {
		t.Fatal("ReplaceScriptDesired() did not refresh the label projection safely")
	}
	if replacement.ActiveGeneration != record.ActiveGeneration {
		t.Fatal("ReplaceScriptDesired() created a body generation for a metadata-only edit")
	}
}
