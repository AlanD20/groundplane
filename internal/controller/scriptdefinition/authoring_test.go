package scriptdefinition

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
)

// Rationale: exporting the current Blueprint must preserve order after human
// edits; reapplying the exported input must not silently revert migration order.
func TestAuthoringRetainsScriptOrder(t *testing.T) {
	at := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	for _, order := range []uint16{0, 10, 65535} {
		result, err := Authoring([]testkeyvalue.Versioned[testscripts.Record]{{Record: testscripts.Record{
			EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 1),
			ServiceID:     ids.NewAt(ids.KindService, at, 2), ActiveGeneration: 1,
			Origin: "blueprint", ReconciliationKey: "migration-hook",
			Desired: core.Script{
				ID:          ids.NewAt(ids.KindScript, at, 3),
				Slug:        "renamed",
				ServiceName: "api",
				Body:        "echo migrate",
				When:        core.ScriptPreDeploy,
				Order:       order,
			},
		}}}, nil, nil)
		if err != nil || result["migration-hook"].Order != order {
			t.Fatalf("authoring order %d: %#v, %v", order, result, err)
		}
	}
}

// Rationale: a direct Script must be visible in the complete Environment
// Blueprint under an id-derived key, even after its mutable slug changes.
func TestAuthoringIncludesDirectScriptWithStableKey(t *testing.T) {
	at := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	id := ids.NewAt(ids.KindScript, at, 1)
	record := testscripts.Record{Origin: "api", ActiveGeneration: 1,
		EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 2), ServiceID: ids.NewAt(ids.KindService, at, 3),
		Desired: core.Script{
			ID: id, Slug: "setup", ServiceName: "api", Body: "echo setup", When: core.ScriptManual,
		}}
	key, err := testscripts.BlueprintAuthoringKey(record)
	if err != nil {
		t.Fatal(err)
	}
	first, err := Authoring([]testkeyvalue.Versioned[testscripts.Record]{{Record: record}}, nil, nil)
	if err != nil || len(first) != 1 || first[key].Slug != "setup" {
		t.Fatalf("direct Script export = %#v, %v", first, err)
	}
	record.Desired.Slug = "renamed"
	second, err := Authoring([]testkeyvalue.Versioned[testscripts.Record]{{Record: record}}, nil, nil)
	if err != nil || len(second) != 1 || second[key].Slug != "renamed" {
		t.Fatalf("renamed direct Script export = %#v, %v", second, err)
	}
}
