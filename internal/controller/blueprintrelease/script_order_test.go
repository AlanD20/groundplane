package blueprintrelease

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: initial apply and routine release planning use identical ordering
// without crossing Service ownership or making manual scripts automatic.
func TestBlueprintDeployHookSelectionUsesNumericOrder(t *testing.T) {
	selected := deployScriptsByService([]etcd.ScriptRecord{
		{ServiceID: "api", Desired: core.Script{Slug: "a-later", Order: 10, When: core.ScriptPreDeploy}},
		{ServiceID: "api", Desired: core.Script{Slug: "z-first", Order: 0, When: core.ScriptPreDeploy}},
		{ServiceID: "api", Desired: core.Script{Slug: "b-later", Order: 10, When: core.ScriptPreDeploy}},
		{ServiceID: "api", Desired: core.Script{Slug: "manual", Order: 0, When: core.ScriptManual}},
		{ServiceID: "worker", Desired: core.Script{Slug: "other", Order: 0, When: core.ScriptPostDeploy}},
	})
	if len(selected) != 2 || len(selected["api"]) != 3 || len(selected["worker"]) != 1 {
		t.Fatalf("selection: %#v", selected)
	}
	for index, slug := range []string{"z-first", "a-later", "b-later"} {
		if got := selected["api"][index].Desired.Slug; got != slug {
			t.Fatalf("selected[%d] = %s, want %s", index, got, slug)
		}
	}
}
