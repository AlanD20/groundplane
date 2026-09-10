package scriptdefinition

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: exporting the current Blueprint must preserve order after human
// edits; reapplying the exported input must not silently revert migration order.
func TestAuthoringRetainsScriptOrder(t *testing.T) {
	for _, order := range []uint16{0, 10, 65535} {
		result, err := Authoring([]etcd.Versioned[etcd.ScriptRecord]{{Record: etcd.ScriptRecord{
			Origin: "blueprint", ReconciliationKey: "migration-hook",
			Desired: core.Script{
				Slug:        "renamed",
				ServiceName: "api",
				Body:        "echo migrate",
				When:        core.ScriptPreDeploy,
				Order:       order,
			},
		}}})
		if err != nil || result["migration-hook"].Order != order {
			t.Fatalf("authoring order %d: %#v, %v", order, result, err)
		}
	}
}
