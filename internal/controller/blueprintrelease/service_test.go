package blueprintrelease

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestPostDeployHookBoundsRejectBeforePublication(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		executions int
		bodyBytes  uint64
	}{
		{name: "seventeenth execution", executions: 17, bodyBytes: 17},
		{name: "aggregate body", executions: 16, bodyBytes: 1<<20 + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateDeployHookBounds(test.executions, test.bodyBytes)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("validateDeployHookBounds() error = %v, want validation.failed", err)
			}
		})
	}
}

// Rationale: initial pre-hooks and post-hooks share one selection and budget;
// manual scripts must not run implicitly during Blueprint apply.
func TestDeployScriptSelectionIncludesBothPhasesAndExcludesManual(t *testing.T) {
	t.Parallel()
	serviceID := "svc_new"
	selected := deployScriptsByService([]testscripts.Record{
		{ServiceID: serviceID, Desired: core.Script{ID: "scr_pre", Slug: "a-prepare", When: core.ScriptPreDeploy}},
		{
			ServiceID: serviceID,
			Desired:   core.Script{ID: "scr_post", Slug: "migrate", When: core.ScriptPostDeploy},
		},
		{
			ServiceID: serviceID,
			Desired:   core.Script{ID: "scr_manual", Slug: "manual", When: core.ScriptManual},
		},
	})
	if len(selected) != 1 || len(selected[serviceID]) != 2 ||
		selected[serviceID][0].Desired.ID != "scr_pre" || selected[serviceID][1].Desired.ID != "scr_post" {
		t.Fatalf("selected post-deploy Scripts = %#v", selected)
	}
}
