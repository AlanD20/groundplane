package component

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: disabled Components retain dormant desired state internally but
// must expose a null active config until they are enabled.
func TestProjectComponentConfigHidesDisabledConfiguration(t *testing.T) {
	t.Parallel()
	component := core.Component{
		Enabled: false,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneID: "net_01ARZ3NDEKTSV4RRFFQ69G5FAX",
		}},
	}
	if config := projectComponentConfig(component); config != nil {
		t.Fatalf("projectComponentConfig(disabled) = %#v, want nil", config)
	}
	component.Enabled = true
	if config := projectComponentConfig(component); config == nil || config.Caddy == nil {
		t.Fatalf("projectComponentConfig(enabled) = %#v, want Caddy config", config)
	}
}
