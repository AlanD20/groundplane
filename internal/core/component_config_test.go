package core

import "testing"

// Rationale: every enabled registered Component must carry its typed config so
// the public config projection can never expose null for enabled durable state.
func TestEnabledCoreDNSRequiresTypedConfig(t *testing.T) {
	t.Parallel()

	component := Component{
		ID:      "cmp_coredns",
		Owner:   ComponentOwnerPlatform,
		Kind:    ComponentKindCoreDNS,
		Enabled: true,
	}

	if err := component.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want enabled CoreDNS config rejection")
	}
}
