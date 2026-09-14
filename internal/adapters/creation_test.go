package adapters_test

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/adapters/postgres16"
	"github.com/AlanD20/groundplane/internal/adapters/valkey9"
	"github.com/AlanD20/groundplane/internal/core"
)

// BACK-01: Rationale: registered backing creation must provide its required
// workload fields with an explicit choice for adapters that support auth modes.
func TestBackingCreationSpecs(t *testing.T) {
	postgres16.Register()
	valkey9.Register()

	for _, test := range []struct {
		key            string
		authentication core.BackingAuthentication
	}{{"postgres:16", ""}, {"valkey:9", core.BackingAuthenticationPassword}} {
		spec, err := adapters.BackingCreationSpec(test.key, test.authentication)
		if err != nil {
			t.Fatalf("BackingCreationSpec(%q): %v", test.key, err)
		}
		if spec.ServiceName == "" || spec.MountPath == "" || len(spec.Environment) == 0 {
			t.Fatalf("BackingCreationSpec(%q) returned an incomplete spec: %#v", test.key, spec)
		}
	}
}

func TestBackingCreationSpecReturnsOwnedSlices(t *testing.T) {
	first, err := adapters.BackingCreationSpec("postgres:16", "")
	if err != nil {
		t.Fatal(err)
	}
	first.Expose[0] = "changed"
	first.Environment[0].Name = "CHANGED"

	second, err := adapters.BackingCreationSpec("postgres:16", "")
	if err != nil {
		t.Fatal(err)
	}
	if second.Expose[0] != "5432" || second.Environment[0].Name != "POSTGRES_DB" {
		t.Fatalf("creation spec leaked mutable adapter state: %#v", second)
	}
}
