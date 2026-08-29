package catalog

import (
	"testing"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

// Rationale: action ids are scoped to one immutable implementation definition
// and must never resolve through another Component registration.
func TestFindActionResolvesWithinImplementation(t *testing.T) {
	action, err := component.NewActionDefinition(
		"activate-config",
		component.CapabilityManagedConfig,
		component.OperationActivate,
	)
	if err != nil {
		t.Fatalf("NewActionDefinition() error = %v", err)
	}
	definition, err := component.NewDefinition(component.DefinitionInput{
		Implementation: "test-router",
		ConfigVariant:   "test-router-v1",
		Provides:        []component.Capability{component.CapabilityHTTPRouter},
		OwnerScopes:     []component.OwnerScope{component.OwnerScopeEnvironment},
		Actions:         []component.ActionDefinition{action},
	})
	if err != nil {
		t.Fatalf("NewDefinition() error = %v", err)
	}
	catalog, err := New(definition)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	gotDefinition, gotAction, found := catalog.FindAction("test-router", "activate-config")
	if !found || gotDefinition.Implementation() != "test-router" || gotAction.ID() != "activate-config" {
		t.Fatal("FindAction() did not resolve the registered action")
	}
	if _, _, found := catalog.FindAction("test-router", "missing"); found {
		t.Fatal("FindAction() resolved an unknown action")
	}
}
