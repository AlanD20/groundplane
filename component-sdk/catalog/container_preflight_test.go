package catalog

import (
	"testing"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

// QA: CMP-04, HTTP-05; pure recipe validation and catalog-digest proof only, not native Caddy validation.
// Rationale: the early stdin validator is execution authority and cannot be
// omitted, mutated through an input slice, or changed without a new catalog.
func TestContainerConfigRecipeRequiresAndSealsPreflight(t *testing.T) {
	image := testOCIImage("example/router", "a")
	makeRecipe := func(args []string) (ContainerConfigActionRecipe, error) {
		return NewContainerConfigActionRecipe("activate-config", "router/config", "/config/file",
			args, []string{"validate", "/config/file"}, []string{"reload", "/config/file"}, image)
	}
	if _, err := makeRecipe(nil); err == nil {
		t.Fatal("missing preflight command was accepted")
	}
	arguments := []string{"validate", "-"}
	recipe, err := makeRecipe(arguments)
	if err != nil {
		t.Fatal(err)
	}
	arguments[0] = "mutated"
	returned := recipe.PreflightArgs()
	returned[0] = "mutated"
	if recipe.PreflightArgs()[0] != "validate" {
		t.Fatal("preflight command was not copied")
	}
	action, err := component.NewActionDefinition(
		"activate-config",
		component.CapabilityManagedConfig,
		component.OperationActivate,
	)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := component.NewDefinition(component.DefinitionInput{
		Implementation: "test-router", ConfigVariant: "test-router-v1",
		Provides: []component.Capability{component.CapabilityHTTPRouter},
		OwnerScopes: []component.OwnerScope{
			component.OwnerScopeEnvironment,
		}, Actions: []component.ActionDefinition{action},
	})
	if err != nil {
		t.Fatal(err)
	}
	base, err := NewRegistered(Registration{Definition: definition, Images: []component.OCIImage{image},
		ContainerConfigActions: []ContainerConfigActionRecipe{recipe}})
	if err != nil {
		t.Fatal(err)
	}
	changedRecipe, err := makeRecipe([]string{"validate-changed", "-"})
	if err != nil {
		t.Fatal(err)
	}
	changed, err := NewRegistered(Registration{Definition: definition, Images: []component.OCIImage{image},
		ContainerConfigActions: []ContainerConfigActionRecipe{changedRecipe}})
	if err != nil {
		t.Fatal(err)
	}
	if base.Digest() == changed.Digest() {
		t.Fatal("catalog digest does not bind the preflight command")
	}
}
