package catalog

import (
	"strings"
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
		ConfigVariant:  "test-router-v1",
		Provides:       []component.Capability{component.CapabilityHTTPRouter},
		OwnerScopes:    []component.OwnerScope{component.OwnerScopeEnvironment},
		Actions:        []component.ActionDefinition{action},
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

func TestManagedConfigRecipeSealsPinnedValidator(t *testing.T) {
	t.Parallel()
	image := testOCIImage("example/resolver", "a")
	recipe, err := NewManagedConfigActionRecipe(
		"activate-config", "resolver/config", []string{"-conf", "/dev/stdin", "-dns.port", "0"}, image,
	)
	if err != nil {
		t.Fatalf("NewManagedConfigActionRecipe() error = %v", err)
	}
	arguments := recipe.ValidateArgs()
	arguments[0] = "changed"
	if !sameOCIImage(recipe.Image(), image) || recipe.ValidateArgs()[0] != "-conf" {
		t.Fatal("managed-config recipe did not retain immutable validator inputs")
	}
	if _, err := NewManagedConfigActionRecipe(
		"activate-config", "resolver/config", []string{"-conf", "/dev/stdin"}, component.OCIImage{},
	); err == nil {
		t.Fatal("NewManagedConfigActionRecipe() accepted a mutable validator image")
	}
}

// Rationale: observation is a catalog-selected closed capability recipe; no
// Controller-provided command or Compose healthcheck may become its authority.
func TestDNSResolverObservationRecipeClosesAgentLocalProofInputs(t *testing.T) {
	t.Parallel()
	image := testOCIImage("example/resolver", "b")
	recipe, err := NewDNSResolverObservationRecipe(
		"observe-serving", "resolver", "/etc/resolver/config", image,
		"127.0.0.1:53", "http://127.0.0.1:9153/metrics", "resolver_reload_version_info",
	)
	if err != nil {
		t.Fatalf("NewDNSResolverObservationRecipe() error = %v", err)
	}
	if recipe.ActionID() != "observe-serving" || recipe.ServiceName() != "resolver" ||
		recipe.ArtifactTarget() != "/etc/resolver/config" || !sameOCIImage(recipe.Image(), image) ||
		recipe.ListenEndpoint() != "127.0.0.1:53" ||
		recipe.MetricsURL() != "http://127.0.0.1:9153/metrics" ||
		recipe.ReloadMetric() != "resolver_reload_version_info" {
		t.Fatal("DNS resolver observation recipe changed its closed Agent-local proof inputs")
	}
}

func sameOCIImage(left, right component.OCIImage) bool {
	if left.Repository != right.Repository || left.IndexDigest != right.IndexDigest ||
		len(left.Platforms) != len(right.Platforms) {
		return false
	}
	for index, platform := range left.Platforms {
		if platform != right.Platforms[index] {
			return false
		}
	}
	return true
}

func testOCIImage(repository, digestCharacter string) component.OCIImage {
	return component.OCIImage{
		Repository: repository, IndexDigest: strings.Repeat(digestCharacter, 64),
		Platforms: []component.OCIPlatform{
			{OS: "linux", Architecture: "amd64", ChildDigest: strings.Repeat("c", 64)},
			{OS: "linux", Architecture: "arm64", Variant: "v8", ChildDigest: strings.Repeat("d", 64)},
		},
	}
}
