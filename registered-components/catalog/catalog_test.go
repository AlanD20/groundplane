package catalog

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane-component-sdk/component"
	registeredtunnel "github.com/AlanD20/groundplane-registered-components/cloudflaretunnel"
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
	for name, value := range map[string]string{"missing": "", "zero": strings.Repeat("0", 64)} {
		invalid := cloneOCIImage(image)
		invalid.Platforms[0].ConfigDigest = value
		if _, err := NewManagedConfigActionRecipe(
			"activate-config", "resolver/config", []string{"-conf", "/dev/stdin"}, invalid,
		); err == nil {
			t.Fatalf("NewManagedConfigActionRecipe() accepted %s image config identity", name)
		}
	}
}

// Rationale: the catalog identity must change when only an authenticated
// platform config digest changes.
func TestCatalogDigestBindsImageConfigDigest(t *testing.T) {
	action, err := component.NewActionDefinition(
		"activate-config", component.CapabilityManagedConfig, component.OperationActivate,
	)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := component.NewDefinition(component.DefinitionInput{
		Implementation: "resolver", ConfigVariant: "resolver-v1",
		Provides:    []component.Capability{component.CapabilityHTTPRouter},
		OwnerScopes: []component.OwnerScope{component.OwnerScopeEnvironment},
		Actions:     []component.ActionDefinition{action},
	})
	if err != nil {
		t.Fatal(err)
	}
	image := testOCIImage("example/resolver", "a")
	recipe, err := NewManagedConfigActionRecipe("activate-config", "resolver/config", []string{"verify"}, image)
	if err != nil {
		t.Fatal(err)
	}
	base, err := NewRegistered(
		Registration{
			Definition:           definition,
			Images:               []component.OCIImage{image},
			ManagedConfigActions: []ManagedConfigActionRecipe{recipe},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	image.Platforms[0].ConfigDigest = strings.Repeat("1", 64)
	changedRecipe, err := NewManagedConfigActionRecipe("activate-config", "resolver/config", []string{"verify"}, image)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := NewRegistered(Registration{
		Definition: definition, Images: []component.OCIImage{image}, ManagedConfigActions: []ManagedConfigActionRecipe{changedRecipe},
	})
	if err != nil {
		t.Fatal(err)
	}
	if base.Digest() == changed.Digest() {
		t.Fatal("Catalog.Digest() ignored changed image config identity")
	}
}

func TestRegistrationImagesAreSoleRecipeAuthority(t *testing.T) {
	actionOne, err := component.NewActionDefinition(
		"activate-one",
		component.CapabilityManagedConfig,
		component.OperationActivate,
	)
	if err != nil {
		t.Fatal(err)
	}
	actionTwo, err := component.NewActionDefinition(
		"activate-two",
		component.CapabilityManagedConfig,
		component.OperationActivate,
	)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := component.NewDefinition(component.DefinitionInput{
		Implementation: "resolver", ConfigVariant: "resolver-v1",
		Provides: []component.Capability{component.CapabilityHostResolution},
		Grants:   []component.Grant{}, OwnerScopes: []component.OwnerScope{component.OwnerScopeEnvironment},
		Actions: []component.ActionDefinition{actionOne, actionTwo},
	})
	if err != nil {
		t.Fatal(err)
	}
	image := testOCIImage("example/resolver", "a")
	first, err := NewManagedConfigActionRecipe("activate-one", "resolver/one", []string{"verify"}, image)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewManagedConfigActionRecipe("activate-two", "resolver/two", []string{"verify"}, image)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRegistered(Registration{Definition: definition, ManagedConfigActions: []ManagedConfigActionRecipe{first}}); err == nil {
		t.Fatal("NewRegistered() accepted recipe without registered image")
	}
	if _, err := NewRegistered(Registration{Definition: definition, Images: []component.OCIImage{image, image}}); err == nil {
		t.Fatal("NewRegistered() accepted duplicate image repository")
	}
	if _, err := NewRegistered(Registration{
		Definition: definition, Images: []component.OCIImage{image},
		ManagedConfigActions: []ManagedConfigActionRecipe{first, first},
	}); err == nil {
		t.Fatal("NewRegistered() accepted duplicate recipe")
	}
	mismatch := cloneOCIImage(image)
	mismatch.Platforms[0].ConfigDigest = strings.Repeat("1", 64)
	mismatchRecipe, err := NewManagedConfigActionRecipe("activate-one", "resolver/one", []string{"verify"}, mismatch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRegistered(Registration{
		Definition: definition, Images: []component.OCIImage{image},
		ManagedConfigActions: []ManagedConfigActionRecipe{mismatchRecipe},
	}); err == nil {
		t.Fatal("NewRegistered() accepted recipe image outside registration")
	}
	catalog, err := NewRegistered(Registration{
		Definition: definition, Images: []component.OCIImage{image},
		ManagedConfigActions: []ManagedConfigActionRecipe{first, second},
	})
	if err != nil {
		t.Fatalf("NewRegistered() rejected reused registered image: %v", err)
	}
	unknown := testOCIImage("example/unknown", "b")
	if err := catalog.ValidateEnvironmentPlanImages(definition.Implementation(), component.EnvironmentPlan{
		Services: []component.ManagedService{{Image: unknown}},
	}); err == nil {
		t.Fatal("ValidateEnvironmentPlanImages() accepted unregistered valid image")
	}
	if err := catalog.ValidateEnvironmentPlanImages(definition.Implementation(), component.EnvironmentPlan{
		Services: []component.ManagedService{{Image: image}, {Image: image}},
	}); err != nil {
		t.Fatalf("ValidateEnvironmentPlanImages() rejected registered image: %v", err)
	}
}

func TestCatalogDigestBindsCloudflareTunnelImageWithoutRecipes(t *testing.T) {
	definition, err := registeredtunnel.Definition()
	if err != nil {
		t.Fatal(err)
	}
	build := func(image component.OCIImage) [32]byte {
		compiled, err := NewRegistered(Registration{Definition: definition, Images: []component.OCIImage{image}})
		if err != nil {
			t.Fatal(err)
		}
		return compiled.Digest()
	}
	base := build(registeredtunnel.Image)
	childChanged := cloneOCIImage(registeredtunnel.Image)
	childChanged.Platforms[0].ChildDigest = strings.Repeat("1", 64)
	if base == build(childChanged) {
		t.Fatal("Catalog.Digest() ignored Cloudflare child digest")
	}
	configChanged := cloneOCIImage(registeredtunnel.Image)
	configChanged.Platforms[1].ConfigDigest = strings.Repeat("2", 64)
	if base == build(configChanged) {
		t.Fatal("Catalog.Digest() ignored Cloudflare config digest")
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
			{
				OS:           "linux",
				Architecture: "amd64",
				ChildDigest:  strings.Repeat("c", 64),
				ConfigDigest: strings.Repeat("e", 64),
			},
			{
				OS:           "linux",
				Architecture: "arm64",
				Variant:      "v8",
				ChildDigest:  strings.Repeat("d", 64),
				ConfigDigest: strings.Repeat("f", 64),
			},
		},
	}
}
