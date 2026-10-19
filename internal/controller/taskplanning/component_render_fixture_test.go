package taskplanning

import (
	"crypto/sha256"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/core"
)

func testSelectedComponentImage(repository string) composeidentity.ComponentImage {
	image, platform, reference := controllerTestOCIPlatform(repository)
	return composeidentity.ComponentImage{
		Repository: repository, IndexDigest: image.IndexDigest, Reference: reference, Platform: platform,
	}
}

func componentTestRegistration(
	t *testing.T, kind core.ComponentKind, plan componentrender.EnvironmentComponentPlanFunc,
) componentrender.EnvironmentComponentRegistration {
	t.Helper()
	action, err := componentsdk.NewActionDefinition(
		componentsdk.ActionID("activate-config"), componentsdk.CapabilityManagedConfig, componentsdk.OperationActivate,
	)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := componentsdk.NewDefinition(componentsdk.DefinitionInput{
		Implementation: componentsdk.ImplementationKey(kind), ConfigVariant: componentsdk.ConfigVariant("test"),
		Provides:    []componentsdk.Capability{componentsdk.CapabilityServices},
		OwnerScopes: []componentsdk.OwnerScope{componentsdk.OwnerScopeEnvironment},
		Actions:     []componentsdk.ActionDefinition{action},
	})
	if err != nil {
		t.Fatal(err)
	}
	return componentrender.EnvironmentComponentRegistration{
		Kind: kind, Definition: definition,
		CatalogDigest: sha256.Sum256([]byte("test Component catalog:" + string(kind))), Plan: plan,
	}
}
