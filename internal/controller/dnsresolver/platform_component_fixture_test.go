package dnsresolver

import (
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
)

func testResolverDefinition(t *testing.T) componentsdk.Definition {
	t.Helper()
	httpRouter, err := componentsdk.NewGrant(
		componentsdk.CapabilityHTTPRouter,
		componentsdk.OperationList,
		componentsdk.OperationRead,
	)
	if err != nil {
		t.Fatalf("create HTTP-router grant: %v", err)
	}
	services, err := componentsdk.NewGrant(
		componentsdk.CapabilityServices,
		componentsdk.OperationRead,
		componentsdk.OperationCreate,
	)
	if err != nil {
		t.Fatalf("create Services grant: %v", err)
	}
	managedConfig, err := componentsdk.NewGrant(
		componentsdk.CapabilityManagedConfig,
		componentsdk.OperationConfigure,
		componentsdk.OperationActivate,
	)
	if err != nil {
		t.Fatalf("create managed-config grant: %v", err)
	}
	hostResolution, err := componentsdk.NewGrant(
		componentsdk.CapabilityHostResolution,
		componentsdk.OperationConfigure,
		componentsdk.OperationObserve,
	)
	if err != nil {
		t.Fatalf("create host-resolution grant: %v", err)
	}
	activate, err := componentsdk.NewActionDefinition(
		"activate-config",
		componentsdk.CapabilityManagedConfig,
		componentsdk.OperationActivate,
	)
	if err != nil {
		t.Fatalf("create config-activation action: %v", err)
	}
	observe, err := componentsdk.NewActionDefinition(
		"observe-serving",
		componentsdk.CapabilityHostResolution,
		componentsdk.OperationObserve,
	)
	if err != nil {
		t.Fatalf("create serving-observation action: %v", err)
	}
	definition, err := componentsdk.NewDefinition(componentsdk.DefinitionInput{
		Implementation: "test-resolver",
		ConfigVariant:  "test-resolver-v1",
		Provides:       []componentsdk.Capability{componentsdk.CapabilityDNSResolver},
		Grants:         []componentsdk.Grant{httpRouter, services, managedConfig, hostResolution},
		OwnerScopes:    []componentsdk.OwnerScope{componentsdk.OwnerScopePlatform},
		Actions:        []componentsdk.ActionDefinition{activate, observe},
	})
	if err != nil {
		t.Fatalf("create resolver definition: %v", err)
	}
	return definition
}
