package dnsresolver

import (
	"context"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SelectResolver selects the sole platform-owned Component after proving that
// the compiled catalog contains a resolver definition with its generic grants.
func (planner *PlatformRenderPlanner) SelectResolver(
	ctx context.Context,
	candidates []etcdstore.Versioned[componentrecord.Record],
) (etcdstore.Versioned[componentrecord.Record], error) {
	if ctx == nil || planner == nil || planner.catalog == nil {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindInternal,
			"dns-resolver selector dependencies are required",
		)
	}
	definition, _, found := planner.catalog.FindActionByCapability(
		componentsdk.CapabilityDNSResolver, planner.managedConfigAction,
	)
	if !found || !definitionProvidesResolverGrants(definition) {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindInternal,
			"registered dns-resolver capability is absent from the compiled catalog",
		)
	}
	var selected etcdstore.Versioned[componentrecord.Record]
	for _, candidate := range candidates {
		if candidate.Record.Desired.Owner != core.ComponentOwnerPlatform || candidate.Record.Desired.OwnerID != "" {
			continue
		}
		if selected.Revision != 0 {
			return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
				errs.KindStateConflict,
				"multiple platform dns-resolver Components are registered",
			)
		}
		selected = candidate
	}
	if selected.Revision <= 0 || selected.ReadRevision <= 0 {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindComponentNotFound,
			"platform dns-resolver Component is not registered",
		)
	}
	return selected, nil
}

func definitionProvidesResolverGrants(definition componentsdk.Definition) bool {
	providesResolver := false
	for _, capability := range definition.Provides() {
		providesResolver = providesResolver || capability == componentsdk.CapabilityDNSResolver
	}
	grants := func(capability componentsdk.Capability, operation componentsdk.Operation) bool {
		for _, grant := range definition.Grants() {
			if grant.Capability() != capability {
				continue
			}
			for _, granted := range grant.Operations() {
				if granted == operation {
					return true
				}
			}
		}
		return false
	}
	return providesResolver && grants(componentsdk.CapabilityManagedConfig, componentsdk.OperationConfigure) &&
		grants(componentsdk.CapabilityManagedConfig, componentsdk.OperationActivate) &&
		grants(componentsdk.CapabilityHostResolution, componentsdk.OperationConfigure) &&
		grants(componentsdk.CapabilityHostResolution, componentsdk.OperationObserve)
}
