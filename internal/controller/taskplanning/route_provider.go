package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type routeProviderStateReader interface {
	SnapshotRevision(context.Context) (int64, error)
	ListRoutes(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[routerecord.Record], error)
	ListServices(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[servicerecord.ServiceRecord], error)
	ListZones(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[zonerecord.Record], error)
}

// ResolveComponentTaskRouteProvider pins the registered HTTP router input for
// a Component lifecycle Task. The boolean is true only when that Task changes
// the provider, allowing nil Provider to mean a deliberate disable projection.
func ResolveComponentTaskRouteProvider(
	catalog []componentrender.EnvironmentComponentRegistration,
	environment core.Environment,
	components []componentrecord.Record,
	candidates []etcd.ComponentTaskCandidate,
	inputRevision int64,
	inputGeneration uint64,
) (*etcd.RouteProviderPin, bool, error) {
	if err := componentrender.ValidateEnvironmentComponentCatalog(catalog); err != nil {
		return nil, false, err
	}
	changesProvider := false
	for _, candidate := range candidates {
		registration, found := environmentComponentRegistration(catalog, candidate.Candidate.Desired.Kind)
		changesProvider = changesProvider || found && registration.ProjectHTTPRouter != nil
	}
	if !changesProvider {
		return nil, false, nil
	}
	var selected *etcd.RouteProviderPin
	for _, record := range components {
		if !record.Desired.Enabled {
			continue
		}
		registration, found := environmentComponentRegistration(catalog, record.Desired.Kind)
		if !found || registration.ProjectHTTPRouter == nil {
			continue
		}
		if selected != nil {
			return nil, false, errs.New(
				errs.KindStateConflict,
				"Environment has multiple enabled HTTP router providers",
			)
		}
		component, err := componentrecord.ProjectRecord(record)
		if err != nil {
			return nil, false, err
		}
		input, err := registration.ProjectHTTPRouter(environment, component)
		if err != nil {
			return nil, false, err
		}
		destination, actionID, found := componentrender.ManagedConfigurationIdentity(registration)
		if !found || inputRevision <= 0 || inputGeneration == 0 {
			return nil, false, errs.New(errs.KindInternal, "HTTP router Component lifecycle pin is incomplete")
		}
		definitionDigest := registration.Definition.Digest()
		pin := etcd.RouteProviderPin{
			ComponentID:      record.Desired.ID,
			DefinitionDigest: hex.EncodeToString(definitionDigest[:]),
			CatalogDigest:    hex.EncodeToString(registration.CatalogDigest[:]),
			InputRevision:    inputRevision, InputGeneration: inputGeneration,
			Destination: destination, ActionID: string(actionID),
			ServiceID: input.GeneratedServiceID, Input: input,
		}
		selected = &pin
	}
	return selected, true, nil
}

func (resolver *TaskPlanResolver) pinRouteProvider(
	ctx context.Context,
	environmentID string,
	projectionRevision int64,
	projection projectionrecord.EnvironmentComposeProjection,
	desired *routerecord.Record,
	removedRouteID string,
	inputGeneration uint64,
) (*etcd.RouteProviderPin, error) {
	if resolver == nil {
		return nil, errs.New(errs.KindInternal, "Route plan state reader is unavailable")
	}
	registration, component, found, err := resolver.routeProviderRegistration(projection)
	if err != nil || !found {
		return nil, err
	}
	environment, inputRevision, err := resolver.routeProviderEnvironment(
		ctx,
		environmentID,
		projectionRevision,
		projection,
		desired,
		removedRouteID,
	)
	if err != nil {
		return nil, err
	}
	input, err := registration.ProjectHTTPRouter(environment, component)
	if err != nil {
		return nil, err
	}
	input = componentsdk.CloneHTTPRouterInput(input)
	destination, actionID, managed := componentrender.ManagedConfigurationIdentity(registration)
	if componentsdk.ValidateHTTPRouterInput(input) != nil || input.ComponentID != component.ID ||
		len(component.GeneratedServices) != 1 || !managed {
		return nil, errs.New(errs.KindStateConflict, "registered HTTP router input is invalid")
	}
	if projectionRevision <= 0 || projectionRevision > inputRevision {
		return nil, errs.New(errs.KindStateConflict, "Route provider projection is newer than its fixed snapshot")
	}
	definitionDigest := registration.Definition.Digest()
	return &etcd.RouteProviderPin{
		ComponentID:      component.ID,
		DefinitionDigest: hex.EncodeToString(definitionDigest[:]),
		CatalogDigest:    hex.EncodeToString(registration.CatalogDigest[:]),
		InputRevision:    inputRevision, InputGeneration: inputGeneration,
		Destination: destination,
		ActionID:    string(actionID),
		ServiceID:   component.GeneratedServices[0], Input: input,
	}, nil
}

func (resolver *TaskPlanResolver) routeProviderRegistration(
	projection projectionrecord.EnvironmentComposeProjection,
) (componentrender.EnvironmentComponentRegistration, core.Component, bool, error) {
	var selected componentrender.EnvironmentComponentRegistration
	var component core.Component
	found := false
	for _, record := range projection.Components {
		candidate, err := componentrecord.ProjectRecord(record)
		if err != nil {
			return componentrender.EnvironmentComponentRegistration{}, core.Component{}, false, err
		}
		if !candidate.Enabled {
			continue
		}
		for _, registration := range resolver.componentCatalog {
			if registration.Kind != candidate.Kind || registration.ProjectHTTPRouter == nil ||
				registration.PlanHTTPRouter == nil || !definitionProvides(registration.Definition, componentsdk.CapabilityHTTPRouter) {
				continue
			}
			if found {
				return componentrender.EnvironmentComponentRegistration{}, core.Component{}, false, errs.New(
					errs.KindStateConflict,
					"Environment has multiple enabled HTTP router providers",
				)
			}
			selected, component, found = registration, candidate, true
		}
	}
	return selected, component, found, nil
}

func definitionProvides(definition componentsdk.Definition, capability componentsdk.Capability) bool {
	for _, provided := range definition.Provides() {
		if provided == capability {
			return true
		}
	}
	return false
}

func (resolver *TaskPlanResolver) routeProviderEnvironment(
	ctx context.Context,
	environmentID string,
	projectionRevision int64,
	projection projectionrecord.EnvironmentComposeProjection,
	desired *routerecord.Record,
	removedRouteID string,
) (core.Environment, int64, error) {
	if projection.EnvironmentID != environmentID || projectionRevision <= 0 {
		return core.Environment{}, 0, errs.New(errs.KindStateConflict, "Route provider desired projection is invalid")
	}
	environment := core.Environment{
		ID:       environmentID,
		Zones:    map[string]core.Zone{},
		Services: map[string]core.Service{},
	}
	for _, record := range projection.Components {
		component, err := componentrecord.ProjectRecord(record)
		if err != nil {
			return core.Environment{}, 0, err
		}
		environment.Components = append(environment.Components, component)
	}
	for _, stored := range projection.DesiredZones {
		environment.Zones[stored.Desired.Name] = stored.Desired
	}
	for _, stored := range projection.DesiredServices {
		environment.Services[stored.Desired.Name] = stored.Desired
	}
	foundDesired := false
	for _, stored := range projection.DesiredRoutes {
		if stored.Desired.ID == removedRouteID {
			continue
		}
		route := stored.Desired
		if desired != nil && route.ID == desired.Desired.ID {
			route = desired.Desired
			foundDesired = true
		}
		environment.Routes = append(environment.Routes, route)
	}
	if desired != nil && !foundDesired {
		environment.Routes = append(environment.Routes, desired.Desired)
	}
	return environment, projectionRevision, nil
}

func (resolver *TaskPlanResolver) renderPinnedRouteProvider(
	pin etcd.RouteProviderPin,
	projection projectionrecord.EnvironmentComposeProjection,
) (componentsdk.EnvironmentPlan, core.Component, [sha256.Size]byte, [sha256.Size]byte, error) {
	definitionBytes, err := hex.DecodeString(pin.DefinitionDigest)
	if err != nil || len(definitionBytes) != sha256.Size {
		return componentsdk.EnvironmentPlan{}, core.Component{}, [sha256.Size]byte{}, [sha256.Size]byte{}, errs.New(
			errs.KindInternal,
			"Route provider definition digest is invalid",
		)
	}
	catalogBytes, err := hex.DecodeString(pin.CatalogDigest)
	if err != nil || len(catalogBytes) != sha256.Size {
		return componentsdk.EnvironmentPlan{}, core.Component{}, [sha256.Size]byte{}, [sha256.Size]byte{}, errs.New(
			errs.KindInternal,
			"Route provider catalog digest is invalid",
		)
	}
	var definitionDigest, catalogDigest [sha256.Size]byte
	copy(definitionDigest[:], definitionBytes)
	copy(catalogDigest[:], catalogBytes)
	for _, registration := range resolver.componentCatalog {
		if registration.Definition.Digest() != definitionDigest || registration.CatalogDigest != catalogDigest {
			continue
		}
		for _, record := range projection.Components {
			if record.Desired.ID != pin.ComponentID {
				continue
			}
			component, projectErr := componentrecord.ProjectRecord(record)
			if projectErr != nil || component.Kind != registration.Kind || !component.Enabled ||
				registration.PlanHTTPRouter == nil {
				return componentsdk.EnvironmentPlan{}, core.Component{}, [sha256.Size]byte{}, [sha256.Size]byte{}, errs.New(
					errs.KindStateConflict,
					"pinned Route provider is unavailable",
				)
			}
			plan, planErr := registration.PlanHTTPRouter(componentsdk.CloneHTTPRouterInput(pin.Input), component)
			return componentsdk.CloneEnvironmentPlan(plan), component, definitionDigest, catalogDigest, planErr
		}
	}
	return componentsdk.EnvironmentPlan{}, core.Component{}, [sha256.Size]byte{}, [sha256.Size]byte{}, errs.New(
		errs.KindStateConflict,
		"pinned Route provider is not registered",
	)
}
