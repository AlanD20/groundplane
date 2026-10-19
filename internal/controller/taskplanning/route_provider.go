package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/core"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type routeProviderStateReader interface {
	SnapshotRevision(context.Context) (int64, error)
	GetRouteProviderRuntimeAtRevision(
		context.Context,
		string,
		int64,
	) (etcdstore.Versioned[componentplanning.Runtime], bool, error)
}

// RouteProviderCapture owns immutable router selection and rendering independently
// of Task scheduling and execution.
type RouteProviderCapture struct {
	state   routeProviderStateReader
	catalog []componentrender.EnvironmentComponentRegistration
}

func NewRouteProviderCapture(
	state routeProviderStateReader,
	catalog []componentrender.EnvironmentComponentRegistration,
) (*RouteProviderCapture, error) {
	if state == nil {
		return nil, errs.New(errs.KindInternal, "Route runtime snapshot reader is required")
	}
	if err := componentrender.ValidateEnvironmentComponentCatalog(catalog); err != nil {
		return nil, err
	}
	return &RouteProviderCapture{state: state, catalog: componentrender.CloneEnvironmentComponentCatalog(catalog)}, nil
}

// InactiveKinds identifies every registered router implementation, including
// providers omitted from the current desired projection. Persistence checks
// their retained runtimes before a metadata-only Route decision is accepted.
func (resolver *RouteProviderCapture) InactiveKinds(
	projection projectionrecord.EnvironmentComposeProjection,
) ([]core.ComponentKind, error) {
	if resolver == nil {
		return nil, errs.New(errs.KindInternal, "Route provider catalog is unavailable")
	}
	if _, _, found, err := resolver.routeProviderRegistration(projection); err != nil || found {
		if err != nil {
			return nil, err
		}
		return nil, errs.New(errs.KindStateConflict, "Route changes require execution while a router is enabled")
	}
	var kinds []core.ComponentKind
	for _, registration := range resolver.catalog {
		if definitionProvides(registration.Definition, componentsdk.CapabilityHTTPRouter) {
			kinds = append(kinds, registration.Kind)
		}
	}
	return kinds, nil
}

func (resolver *RouteProviderCapture) Capture(
	ctx context.Context,
	environmentID string,
	projectionRevision int64,
	projection projectionrecord.EnvironmentComposeProjection,
	desired *routerecord.Record,
	removedRouteID string,
	inputGeneration uint64,
) (*environmentchanges.RouteProviderPin, error) {
	if resolver == nil {
		return nil, errs.New(errs.KindInternal, "Route plan state reader is unavailable")
	}
	registration, component, found, err := resolver.routeProviderRegistration(projection)
	if err != nil || !found {
		return nil, err
	}
	environment, inputRevision, err := routeProviderEnvironment(
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
	if resolver.state == nil {
		return nil, errs.New(errs.KindInternal, "Route runtime snapshot reader is unavailable")
	}
	readRevision, err := resolver.state.SnapshotRevision(ctx)
	if err != nil {
		return nil, err
	}
	if readRevision < inputRevision {
		return nil, errs.New(errs.KindStateConflict, "Route runtime snapshot precedes its selected desired input")
	}
	runtime, present, err := resolver.state.GetRouteProviderRuntimeAtRevision(ctx, component.ID, readRevision)
	if err != nil {
		return nil, err
	}
	if runtime.ReadRevision != readRevision {
		return nil, errs.New(errs.KindStateConflict, "Route router runtime was read at a different snapshot")
	}
	retained := &environmentchanges.RouteProviderRuntime{EnvironmentID: environmentID}
	if present {
		if runtime.Revision <= 0 || runtime.Record.EnvironmentID != environmentID ||
			runtime.Record.Component.Desired.ID != component.ID || runtime.Record.Component.Desired.Kind != component.Kind ||
			componentplanning.ValidateRuntime(runtime.Record) != nil {
			return nil, errs.New(errs.KindStateConflict, "Route router runtime belongs to a different provider")
		}
		retained.Revision = runtime.Revision
		retained.Artifact = append([]byte(nil), runtime.Record.Artifact...)
	}
	definitionDigest := registration.Definition.Digest()
	return &environmentchanges.RouteProviderPin{
		ComponentID:      component.ID,
		DefinitionDigest: hex.EncodeToString(definitionDigest[:]),
		CatalogDigest:    hex.EncodeToString(registration.CatalogDigest[:]),
		InputRevision:    readRevision, InputGeneration: inputGeneration,
		Destination: destination,
		ActionID:    string(actionID),
		ServiceID:   component.GeneratedServices[0], Input: input, Runtime: retained,
	}, nil
}

func (resolver *RouteProviderCapture) routeProviderRegistration(
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
		for _, registration := range resolver.catalog {
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

func routeProviderEnvironment(
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

func (resolver *RouteProviderCapture) Render(
	pin environmentchanges.RouteProviderPin,
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
	for _, registration := range resolver.catalog {
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

func (resolver *RouteProviderCapture) Configuration(
	pin environmentchanges.RouteProviderPin,
	projection projectionrecord.EnvironmentComposeProjection,
) ([]byte, error) {
	plan, _, _, _, err := resolver.Render(pin, projection)
	if err != nil {
		return nil, err
	}
	for _, file := range plan.Files {
		if file.Path == pin.Destination {
			return append([]byte(nil), file.Content...), nil
		}
	}
	return nil, errs.New(errs.KindStateConflict, "pinned Route provider did not emit its managed configuration")
}
