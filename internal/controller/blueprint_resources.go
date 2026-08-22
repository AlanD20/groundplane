package controller

import (
	"sort"

	"github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProjectServiceProjection builds the complete core.Service projection for
// every owned Compose service after stable identities have been reconciled.
// The parsed Compose project remains the lossless render source.
func ProjectServiceProjection(
	project *types.Project,
	identities ComposeIdentitySnapshot,
) ([]core.Service, error) {
	if project == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Service projection requires a parsed Compose project")
	}
	names, err := ownedServiceNames(project)
	if err != nil {
		return nil, err
	}
	serviceIDs, err := indexComposeIdentities(ids.KindService, names, identities.Services)
	if err != nil {
		return nil, err
	}
	services := make([]core.Service, 0, len(names))
	for _, name := range names {
		config, exists := project.Services[name]
		if !exists {
			config = project.DisabledServices[name]
		}
		zones := make([]string, 0, len(config.Networks))
		aliases := make(map[string][]string, len(config.Networks))
		for zoneName, network := range config.Networks {
			zones = append(zones, zoneName)
			if network != nil && len(network.Aliases) != 0 {
				aliases[zoneName] = append([]string(nil), network.Aliases...)
				sort.Strings(aliases[zoneName])
			}
		}
		sort.Strings(zones)
		if len(aliases) == 0 {
			aliases = nil
		}
		dependsOn := make(map[string]core.ServiceDependency, len(config.DependsOn))
		for dependencyName, dependency := range config.DependsOn {
			dependsOn[dependencyName] = core.ServiceDependency{Condition: dependency.Condition}
		}
		if len(dependsOn) == 0 {
			dependsOn = nil
		}
		service := core.Service{
			ID: serviceIDs[name], Name: name, Image: config.Image,
			Zones: zones, Command: append([]string(nil), config.Command...), Aliases: aliases,
			DependsOn: dependsOn, Expose: append([]string(nil), config.Expose...),
			Restart: config.Restart, Replicas: config.GetScale(),
		}
		if err := service.Validate(); err != nil {
			return nil, errs.Wrap(errs.KindValidationFailed, err)
		}
		services = append(services, service)
	}
	return services, nil
}

// RouteIdentity is the durable match-to-id projection required to preserve
// Route identities across Blueprint replacement.
type RouteIdentity struct {
	ID   string
	Host string
	Path string
}

// BlueprintRouteChanges separates the next desired Routes from identities
// that require the explicit Route removal workflow.
type BlueprintRouteChanges struct {
	Current         []core.Route
	RemovedRouteIDs []string
}

// ReconcileBlueprintRoutes resolves authored Service names to stable ids,
// reuses Route ids by immutable host/path match, and reports omissions without
// deleting them implicitly.
func ReconcileBlueprintRoutes(
	specs []core.RouteSpec,
	services []core.Service,
	previous []RouteIdentity,
	allocate func(ids.Kind) string,
) (BlueprintRouteChanges, error) {
	if allocate == nil {
		return BlueprintRouteChanges{}, errs.New(errs.KindInternal, "Blueprint Route id allocator is required")
	}
	servicesByName := make(map[string]core.Service, len(services))
	for _, service := range services {
		if service.Name == "" || ids.Validate(ids.KindService, service.ID) != nil {
			return BlueprintRouteChanges{}, errs.New(errs.KindInternal, "Blueprint Service projection is invalid")
		}
		if _, duplicate := servicesByName[service.Name]; duplicate {
			return BlueprintRouteChanges{}, errs.New(errs.KindInternal, "Blueprint Service projection repeats a name")
		}
		servicesByName[service.Name] = service
	}
	previousByMatch := make(map[string]RouteIdentity, len(previous))
	usedIDs := make(map[string]struct{}, len(previous)+len(specs))
	for _, identity := range previous {
		if ids.Validate(ids.KindRoute, identity.ID) != nil || identity.Path == "" {
			return BlueprintRouteChanges{}, errs.New(errs.KindInternal, "durable Route identity projection is invalid")
		}
		match := routeIdentityMatch(identity.Host, identity.Path)
		if _, duplicate := previousByMatch[match]; duplicate {
			return BlueprintRouteChanges{}, errs.New(
				errs.KindInternal,
				"durable Route identity projection repeats a match",
			)
		}
		if _, duplicate := usedIDs[identity.ID]; duplicate {
			return BlueprintRouteChanges{}, errs.New(
				errs.KindInternal,
				"durable Route identity projection repeats an id",
			)
		}
		previousByMatch[match] = identity
		usedIDs[identity.ID] = struct{}{}
	}
	current := make([]core.Route, 0, len(specs))
	retained := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		path := spec.Path
		if path == "" {
			path = "/"
		}
		match := routeIdentityMatch(spec.Hostname, path)
		if _, duplicate := retained[match]; duplicate {
			return BlueprintRouteChanges{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint repeats a Route host and path",
			)
		}
		target, exists := servicesByName[spec.Target]
		if !exists {
			return BlueprintRouteChanges{}, errs.Newf(
				errs.KindValidationFailed,
				"Blueprint Route target Service %q does not exist",
				spec.Target,
			)
		}
		routeID := ""
		if identity, exists := previousByMatch[match]; exists {
			routeID = identity.ID
		} else {
			routeID = allocate(ids.KindRoute)
			if ids.Validate(ids.KindRoute, routeID) != nil {
				return BlueprintRouteChanges{}, errs.New(
					errs.KindInternal,
					"Blueprint Route allocator returned an invalid id",
				)
			}
			if _, duplicate := usedIDs[routeID]; duplicate {
				return BlueprintRouteChanges{}, errs.New(errs.KindInternal, "Blueprint Route allocator reused an id")
			}
			usedIDs[routeID] = struct{}{}
		}
		route := core.Route{
			ID: routeID, Host: spec.Hostname, Path: path, TargetServiceID: target.ID,
			TargetPort: spec.TargetPort, Exposure: spec.Exposure,
		}
		if err := route.Validate(); err != nil {
			return BlueprintRouteChanges{}, errs.Wrap(errs.KindValidationFailed, err)
		}
		current = append(current, route)
		retained[match] = struct{}{}
	}
	sort.Slice(current, func(i, j int) bool {
		if current[i].Host != current[j].Host {
			return current[i].Host < current[j].Host
		}
		if current[i].Path != current[j].Path {
			return current[i].Path < current[j].Path
		}
		return current[i].ID < current[j].ID
	})
	removed := make([]string, 0, len(previous))
	for match, identity := range previousByMatch {
		if _, keep := retained[match]; !keep {
			removed = append(removed, identity.ID)
		}
	}
	sort.Strings(removed)
	return BlueprintRouteChanges{Current: current, RemovedRouteIDs: removed}, nil
}

func routeIdentityMatch(host string, path string) string {
	return host + "\x00" + path
}
