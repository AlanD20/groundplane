package environmentqueries

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/zonedns"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/core/composeinput"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/compose-spec/compose-go/v2/loader"
	"github.com/compose-spec/compose-go/v2/types"
)

// DeclaredResources is a query view, not sealed execution input. An accepted
// Blueprint stays inspectable while its fact-dependent runtime is unavailable.
type DeclaredResources struct {
	EnvironmentID   string
	DesiredServices []services.EnvironmentServiceProjection
	DesiredZones    []environmentprojection.EnvironmentZoneProjection
	DesiredRoutes   []environmentprojection.EnvironmentRouteProjection
}

// ReadDeclaredResources joins current authored resources at one MVCC revision.
// It never substitutes a previous runtime or supplies authority for execution.
func ReadDeclaredResources(
	ctx context.Context,
	store snapshotReader,
	environmentID string,
	revision int64,
) (keyvalue.Versioned[DeclaredResources], bool, error) {
	input, found, err := blueprints.ReadCurrentDesiredInput(ctx, store, environmentID, revision)
	if err != nil || !found {
		return keyvalue.Versioned[DeclaredResources]{ReadRevision: input.ReadRevision}, found, err
	}
	owned, found, err := blueprints.ReadCurrentOwnedIdentities(ctx, store, environmentID, input.ReadRevision)
	if err != nil {
		return keyvalue.Versioned[DeclaredResources]{}, false, err
	}
	if !found || owned.Revision != input.Revision || owned.Record.RevisionID != input.Record.RevisionID ||
		owned.Record.RenderGeneration != input.Record.RenderGeneration ||
		owned.Record.EnvironmentID != input.Record.EnvironmentID {
		return keyvalue.Versioned[DeclaredResources]{}, false, errs.New(
			errs.KindInternal,
			"Environment declared resource identities are inconsistent",
		)
	}
	// A rendered view of this same head carries adapter bindings and per-resource
	// observation generations. Pending heads use declarations, never an older render.
	runtime, available, err := blueprints.ReadAvailableRuntimeRevision(
		ctx,
		store,
		environmentID,
		input.Record.RevisionID,
		input.ReadRevision,
	)
	if err != nil {
		return keyvalue.Versioned[DeclaredResources]{}, false, err
	}
	if available {
		runtime.Record.DesiredServices = OrdinaryServices(runtime.Record)
		return keyvalue.Versioned[DeclaredResources]{
			Record: DeclaredResources{
				EnvironmentID:   environmentID,
				DesiredServices: ServicesWithVolumeMounts(runtime.Record),
				DesiredZones:    runtime.Record.DesiredZones,
				DesiredRoutes:   runtime.Record.DesiredRoutes,
			},
			Revision:     input.Revision,
			ReadRevision: input.ReadRevision,
		}, true, nil
	}
	project, err := loader.LoadWithContext(
		ctx,
		types.ConfigDetails{
			WorkingDir:  "/",
			ConfigFiles: []types.ConfigFile{{Filename: "compose.yaml", Content: input.Record.Input.NormalizedCompose}},
		},
		func(options *loader.Options) {
			options.ResolvePaths = false
			options.SkipInterpolation = true
			options.SkipNormalization = true
			options.SkipResolveEnvironment = true
			options.SetProjectName("gp-"+strings.ToLower(environmentID), true)
		},
	)
	if err != nil {
		return keyvalue.Versioned[DeclaredResources]{}, false, errs.Wrap(errs.KindInternal, err)
	}
	view, err := declaredResources(input.Record, owned.Record, project)
	return keyvalue.Versioned[DeclaredResources]{
		Record:       view,
		Revision:     input.Revision,
		ReadRevision: input.ReadRevision,
	}, err == nil, err
}

func declaredResources(
	input environmentprojection.EnvironmentDesiredInput,
	owned environmentprojection.EnvironmentOwnedIdentities,
	project *types.Project,
) (DeclaredResources, error) {
	view := DeclaredResources{EnvironmentID: input.EnvironmentID}
	serviceIDs := make(map[string]string, len(owned.Services))
	volumeIDs := make(map[string]string, len(owned.Volumes))
	for _, volume := range owned.Volumes {
		volumeIDs[volume.Name] = volume.ID
	}
	for _, identity := range owned.Services {
		config, exists := project.Services[identity.Name]
		if !exists {
			config, exists = project.DisabledServices[identity.Name]
		}
		// Component-generated Services have identity records but no operator-authored
		// Compose definition. They are shown through their Component, not this list.
		if !exists {
			continue
		}
		service, err := composeinput.Service(
			identity.ID,
			identity.Name,
			config,
			input.Input.ServiceExtensions[identity.Name],
		)
		if err != nil {
			return DeclaredResources{}, err
		}
		for _, mount := range config.Volumes {
			if volumeID, owned := volumeIDs[mount.Source]; owned && mount.Type == types.VolumeTypeVolume {
				service.Mounts = append(
					service.Mounts,
					core.Mount{Volume: volumeID, Mount: mount.Target, RO: mount.ReadOnly},
				)
			}
		}
		view.DesiredServices = append(
			view.DesiredServices,
			services.EnvironmentServiceProjection{EnvironmentID: input.EnvironmentID, Desired: service},
		)
		serviceIDs[identity.Name] = identity.ID
	}
	for _, identity := range owned.Networks {
		network, exists := project.Networks[identity.Name]
		if !exists || len(network.Ipam.Config) != 1 || network.Ipam.Config[0] == nil {
			return DeclaredResources{}, errs.New(errs.KindInternal, "Declared Zone has no authored subnet")
		}
		allowDNS, err := zonedns.Enabled(network)
		if err != nil {
			return DeclaredResources{}, err
		}
		zone := core.Zone{
			ID:         identity.ID,
			Name:       identity.Name,
			Subnet:     network.Ipam.Config[0].Subnet,
			Internal:   network.Internal,
			AllowGPDNS: allowDNS,
			OwnerKind:  core.ZoneOwnerEnvironment,
			OwnerID:    input.EnvironmentID,
		}
		view.DesiredZones = append(
			view.DesiredZones,
			environmentprojection.EnvironmentZoneProjection{EnvironmentID: input.EnvironmentID, Desired: zone},
		)
	}
	routeIDs := make(map[string]string, len(owned.Routes))
	for _, identity := range owned.Routes {
		routeIDs[identity.Name] = identity.ID
	}
	for _, spec := range input.Input.Routes {
		path := spec.Path
		if path == "" {
			path = "/"
		}
		route := core.Route{
			ID:              routeIDs[environmentprojection.RouteIdentityName(spec.Hostname, path)],
			Host:            spec.Hostname,
			Path:            path,
			TargetServiceID: serviceIDs[spec.Target],
			TargetPort:      spec.TargetPort,
			Exposure:        spec.Exposure,
		}
		if err := route.Validate(); err != nil {
			return DeclaredResources{}, errs.Wrap(errs.KindInternal, err)
		}
		view.DesiredRoutes = append(
			view.DesiredRoutes,
			environmentprojection.EnvironmentRouteProjection{
				EnvironmentID:     input.EnvironmentID,
				Desired:           route,
				DesiredGeneration: input.RenderGeneration,
			},
		)
	}
	return view, nil
}
