package blueprint

import (
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func environmentBlueprintServiceNames(project *composetypes.Project) map[string]struct{} {
	names := make(map[string]struct{}, len(project.Services)+len(project.DisabledServices))
	for name := range project.Services {
		names[name] = struct{}{}
	}
	for name := range project.DisabledServices {
		names[name] = struct{}{}
	}
	return names
}

func preserveEnvironmentBlueprintServiceExtensions(
	submitted map[string]core.ServiceExtensionSpec,
	submittedServiceNames map[string]struct{},
	previous []controller.ComposeResourceIdentity,
	previousExtensions map[string]core.ServiceExtensionSpec,
) (map[string]core.ServiceExtensionSpec, error) {
	result := cloneEnvironmentBlueprintServiceExtensions(submitted)
	for _, identity := range previous {
		if _, submittedNow := submittedServiceNames[identity.Name]; submittedNow {
			continue
		}
		if extension, exists := previousExtensions[identity.Name]; exists {
			result[identity.Name] = core.CloneServiceExtension(extension)
		}
	}
	return result, nil
}

func cloneEnvironmentBlueprintServiceExtensions(
	source map[string]core.ServiceExtensionSpec,
) map[string]core.ServiceExtensionSpec {
	result := make(map[string]core.ServiceExtensionSpec, len(source))
	for name, extension := range source {
		result[name] = core.CloneServiceExtension(extension)
	}
	return result
}

func preserveEnvironmentBlueprintResources(
	project *composetypes.Project,
	prior *composetypes.Project,
	previous controller.ComposeIdentitySnapshot,
	previousVolumes []etcd.EnvironmentVolumeIdentity,
	hasPrevious bool,
) error {
	if project == nil || (hasPrevious && prior == nil) {
		return errs.New(errs.KindInternal, "Blueprint Compose project is missing")
	}
	if !hasPrevious {
		return nil
	}
	if project.Services == nil {
		project.Services = make(composetypes.Services, len(previous.Services))
	}
	for _, identity := range previous.Services {
		if _, authored := project.Services[identity.Name]; authored {
			continue
		}
		if _, disabled := project.DisabledServices[identity.Name]; disabled {
			continue
		}
		config, active := prior.Services[identity.Name]
		disabled, profileDisabled := prior.DisabledServices[identity.Name]
		if active && profileDisabled {
			return errs.New(errs.KindResourceInUse, "Blueprint cannot unambiguously preserve an omitted Service")
		}
		if active {
			config.Name = identity.Name
			project.Services[identity.Name] = config
			continue
		}
		if profileDisabled {
			if project.DisabledServices == nil {
				project.DisabledServices = make(composetypes.Services, len(previous.Services))
			}
			disabled.Name = identity.Name
			project.DisabledServices[identity.Name] = disabled
			continue
		}
		return errs.New(errs.KindResourceInUse, "Blueprint cannot preserve an omitted Service")
	}
	if project.Networks == nil {
		project.Networks = make(composetypes.Networks, len(previous.Networks))
	}
	for _, identity := range previous.Networks {
		if _, authored := project.Networks[identity.Name]; authored {
			continue
		}
		network, found := prior.Networks[identity.Name]
		if !found {
			return errs.New(errs.KindResourceInUse, "Blueprint cannot preserve an omitted Zone")
		}
		project.Networks[identity.Name] = network
	}
	if len(prior.Configs) != 0 {
		if project.Configs == nil {
			project.Configs = make(composetypes.Configs, len(prior.Configs))
		}
		for name, config := range prior.Configs {
			if _, authored := project.Configs[name]; !authored {
				project.Configs[name] = config
			}
		}
	}
	if len(prior.Secrets) != 0 {
		if project.Secrets == nil {
			project.Secrets = make(composetypes.Secrets, len(prior.Secrets))
		}
		for name, secret := range prior.Secrets {
			if _, authored := project.Secrets[name]; !authored {
				project.Secrets[name] = secret
			}
		}
	}
	if project.Volumes == nil {
		project.Volumes = make(composetypes.Volumes, len(previousVolumes))
	}
	for _, volume := range previousVolumes {
		if _, authored := project.Volumes[volume.Key]; authored {
			continue
		}
		config, found := prior.Volumes[volume.Key]
		if !found {
			return errs.New(errs.KindResourceInUse, "Blueprint cannot preserve an omitted Volume")
		}
		if config.Extensions == nil {
			config.Extensions = make(composetypes.Extensions)
		}
		config.Extensions["x-gp-slug"] = volume.Slug
		project.Volumes[volume.Key] = config
	}
	return nil
}

func preserveEnvironmentBlueprintRoutes(
	specs []core.RouteSpec,
	services []core.Service,
	current []etcd.Versioned[etcd.RouteRecord],
) ([]core.RouteSpec, error) {
	serviceByID := make(map[string]string, len(services))
	for _, service := range services {
		serviceByID[service.ID] = service.Name
	}
	retained := make(map[string]struct{}, len(specs)+len(current))
	for _, spec := range specs {
		path := spec.Path
		if path == "" {
			path = "/"
		}
		retained[spec.Hostname+"\x00"+path] = struct{}{}
	}
	result := append([]core.RouteSpec(nil), specs...)
	for _, versioned := range current {
		route := versioned.Record.Desired
		serviceName, exists := serviceByID[route.TargetServiceID]
		if !exists {
			return nil, errs.New(errs.KindInternal, "durable Route target Service is not retained")
		}
		path := route.Path
		if path == "" {
			path = "/"
		}
		match := route.Host + "\x00" + path
		if _, authored := retained[match]; authored {
			continue
		}
		result = append(result, core.RouteSpec{
			Hostname: route.Host, Path: path, Target: serviceName,
			TargetPort: route.TargetPort, Exposure: route.Exposure,
		})
		retained[match] = struct{}{}
	}
	return result, nil
}
