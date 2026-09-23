package blueprint

import (
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"

	"github.com/AlanD20/groundplane/internal/core"

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

func requireExplicitBlueprintVolumes(
	project *composetypes.Project,
	volumes []projectionrecord.EnvironmentVolumeIdentity,
) error {
	if project == nil {
		return errs.New(errs.KindInternal, "Blueprint Compose project is missing")
	}
	for _, volume := range volumes {
		if _, included := project.Volumes[volume.Key]; !included {
			return errs.New(
				errs.KindResourceInUse,
				"Blueprint omits an existing Volume; use its protected Remove action first",
			)
		}
	}
	return nil
}

func preserveEnvironmentBlueprintServiceExtensions(
	submitted map[string]core.ServiceExtensionSpec,
	submittedServiceNames map[string]struct{},
	previous []composeidentity.Resource,
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
	previous composeidentity.Snapshot,
	previousVolumes []projectionrecord.EnvironmentVolumeIdentity,
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
