package composeinput

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/compose-spec/compose-go/v2/types"
	"sort"
	"strconv"
)

// Service projects authored Compose settings without requiring a rendered runtime.
func Service(id, name string, config types.ServiceConfig, extension core.ServiceExtensionSpec) (core.Service, error) {
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
	dependsOn := make(map[string]core.ServiceDependency, len(config.DependsOn)+len(extension.DependsOn))
	for dependencyName, dependency := range config.DependsOn {
		dependsOn[dependencyName] = core.ServiceDependency{
			Condition: core.ServiceDependencyCondition(dependency.Condition),
		}
	}
	for dependencyName, dependency := range extension.DependsOn {
		dependsOn[dependencyName] = dependency
	}
	if len(dependsOn) == 0 {
		dependsOn = nil
	}
	service := core.Service{
		ID: id, Name: name, Image: config.Image,
		Zones: zones, Command: append([]string(nil), config.Command...),
		Entrypoint: append([]string(nil), config.Entrypoint...),
		WorkingDir: config.WorkingDir, User: config.User, Aliases: aliases,
		DependsOn: dependsOn, Expose: append([]string(nil), config.Expose...),
		Restart: config.Restart, Replicas: config.GetScale(),
		Healthcheck: Healthcheck(config.HealthCheck),
	}
	if config.Logging != nil {
		service.Logging.MaxSize = config.Logging.Options["max-size"]
		if value := config.Logging.Options["max-file"]; value != "" {
			maximum, parseErr := strconv.Atoi(value)
			if parseErr == nil && maximum >= 0 {
				service.Logging.MaxFile = maximum
			}
		}
	}
	if extension.Release != nil {
		service.Strategy = extension.Release.DefaultStrategy
		service.OnFailure = extension.Release.OnFailure
		service.RollingReplicaTimeoutSeconds = extension.Release.RollingReplicaTimeoutSeconds
	}
	if err := service.Healthcheck.Validate(); err != nil {
		return core.Service{}, errs.Newf(errs.KindValidationFailed, "Service %q: %s", name, err.Error())
	}
	if err := service.Validate(); err != nil {
		return core.Service{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	return service, nil
}
