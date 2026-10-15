package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/core/composeinput"
	"sort"

	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Validate and Apply reject the same authored rollout prerequisites before the
// desired head or visible Task exists. This does not assert live image/health.
func validateBlueprintDeploymentSettings(parsed blueprintparser.Result) error {
	names := make([]string, 0, len(parsed.Project.Services))
	for name := range parsed.Project.Services {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		service := parsed.Project.Services[name]
		declared := ""
		if release := parsed.ServiceExtensions[name].Release; release != nil {
			declared = string(release.DefaultStrategy)
		}
		strategy, err := domain.DeploymentStrategy("", declared)
		if err != nil {
			return err
		}
		replicas := service.GetScale()
		if replicas < 1 || uint64(replicas) > uint64(^uint32(0)) {
			return errs.Newf(errs.KindValidationFailed, "Service %q: replica count must be positive", name)
		}
		if err := domain.ValidateDeploymentReadiness(name, strategy, uint32(replicas),
			composeinput.Healthcheck(service.HealthCheck).Enabled(), service.Expose); err != nil {
			return err
		}
		if strategy == domain.StrategyBlueGreen && len(service.Expose) == 0 {
			return errs.Newf(
				errs.KindValidationFailed,
				"Service %q: blue-green requires an exposed TCP port; use rolling or recreate for a portless worker",
				name,
			)
		}
	}
	return nil
}
