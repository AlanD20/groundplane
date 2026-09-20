package blueprint

import (
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"

	"github.com/AlanD20/groundplane/internal/core"
)

// buildEnvironmentDependencyPlans freezes release phase edges at Blueprint apply.
func buildEnvironmentDependencyPlans(
	services []composeidentity.Resource,
	extensions map[string]core.ServiceExtensionSpec,
) (core.ServiceDependencyPlans, error) {
	names := make([]string, len(services))
	for index, service := range services {
		names[index] = service.Name
	}
	deploy, err := core.BuildServiceDependencyPhasePlan(names, extensions, core.ServiceLifecycleDeploy)
	if err != nil {
		return core.ServiceDependencyPlans{}, err
	}
	rollback, err := core.BuildServiceDependencyPhasePlan(names, extensions, core.ServiceLifecycleRollback)
	if err != nil {
		return core.ServiceDependencyPlans{}, err
	}
	return core.ServiceDependencyPlans{
		DeployDependencyPlan:   deploy,
		RollbackDependencyPlan: rollback,
	}, nil
}
