package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/compose-spec/compose-go/v2/types"
	"sort"
)

func applyServiceDependencyPhase(
	project *types.Project,
	extensions map[string]core.ServiceExtensionSpec,
	phase core.ServiceLifecyclePhase,
) error {
	if project == nil {
		return errs.New(errs.KindInternal, "Service dependency planning requires a parsed Compose project")
	}
	names := make([]string, 0, len(project.Services)+len(project.DisabledServices))
	for name := range project.Services {
		names = append(names, name)
	}
	for name := range project.DisabledServices {
		names = append(names, name)
	}
	sort.Strings(names)
	plan, err := core.BuildServiceDependencyPhasePlan(names, extensions, phase)
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	for _, edge := range plan.Edges {
		service, enabled := project.Services[edge.Service]
		if !enabled {
			service = project.DisabledServices[edge.Service]
		}
		if service.DependsOn == nil {
			service.DependsOn = make(map[string]types.ServiceDependency)
		}
		dependency := types.ServiceDependency{Condition: edge.Condition.String(), Required: true}
		if current, exists := service.DependsOn[edge.Dependency]; exists {
			if current.Condition != dependency.Condition || current.Required != dependency.Required {
				return errs.New(errs.KindValidationFailed, "Service dependency phase conflicts with native Compose")
			}
		} else {
			service.DependsOn[edge.Dependency] = dependency
		}
		if enabled {
			project.Services[edge.Service] = service
		} else {
			project.DisabledServices[edge.Service] = service
		}
	}
	return nil
}
