package blueprintparser

import (
	"bytes"
	"sort"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/compose-spec/compose-go/v2/types"
	"gopkg.in/yaml.v3"
)

func normalizeServiceExtensions(project *types.Project) (map[string]core.ServiceExtensionSpec, error) {
	result := make(map[string]core.ServiceExtensionSpec)
	serviceNames := make([]string, 0, len(project.Services)+len(project.DisabledServices))
	for name := range project.Services {
		serviceNames = append(serviceNames, name)
	}
	for name := range project.DisabledServices {
		serviceNames = append(serviceNames, name)
	}
	sort.Strings(serviceNames)

	allServices := make(map[string]struct{}, len(serviceNames))
	for _, name := range serviceNames {
		allServices[name] = struct{}{}
	}
	composeGraph := make(map[string][]string)

	for _, name := range serviceNames {
		service, enabled := project.Services[name]
		if !enabled {
			service = project.DisabledServices[name]
		}
		spec := core.ServiceExtensionSpec{}
		for dependency := range service.DependsOn {
			composeGraph[name] = append(composeGraph[name], dependency)
		}

		if raw, exists := service.Extensions["x-gp-release"]; exists {
			release, err := decodeServiceRelease(raw)
			if err != nil {
				return nil, err
			}
			spec.Release = &release
		}

		if raw, exists := service.Extensions["x-gp-depends_on"]; exists {
			dependencies, err := decodeServiceDependencies(raw)
			if err != nil {
				return nil, err
			}
			if service.DependsOn == nil {
				service.DependsOn = make(map[string]types.ServiceDependency)
			}
			names := make([]string, 0, len(dependencies))
			for dependency := range dependencies {
				names = append(names, dependency)
			}
			sort.Strings(names)
			spec.DependsOn = make(map[string]core.ServiceDependency, len(names))
			for _, dependencyName := range names {
				dependency := dependencies[dependencyName]
				if dependencyName == "" || dependencyName == name {
					return nil, validationError("blueprint Service dependency target is invalid")
				}
				if _, exists := allServices[dependencyName]; !exists {
					return nil, validationError("blueprint Service dependency target does not exist")
				}
				if _, duplicate := service.DependsOn[dependencyName]; duplicate {
					return nil, validationError("blueprint Service dependency is defined twice")
				}
				phases, err := normalizeDependencyPhases(dependency.Phases)
				if err != nil {
					return nil, err
				}
				dependency.Phases = phases
				spec.DependsOn[dependencyName] = dependency
				if len(phases) == 0 {
					service.DependsOn[dependencyName] = types.ServiceDependency{
						Condition: dependency.Condition.String(), Required: true,
					}
					composeGraph[name] = append(composeGraph[name], dependencyName)
					continue
				}
			}
		}

		if spec.Release != nil || len(spec.DependsOn) != 0 {
			result[name] = spec
		}
		if enabled {
			project.Services[name] = service
		} else {
			project.DisabledServices[name] = service
		}
	}
	if dependencyGraphHasCycle(composeGraph) {
		return nil, validationError("blueprint Service dependency graph contains a cycle")
	}
	for _, phase := range []core.ServiceLifecyclePhase{
		core.ServiceLifecycleStart,
		core.ServiceLifecycleDeploy,
		core.ServiceLifecycleRollback,
	} {
		if _, err := core.BuildServiceDependencyPhasePlan(serviceNames, result, phase); err != nil {
			return nil, validationError("blueprint Service dependency graph is invalid: " + err.Error())
		}
	}
	return result, nil
}

func decodeExtension(value any, destination any) error {
	encoded, err := yaml.Marshal(value)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	return decoder.Decode(destination)
}

func normalizeDependencyPhases(phases []core.ServiceDependencyPhase) ([]core.ServiceDependencyPhase, error) {
	seen := make(map[core.ServiceDependencyPhase]struct{}, len(phases))
	result := append([]core.ServiceDependencyPhase(nil), phases...)
	for _, phase := range result {
		if dependency := (core.ServiceDependency{
			Condition: core.ServiceDependencyStarted,
			Phases:    []core.ServiceDependencyPhase{phase},
		}); dependency.Validate() != nil {
			return nil, validationError("blueprint Service dependency phase is invalid")
		}
		if _, duplicate := seen[phase]; duplicate {
			return nil, validationError("blueprint Service dependency phase is duplicated")
		}
		seen[phase] = struct{}{}
	}
	sort.Slice(result, func(left int, right int) bool { return result[left] < result[right] })
	return result, nil
}

func dependencyGraphHasCycle(graph map[string][]string) bool {
	const (
		unvisited = iota
		visiting
		visited
	)
	states := make(map[string]int, len(graph))
	var visit func(string) bool
	visit = func(node string) bool {
		switch states[node] {
		case visiting:
			return true
		case visited:
			return false
		}
		states[node] = visiting
		for _, dependency := range graph[node] {
			if visit(dependency) {
				return true
			}
		}
		states[node] = visited
		return false
	}
	for node := range graph {
		if visit(node) {
			return true
		}
	}
	return false
}
