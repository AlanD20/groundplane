package core

import (
	"errors"
	"fmt"
	"sort"
)

// ServiceDependencyCondition is the closed set of native Compose dependency
// conditions accepted by x-gp-depends_on.
type ServiceDependencyCondition string

const (
	ServiceDependencyStarted               ServiceDependencyCondition = "service_started"
	ServiceDependencyHealthy               ServiceDependencyCondition = "service_healthy"
	ServiceDependencyCompletedSuccessfully ServiceDependencyCondition = "service_completed_successfully"
)

func (condition ServiceDependencyCondition) String() string {
	return string(condition)
}

func (condition ServiceDependencyCondition) valid() bool {
	switch condition {
	case ServiceDependencyStarted, ServiceDependencyHealthy, ServiceDependencyCompletedSuccessfully:
		return true
	default:
		return false
	}
}

// ServiceDependencyPhase is the closed set of lifecycle phases accepted by
// x-gp-depends_on. An empty phase list means native Compose startup only.
type ServiceDependencyPhase string

const (
	ServiceDependencyPhaseStart    ServiceDependencyPhase = "start"
	ServiceDependencyPhaseDeploy   ServiceDependencyPhase = "deploy"
	ServiceDependencyPhaseRollback ServiceDependencyPhase = "rollback"
	ServiceDependencyPhaseAlways   ServiceDependencyPhase = "always"
)

func (phase ServiceDependencyPhase) String() string {
	return string(phase)
}

func (phase ServiceDependencyPhase) valid() bool {
	switch phase {
	case ServiceDependencyPhaseStart, ServiceDependencyPhaseDeploy,
		ServiceDependencyPhaseRollback, ServiceDependencyPhaseAlways:
		return true
	default:
		return false
	}
}

// ServiceDependency is x-gp-depends_on's per-dependency shape.
type ServiceDependency struct {
	Condition ServiceDependencyCondition `yaml:"condition"        json:"condition"`
	Phases    []ServiceDependencyPhase   `yaml:"phases,omitempty" json:"phases,omitempty"`
}

func (dependency ServiceDependency) Validate() error {
	if !dependency.Condition.valid() {
		return errors.New("dependency condition is invalid")
	}
	seen := make(map[ServiceDependencyPhase]struct{}, len(dependency.Phases))
	for _, phase := range dependency.Phases {
		if !phase.valid() {
			return errors.New("dependency phase is invalid")
		}
		if _, duplicate := seen[phase]; duplicate {
			return errors.New("dependency phase is duplicated")
		}
		seen[phase] = struct{}{}
	}
	return nil
}

// PhaseStrings returns the authored lifecycle phases in API representation.
func (dependency ServiceDependency) PhaseStrings() []string {
	if len(dependency.Phases) == 0 {
		return nil
	}
	result := make([]string, len(dependency.Phases))
	for index, phase := range dependency.Phases {
		result[index] = phase.String()
	}
	return result
}

// ServiceLifecyclePhase is an executable lifecycle phase. It deliberately
// excludes "always", which is an authored selector rather than an operation.
type ServiceLifecyclePhase string

const (
	ServiceLifecycleStart    ServiceLifecyclePhase = "start"
	ServiceLifecycleDeploy   ServiceLifecyclePhase = "deploy"
	ServiceLifecycleRollback ServiceLifecyclePhase = "rollback"
)

func (phase ServiceLifecyclePhase) valid() bool {
	switch phase {
	case ServiceLifecycleStart, ServiceLifecycleDeploy, ServiceLifecycleRollback:
		return true
	default:
		return false
	}
}

// ServiceDependencyEdge is one operation-specific prerequisite. Service and
// Dependency are Compose service names from the same Environment.
type ServiceDependencyEdge struct {
	Service    string                     `json:"service"`
	Dependency string                     `json:"dependency"`
	Condition  ServiceDependencyCondition `json:"condition"`
}

// ServiceDependencyPhasePlan is the deterministic task-DAG projection for one
// lifecycle phase. OrderedServices always places prerequisites before their
// consumers; Edges are sorted by consumer and prerequisite.
type ServiceDependencyPhasePlan struct {
	Phase           ServiceLifecyclePhase   `json:"phase"`
	OrderedServices []string                `json:"ordered_services"`
	Edges           []ServiceDependencyEdge `json:"edges,omitempty"`
}

// BuildServiceDependencyPhasePlan compiles authored phase selectors into one
// closed operation DAG. Native startup-only dependencies are excluded because
// Compose already owns them in the canonical project.
func BuildServiceDependencyPhasePlan(
	serviceNames []string,
	extensions map[string]ServiceExtensionSpec,
	phase ServiceLifecyclePhase,
) (ServiceDependencyPhasePlan, error) {
	if !phase.valid() {
		return ServiceDependencyPhasePlan{}, errors.New("service lifecycle phase is invalid")
	}
	services := make(map[string]struct{}, len(serviceNames))
	for _, name := range serviceNames {
		if name == "" {
			return ServiceDependencyPhasePlan{}, errors.New("service name is empty")
		}
		if _, duplicate := services[name]; duplicate {
			return ServiceDependencyPhasePlan{}, fmt.Errorf("service %q is duplicated", name)
		}
		services[name] = struct{}{}
	}

	edges := make([]ServiceDependencyEdge, 0)
	for service, extension := range extensions {
		if _, exists := services[service]; !exists {
			return ServiceDependencyPhasePlan{}, fmt.Errorf("service %q does not exist", service)
		}
		for dependencyName, dependency := range extension.DependsOn {
			if err := dependency.Validate(); err != nil {
				return ServiceDependencyPhasePlan{}, fmt.Errorf(
					"service %q dependency %q is invalid: %w",
					service,
					dependencyName,
					err,
				)
			}
			if dependencyName == service {
				return ServiceDependencyPhasePlan{}, fmt.Errorf("service %q depends on itself", service)
			}
			if _, exists := services[dependencyName]; !exists {
				return ServiceDependencyPhasePlan{}, fmt.Errorf(
					"service %q dependency %q does not exist",
					service,
					dependencyName,
				)
			}
			if !dependency.appliesTo(phase) {
				continue
			}
			edges = append(edges, ServiceDependencyEdge{
				Service: service, Dependency: dependencyName, Condition: dependency.Condition,
			})
		}
	}
	sort.Slice(edges, func(left int, right int) bool {
		if edges[left].Service != edges[right].Service {
			return edges[left].Service < edges[right].Service
		}
		return edges[left].Dependency < edges[right].Dependency
	})

	ordered, err := orderServiceDependencyPhase(edges)
	if err != nil {
		return ServiceDependencyPhasePlan{}, err
	}
	return ServiceDependencyPhasePlan{Phase: phase, OrderedServices: ordered, Edges: edges}, nil
}

func (dependency ServiceDependency) appliesTo(phase ServiceLifecyclePhase) bool {
	for _, selector := range dependency.Phases {
		if selector == ServiceDependencyPhaseAlways || selector.String() == string(phase) {
			return true
		}
	}
	return false
}

func orderServiceDependencyPhase(edges []ServiceDependencyEdge) ([]string, error) {
	nodes := make(map[string]struct{}, len(edges)*2)
	indegree := make(map[string]int, len(edges)*2)
	dependents := make(map[string][]string, len(edges))
	for _, edge := range edges {
		nodes[edge.Service] = struct{}{}
		nodes[edge.Dependency] = struct{}{}
		indegree[edge.Service]++
		dependents[edge.Dependency] = append(dependents[edge.Dependency], edge.Service)
	}
	ready := make([]string, 0, len(nodes))
	for node := range nodes {
		if indegree[node] == 0 {
			ready = append(ready, node)
		}
	}
	sort.Strings(ready)
	ordered := make([]string, 0, len(nodes))
	for len(ready) != 0 {
		node := ready[0]
		ready = ready[1:]
		ordered = append(ordered, node)
		for _, dependent := range dependents[node] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
				sort.Strings(ready)
			}
		}
	}
	if len(ordered) != len(nodes) {
		return nil, errors.New("service dependency phase graph contains a cycle")
	}
	return ordered, nil
}
