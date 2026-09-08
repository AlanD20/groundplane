package core

import (
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ServiceDependencyPlans is the applied deploy/rollback dependency authority.
// Empty embedded plans mean that the producer authored no lifecycle edges.
type ServiceDependencyPlans struct {
	DeployDependencyPlan   ServiceDependencyPhasePlan `json:"deploy_dependency_plan,omitempty"`
	RollbackDependencyPlan ServiceDependencyPhasePlan `json:"rollback_dependency_plan,omitempty"`
}

func (plans ServiceDependencyPlans) Validate(serviceNames []string) error {
	for _, selected := range []struct {
		phase ServiceLifecyclePhase
		plan  ServiceDependencyPhasePlan
	}{
		{phase: ServiceLifecycleDeploy, plan: plans.DeployDependencyPlan},
		{phase: ServiceLifecycleRollback, plan: plans.RollbackDependencyPlan},
	} {
		if selected.plan.Phase == "" && len(selected.plan.OrderedServices) == 0 && len(selected.plan.Edges) == 0 {
			continue
		}
		if selected.plan.Phase != selected.phase {
			return errs.New(errs.KindValidationFailed, "Service dependency projection phase is invalid")
		}
		if err := ValidateServiceDependencyPhasePlan(serviceNames, selected.plan); err != nil {
			return err
		}
	}
	return nil
}

func (plans ServiceDependencyPlans) Clone() ServiceDependencyPlans {
	plans.DeployDependencyPlan = CloneServiceDependencyPhasePlan(plans.DeployDependencyPlan)
	plans.RollbackDependencyPlan = CloneServiceDependencyPhasePlan(plans.RollbackDependencyPlan)
	return plans
}

func (plans ServiceDependencyPlans) Equal(other ServiceDependencyPlans) bool {
	return serviceDependencyPhasePlansEqual(plans.DeployDependencyPlan, other.DeployDependencyPlan) &&
		serviceDependencyPhasePlansEqual(plans.RollbackDependencyPlan, other.RollbackDependencyPlan)
}

// WithoutService returns the dependency authority after one desired Service is
// removed. Callers must reject inbound references before publishing removal;
// this helper only produces the exact candidate projection.
func (plans ServiceDependencyPlans) WithoutService(name string) ServiceDependencyPlans {
	plans.DeployDependencyPlan = dependencyPlanWithoutService(plans.DeployDependencyPlan, name)
	plans.RollbackDependencyPlan = dependencyPlanWithoutService(plans.RollbackDependencyPlan, name)
	return plans
}

func dependencyPlanWithoutService(plan ServiceDependencyPhasePlan, name string) ServiceDependencyPhasePlan {
	ordered := plan.OrderedServices[:0]
	for _, service := range plan.OrderedServices {
		if service != name {
			ordered = append(ordered, service)
		}
	}
	plan.OrderedServices = ordered
	edges := plan.Edges[:0]
	for _, edge := range plan.Edges {
		if edge.Service != name && edge.Dependency != name {
			edges = append(edges, edge)
		}
	}
	plan.Edges = edges
	if len(plan.OrderedServices) == 0 && len(plan.Edges) == 0 {
		plan.Phase = ""
	}
	return plan
}

func serviceDependencyPhasePlansEqual(left, right ServiceDependencyPhasePlan) bool {
	return left.Phase == right.Phase && slices.Equal(left.OrderedServices, right.OrderedServices) &&
		slices.Equal(left.Edges, right.Edges)
}

// ValidEnvironmentComposeName is the closed durable-name rule shared by
// applied projections and immutable task render inputs.
func ValidEnvironmentComposeName(value string) bool {
	if value == "" || len(value) > 255 || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if character <= ' ' || character == '/' || character == '\\' {
			return false
		}
	}
	return true
}

// ValidateServiceDependencyPhasePlan verifies a frozen phase plan against the
// complete Service name set from the same applied Blueprint projection.
func ValidateServiceDependencyPhasePlan(serviceNames []string, plan ServiceDependencyPhasePlan) error {
	if !plan.Phase.valid() {
		return errs.New(errs.KindValidationFailed, "Service dependency phase plan phase is invalid")
	}
	services := make(map[string]struct{}, len(serviceNames))
	for _, name := range serviceNames {
		if name == "" {
			return errs.New(errs.KindValidationFailed, "Service dependency phase plan has an empty Service name")
		}
		if _, duplicate := services[name]; duplicate {
			return errs.New(errs.KindValidationFailed, "Service dependency phase plan Service names are duplicated")
		}
		services[name] = struct{}{}
	}
	nodes := make(map[string]struct{}, len(plan.Edges)*2)
	previousService, previousDependency := "", ""
	for index, edge := range plan.Edges {
		if _, exists := services[edge.Service]; !exists {
			return errs.New(errs.KindValidationFailed, "Service dependency phase plan edge has an unknown dependent")
		}
		if _, exists := services[edge.Dependency]; !exists {
			return errs.New(errs.KindValidationFailed, "Service dependency phase plan edge has an unknown prerequisite")
		}
		if edge.Service == edge.Dependency || !edge.Condition.valid() {
			return errs.New(errs.KindValidationFailed, "Service dependency phase plan edge is invalid")
		}
		if index > 0 && (edge.Service < previousService ||
			(edge.Service == previousService && edge.Dependency <= previousDependency)) {
			return errs.New(errs.KindValidationFailed, "Service dependency phase plan edges are duplicated or unsorted")
		}
		nodes[edge.Service] = struct{}{}
		nodes[edge.Dependency] = struct{}{}
		previousService, previousDependency = edge.Service, edge.Dependency
	}
	if len(plan.OrderedServices) != len(nodes) {
		return errs.New(errs.KindValidationFailed, "Service dependency phase plan order is incomplete")
	}
	positions := make(map[string]int, len(plan.OrderedServices))
	for index, name := range plan.OrderedServices {
		if _, exists := nodes[name]; !exists {
			return errs.New(
				errs.KindValidationFailed,
				"Service dependency phase plan order references an unrelated Service",
			)
		}
		if _, duplicate := positions[name]; duplicate {
			return errs.New(errs.KindValidationFailed, "Service dependency phase plan order duplicates a Service")
		}
		positions[name] = index
	}
	for _, edge := range plan.Edges {
		if positions[edge.Dependency] >= positions[edge.Service] {
			return errs.New(errs.KindValidationFailed, "Service dependency phase plan order violates an edge")
		}
	}
	expectedOrder, err := orderServiceDependencyPhase(plan.Edges)
	if err != nil {
		return errs.New(errs.KindValidationFailed, "Service dependency phase plan contains a cycle")
	}
	if !slices.Equal(plan.OrderedServices, expectedOrder) {
		return errs.New(errs.KindValidationFailed, "Service dependency phase plan order is not canonical")
	}
	return nil
}

// CloneServiceDependencyPhasePlan returns an owned copy suitable for durable
// projections and immutable retry inputs.
func CloneServiceDependencyPhasePlan(plan ServiceDependencyPhasePlan) ServiceDependencyPhasePlan {
	plan.OrderedServices = slices.Clone(plan.OrderedServices)
	plan.Edges = slices.Clone(plan.Edges)
	return plan
}
