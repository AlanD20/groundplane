package core

import (
	"slices"
	"testing"
)

func TestValidateServiceDependencyPhasePlanRejectsNonCanonicalDurablePlans(t *testing.T) {
	t.Parallel()
	valid := ServiceDependencyPhasePlan{
		Phase:           ServiceLifecycleDeploy,
		OrderedServices: []string{"database", "api"},
		Edges: []ServiceDependencyEdge{{
			Service: "api", Dependency: "database", Condition: ServiceDependencyCompletedSuccessfully,
		}},
	}
	if err := ValidateServiceDependencyPhasePlan([]string{"api", "database", "worker"}, valid); err != nil {
		t.Fatalf("ValidateServiceDependencyPhasePlan() error = %v", err)
	}
	for name, mutate := range map[string]func(*ServiceDependencyPhasePlan){
		"missing service": func(plan *ServiceDependencyPhasePlan) { plan.OrderedServices = plan.OrderedServices[:1] },
		"edge violation": func(plan *ServiceDependencyPhasePlan) {
			plan.OrderedServices[0], plan.OrderedServices[1] = plan.OrderedServices[1], plan.OrderedServices[0]
		},
		"unknown service": func(plan *ServiceDependencyPhasePlan) { plan.Edges[0].Dependency = "cache" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := CloneServiceDependencyPhasePlan(valid)
			mutate(&candidate)
			if err := ValidateServiceDependencyPhasePlan([]string{"api", "database", "worker"}, candidate); err == nil {
				t.Fatal("ValidateServiceDependencyPhasePlan() error = nil")
			}
		})
	}
}

func TestCloneServiceDependencyPhasePlanOwnsSlices(t *testing.T) {
	t.Parallel()
	original := ServiceDependencyPhasePlan{
		Phase:           ServiceLifecycleRollback,
		OrderedServices: []string{"database", "api"},
		Edges: []ServiceDependencyEdge{{
			Service: "api", Dependency: "database", Condition: ServiceDependencyCompletedSuccessfully,
		}},
	}
	clone := CloneServiceDependencyPhasePlan(original)
	clone.OrderedServices[0] = "changed"
	clone.Edges[0].Dependency = "changed"
	if !slices.Equal(original.OrderedServices, []string{"database", "api"}) ||
		original.Edges[0].Dependency != "database" {
		t.Fatalf("CloneServiceDependencyPhasePlan() aliased input: %#v", original)
	}
}

func TestValidateServiceDependencyPhasePlanRejectsNonCanonicalOrder(t *testing.T) {
	t.Parallel()

	plan := ServiceDependencyPhasePlan{
		Phase:           ServiceLifecycleDeploy,
		OrderedServices: []string{"database", "api", "cache", "worker"},
		Edges: []ServiceDependencyEdge{
			{Service: "api", Dependency: "database", Condition: ServiceDependencyStarted},
			{Service: "worker", Dependency: "cache", Condition: ServiceDependencyStarted},
		},
	}

	if err := ValidateServiceDependencyPhasePlan([]string{"worker", "api", "cache", "database"}, plan); err == nil {
		t.Fatal("expected non-canonical order to be rejected")
	}
}
