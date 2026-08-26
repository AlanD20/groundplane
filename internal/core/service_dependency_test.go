package core

import (
	"reflect"
	"testing"
)

func TestBuildServiceDependencyPhasePlanOrdersEveryPhaseDeterministically(t *testing.T) {
	t.Parallel()
	services := []string{"worker", "api", "migrate", "shared"}
	extensions := map[string]ServiceExtensionSpec{
		"api": {DependsOn: map[string]ServiceDependency{
			"migrate": {
				Condition: ServiceDependencyCompletedSuccessfully,
				Phases:    []ServiceDependencyPhase{ServiceDependencyPhaseDeploy},
			},
			"shared": {
				Condition: ServiceDependencyHealthy,
				Phases:    []ServiceDependencyPhase{ServiceDependencyPhaseAlways},
			},
		}},
		"worker": {DependsOn: map[string]ServiceDependency{
			"api": {
				Condition: ServiceDependencyStarted,
				Phases:    []ServiceDependencyPhase{ServiceDependencyPhaseRollback},
			},
		}},
	}
	tests := []struct {
		phase ServiceLifecyclePhase
		want  []string
	}{
		{phase: ServiceLifecycleStart, want: []string{"shared", "api"}},
		{phase: ServiceLifecycleDeploy, want: []string{"migrate", "shared", "api"}},
		{phase: ServiceLifecycleRollback, want: []string{"shared", "api", "worker"}},
	}
	for _, test := range tests {
		plan, err := BuildServiceDependencyPhasePlan(services, extensions, test.phase)
		if err != nil {
			t.Fatalf("BuildServiceDependencyPhasePlan(%s) error = %v", test.phase, err)
		}
		if !reflect.DeepEqual(plan.OrderedServices, test.want) {
			t.Fatalf(
				"BuildServiceDependencyPhasePlan(%s) order = %v, want %v",
				test.phase,
				plan.OrderedServices,
				test.want,
			)
		}
	}
}

func TestBuildServiceDependencyPhasePlanRejectsBrokenGraph(t *testing.T) {
	t.Parallel()
	for name, extensions := range map[string]map[string]ServiceExtensionSpec{
		"cycle": {
			"api":    {DependsOn: map[string]ServiceDependency{"worker": {Condition: ServiceDependencyStarted, Phases: []ServiceDependencyPhase{ServiceDependencyPhaseDeploy}}}},
			"worker": {DependsOn: map[string]ServiceDependency{"api": {Condition: ServiceDependencyStarted, Phases: []ServiceDependencyPhase{ServiceDependencyPhaseDeploy}}}},
		},
		"missing target": {
			"api": {DependsOn: map[string]ServiceDependency{"missing": {Condition: ServiceDependencyStarted, Phases: []ServiceDependencyPhase{ServiceDependencyPhaseDeploy}}}},
		},
		"invalid condition": {
			"api": {DependsOn: map[string]ServiceDependency{"worker": {Condition: "ready", Phases: []ServiceDependencyPhase{ServiceDependencyPhaseDeploy}}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := BuildServiceDependencyPhasePlan(
				[]string{"api", "worker"}, extensions, ServiceLifecycleDeploy,
			); err == nil {
				t.Fatal("BuildServiceDependencyPhasePlan() error = nil")
			}
		})
	}
}
