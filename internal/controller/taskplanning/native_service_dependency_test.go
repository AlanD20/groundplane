package taskplanning

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	compose "github.com/compose-spec/compose-go/v2/types"
)

// SVC-15/BP-04/TASK-73: deploy-only and rollback-only jobs must not leak into each
// other's rendered dependencies; native startup dependencies remain intact.
func TestNativeProjectedDependenciesRespectLifecyclePhase(t *testing.T) {
	names := []string{"api", "shared", "migrate", "previous"}
	extensions := map[string]core.ServiceExtensionSpec{"api": {DependsOn: map[string]core.ServiceDependency{
		"shared": {
			Condition: core.ServiceDependencyHealthy,
			Phases:    []core.ServiceDependencyPhase{core.ServiceDependencyPhaseAlways},
		},
		"migrate": {
			Condition: core.ServiceDependencyCompletedSuccessfully,
			Phases:    []core.ServiceDependencyPhase{core.ServiceDependencyPhaseDeploy},
		},
		"previous": {
			Condition: core.ServiceDependencyStarted,
			Phases:    []core.ServiceDependencyPhase{core.ServiceDependencyPhaseRollback},
		},
	}}}
	deploy, err := core.BuildServiceDependencyPhasePlan(names, extensions, core.ServiceLifecycleDeploy)
	if err != nil {
		t.Fatal(err)
	}
	rollback, err := core.BuildServiceDependencyPhasePlan(names, extensions, core.ServiceLifecycleRollback)
	if err != nil {
		t.Fatal(err)
	}
	plans := core.ServiceDependencyPlans{DeployDependencyPlan: deploy, RollbackDependencyPlan: rollback}
	for _, selected := range []struct {
		phase    core.ServiceLifecyclePhase
		extra    string
		disabled bool
	}{
		{core.ServiceLifecycleStart, "", false}, {core.ServiceLifecycleDeploy, "migrate", false}, {core.ServiceLifecycleRollback, "previous", false},
		{core.ServiceLifecycleStart, "", true}, {core.ServiceLifecycleDeploy, "migrate", true}, {core.ServiceLifecycleRollback, "previous", true},
	} {
		project := &compose.Project{Services: compose.Services{}}
		for _, name := range names {
			project.Services[name] = compose.ServiceConfig{Name: name, Image: name}
		}
		service := project.Services["api"]
		service.DependsOn = compose.DependsOnConfig{"shared": {Condition: "service_healthy", Required: true}}
		project.Services["api"] = service
		if selected.disabled {
			service.Profiles = []string{"jobs"}
			project.DisabledServices = compose.Services{"api": service}
			delete(project.Services, "api")
		}
		if err := composerender.ApplyProjectedServiceDependencyPhase(project, plans, selected.phase); err != nil {
			t.Fatal(err)
		}
		consumer := project.Services["api"]
		if selected.disabled {
			if _, enabled := project.Services["api"]; enabled {
				t.Fatal("dependency projection enabled a disabled consumer")
			}
			consumer = project.DisabledServices["api"]
			if len(consumer.Profiles) != 1 || consumer.Profiles[0] != "jobs" {
				t.Fatal("dependency projection lost the consumer profile")
			}
		}
		dependencies := consumer.DependsOn
		if dependencies["shared"].Condition != "service_healthy" {
			t.Fatal("native startup dependency was changed")
		}
		want := 1
		if selected.extra != "" {
			want++
			if _, exists := dependencies[selected.extra]; !exists {
				t.Fatal("selected lifecycle dependency was omitted")
			}
		}
		if len(dependencies) != want {
			t.Fatal("another lifecycle phase's dependency leaked into the rendered workload")
		}
	}
}
