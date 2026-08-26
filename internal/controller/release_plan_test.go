package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: a profiled Service remains disabled when its frozen external
// dependency is compiled into the immutable release artifact.
func TestApplyExternalReleaseDependenciesPreservesDisabledConsumer(t *testing.T) {
	t.Parallel()
	project := &composetypes.Project{
		Services: composetypes.Services{"database": composetypes.ServiceConfig{Image: "database:1"}},
		DisabledServices: composetypes.Services{"worker": composetypes.ServiceConfig{
			Image: "worker:1", Profiles: []string{"jobs"},
		}},
	}
	plan := core.ServiceDependencyPhasePlan{
		Phase: core.ServiceLifecycleDeploy,
		Edges: []core.ServiceDependencyEdge{{
			Service: "worker", Dependency: "database", Condition: core.ServiceDependencyHealthy,
		}},
	}
	if err := applyExternalReleaseDependencies(project, map[string]struct{}{"worker": {}}, plan); err != nil {
		t.Fatalf("applyExternalReleaseDependencies() error = %v", err)
	}
	if _, active := project.Services["worker"]; active {
		t.Fatal("disabled release consumer was promoted into active Services")
	}
	worker, disabled := project.DisabledServices["worker"]
	dependency, exists := worker.DependsOn["database"]
	if !disabled || !exists || dependency.Condition != core.ServiceDependencyHealthy.String() ||
		!dependency.Required || len(worker.Profiles) != 1 || worker.Profiles[0] != "jobs" {
		t.Fatalf("disabled release consumer = %#v", worker)
	}
}
