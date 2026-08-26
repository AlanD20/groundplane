package controller

import (
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

func TestBuildEnvironmentComposeProjectionFreezesAlwaysForReleasePhases(t *testing.T) {
	t.Parallel()
	snapshot := ComposeIdentitySnapshot{Services: []ComposeResourceIdentity{
		{Name: "api"}, {Name: "database"}, {Name: "worker"},
	}}
	extensions := map[string]core.ServiceExtensionSpec{
		"api": {DependsOn: map[string]core.ServiceDependency{
			"database": {
				Condition: core.ServiceDependencyCompletedSuccessfully,
				Phases:    []core.ServiceDependencyPhase{core.ServiceDependencyPhaseAlways},
			},
		}},
	}
	names := []string{"api", "database", "worker"}
	deploy, err := core.BuildServiceDependencyPhasePlan(names, extensions, core.ServiceLifecycleDeploy)
	if err != nil {
		t.Fatal(err)
	}
	rollback, err := core.BuildServiceDependencyPhasePlan(names, extensions, core.ServiceLifecycleRollback)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := BuildEnvironmentComposeProjection(
		"",
		"",
		1,
		snapshot,
		nil,
		nil,
		nil,
		core.ServiceDependencyPlans{
			DeployDependencyPlan: deploy, RollbackDependencyPlan: rollback,
		},
	)
	if err != nil {
		t.Fatalf("BuildEnvironmentComposeProjection() error = %v", err)
	}
	frozenDeploy, frozenRollback := projection.DeployDependencyPlan, projection.RollbackDependencyPlan
	if frozenDeploy.Phase != core.ServiceLifecycleDeploy || frozenRollback.Phase != core.ServiceLifecycleRollback ||
		len(frozenDeploy.Edges) != 1 || len(frozenRollback.Edges) != 1 ||
		!slices.Equal(frozenDeploy.OrderedServices, frozenRollback.OrderedServices) {
		t.Fatalf("BuildEnvironmentComposeProjection() = deploy %#v, rollback %#v", frozenDeploy, frozenRollback)
	}
}
