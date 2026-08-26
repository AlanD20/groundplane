package controller

import (
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
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
	projection := etcd.EnvironmentComposeProjection{
		Services: []etcd.EnvironmentComposeIdentity{
			{ID: snapshot.Services[0].ID, Name: snapshot.Services[0].Name},
			{ID: snapshot.Services[1].ID, Name: snapshot.Services[1].Name},
			{ID: snapshot.Services[2].ID, Name: snapshot.Services[2].Name},
		},
		ServiceDependencyPlans: core.ServiceDependencyPlans{
			DeployDependencyPlan: deploy, RollbackDependencyPlan: rollback,
		},
	}
	frozenDeploy, frozenRollback := projection.DeployDependencyPlan, projection.RollbackDependencyPlan
	if frozenDeploy.Phase != core.ServiceLifecycleDeploy || frozenRollback.Phase != core.ServiceLifecycleRollback ||
		len(frozenDeploy.Edges) != 1 || len(frozenRollback.Edges) != 1 ||
		!slices.Equal(frozenDeploy.OrderedServices, frozenRollback.OrderedServices) {
		t.Fatalf("BuildEnvironmentComposeProjection() = deploy %#v, rollback %#v", frozenDeploy, frozenRollback)
	}
}
