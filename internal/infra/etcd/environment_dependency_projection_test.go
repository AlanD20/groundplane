package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestEnvironmentComposeProjectionOwnsAndValidatesDependencyPlans(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)
	services := []EnvironmentComposeIdentity{
		{ID: ids.NewAt(ids.KindService, at, 1), Name: "api"},
		{ID: ids.NewAt(ids.KindService, at, 2), Name: "database"},
	}
	deploy := core.ServiceDependencyPhasePlan{
		Phase:           core.ServiceLifecycleDeploy,
		OrderedServices: []string{"database", "api"},
		Edges: []core.ServiceDependencyEdge{{
			Service: "api", Dependency: "database", Condition: core.ServiceDependencyCompletedSuccessfully,
		}},
	}
	rollback := core.ServiceDependencyPhasePlan{
		Phase:           core.ServiceLifecycleRollback,
		OrderedServices: []string{"database", "api"},
		Edges: []core.ServiceDependencyEdge{{
			Service: "api", Dependency: "database", Condition: core.ServiceDependencyCompletedSuccessfully,
		}},
	}
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID:    ids.NewAt(ids.KindEnvironment, at, 3),
		RevisionID:       ids.NewAt(ids.KindTask, at, 4),
		RenderGeneration: 1,
		Services:         services,
		ServiceDependencyPlans: core.ServiceDependencyPlans{
			DeployDependencyPlan: deploy, RollbackDependencyPlan: rollback,
		},
	})
	if err := validateEnvironmentComposeProjection(projection); err != nil {
		t.Fatalf("validateEnvironmentComposeProjection() error = %v", err)
	}
	clone := cloneEnvironmentComposeProjection(projection)
	clone.DeployDependencyPlan.OrderedServices[0] = "changed"
	clone.RollbackDependencyPlan.Edges[0].Dependency = "changed"
	if projection.DeployDependencyPlan.OrderedServices[0] != "database" ||
		projection.RollbackDependencyPlan.Edges[0].Dependency != "database" {
		t.Fatalf("cloneEnvironmentComposeProjection() aliased plans: %#v", projection)
	}
	invalid := projection
	invalid.DeployDependencyPlan = core.CloneServiceDependencyPhasePlan(projection.DeployDependencyPlan)
	invalid.DeployDependencyPlan.OrderedServices[0], invalid.DeployDependencyPlan.OrderedServices[1] =
		invalid.DeployDependencyPlan.OrderedServices[1], invalid.DeployDependencyPlan.OrderedServices[0]
	if err := validateEnvironmentComposeProjection(invalid); err == nil {
		t.Fatal("validateEnvironmentComposeProjection(invalid order) error = nil")
	}
}
