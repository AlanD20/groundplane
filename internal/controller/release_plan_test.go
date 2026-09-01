package controller

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
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

// Rationale: a workload-targeted Release must not authorize or render an
// unrelated Service produced by a registered Component. Component artifact
// validation remains strict at the full Environment projection boundary.
func TestProjectReleaseWorkloadServicesIgnoresUnrelatedComponentService(t *testing.T) {
	t.Parallel()
	const (
		serviceID   = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		generatedID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		componentID = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	projection := etcd.EnvironmentComposeProjection{
		DesiredServices: []etcd.EnvironmentServiceProjection{{
			Desired: core.Service{ID: serviceID, Name: "api"},
		}},
		Components: []etcd.ComponentRecord{{
			Desired: etcd.ComponentDesiredRecord{ID: componentID},
			Runtime: etcd.ComponentRuntimeRecord{GeneratedServices: []string{generatedID}},
		}},
	}
	if _, err := ComposeIdentitySnapshotFromProjection(projection); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("full Environment identity error = %v, want internal", err)
	}

	project := &composetypes.Project{Services: composetypes.Services{
		"api":    {Name: "api", Image: "example/api:1"},
		"router": {Name: "router", Image: "example/router:1"},
	}}
	releaseProjection := releaseWorkloadProjection(projection)
	if err := projectReleaseWorkloadServices(project, releaseProjection); err != nil {
		t.Fatalf("projectReleaseWorkloadServices() error = %v", err)
	}
	if _, exists := project.Services["router"]; exists {
		t.Fatal("Component-generated Service remained in workload release project")
	}
	if service, exists := project.Services["api"]; !exists || service.Name != "api" {
		t.Fatalf("target workload Service = %#v, exists = %v", service, exists)
	}
	identities, err := ComposeIdentitySnapshotFromProjection(releaseProjection)
	if err != nil {
		t.Fatalf("release identity snapshot error = %v", err)
	}
	if len(identities.Services) != 1 || identities.Services[0].ID != serviceID ||
		identities.Services[0].Name != "api" || len(releaseProjection.Components) != 0 ||
		len(projection.Components) != 1 || ids.Validate(ids.KindService, identities.Services[0].ID) != nil {
		t.Fatalf("release workload identities = %#v", identities.Services)
	}
}
