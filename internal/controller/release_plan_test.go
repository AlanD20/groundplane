package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func TestPrepareReleaseTaskFirstDeploySealsCandidateAbsence(t *testing.T) {
	reader, task := blueprintPlanTestState(t)
	const (
		publicationID = "publication-first-release"
		releaseID     = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		serviceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
	)
	task.Type, task.Target = etcd.TaskDeploy, serviceID
	task.Params[etcd.TaskReleasePublicationParam] = publicationID
	task.Steps = []etcd.TaskStepRecord{
		{Kind: etcd.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB0"},
		{Kind: etcd.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB1"},
		{Kind: etcd.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB2"},
		{Kind: etcd.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB3"},
		{Kind: etcd.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB4"},
	}
	member := etcd.ReleaseTaskRenderMember{
		Intent: domain.Intent{ID: releaseID, EnvironmentID: reader.environment.ID, ServiceID: serviceID,
			OperationID: task.OperationID, OperationKind: domain.OperationDeploy, Image: "example/api:first",
			Strategy: domain.StrategyRecreate, OnFailure: domain.OnFailureSwitchBack},
		Render: etcd.ReleaseRenderInput{ReleaseID: releaseID, PlanID: task.PlanID, ArtifactID: artifactID,
			ServiceID: serviceID, ServiceName: "api", Image: "example/api:first", Strategy: domain.StrategyRecreate,
			CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
			TenantID: reader.tenant.ID, TenantSlug: reader.tenant.Slug, ProjectID: reader.project.ID,
			ProjectSlug: reader.project.Slug, EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
			AuthorizedVolumeDir: reader.environment.VolumeDir, Projection: reader.projection},
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, plan, err := resolver.PrepareReleaseTask(context.Background(), task, etcd.ReleaseTaskRenderInput{
		PublicationID: publicationID,
		Operation: etcd.ReleaseOperationHead{OperationID: task.OperationID, PublicationID: publicationID,
			EnvironmentID: reader.environment.ID, FailurePolicy: domain.OnFailureSwitchBack},
		Members: []etcd.ReleaseTaskRenderMember{member},
	})
	if err != nil {
		t.Fatalf("PrepareReleaseTask(first candidate) error = %v", err)
	}
	procedure := plan.GetCandidateReleaseProcedure()
	if procedure == nil || len(procedure.GetMembers()) != 1 || procedure.GetMembers()[0].GetCandidateAbsence() == nil ||
		procedure.GetMembers()[0].GetServingPredecessor() != nil || len(plan.GetArtifacts()) != 1 {
		t.Fatalf("first Release procedure = %#v", procedure)
	}
}

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
