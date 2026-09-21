package taskplanning

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func TestPrepareReleaseTaskFirstDeploySealsCandidateAbsence(t *testing.T) {
	testPrepareReleaseTaskFirstDeploy(t, false)
}

func TestPrepareReleaseTaskFirstBlueGreenDeploySealsCandidateAbsence(t *testing.T) {
	testPrepareReleaseTaskFirstDeploy(t, true)
}

func testPrepareReleaseTaskFirstDeploy(t *testing.T, blueGreen bool) {
	t.Helper()
	reader, task := blueprintPlanTestState(t)
	// A deployable Release must carry its healthcheck; the generic Blueprint
	// fixture intentionally has none and is also used for configured-only work.
	project := &composetypes.Project{
		Services: composetypes.Services{"api": {
			Name: "api", Image: "example/api:latest",
			HealthCheck: &composetypes.HealthCheckConfig{Test: composetypes.HealthCheckTest{"CMD", "true"}},
			Networks:    map[string]*composetypes.ServiceNetworkConfig{"frontend": {}},
			Volumes: []composetypes.ServiceVolumeConfig{
				{Type: composetypes.VolumeTypeVolume, Source: "app-data", Target: "/data"},
			},
		}},
		Networks: composetypes.Networks{"frontend": {}},
		Volumes:  composetypes.Volumes{"app-data": {}},
	}
	if blueGreen {
		api := project.Services["api"]
		api.Expose = []string{"8080"}
		project.Services["api"] = api
	}
	normalized, err := project.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	reader.projection.NormalizedCompose = normalized
	const (
		publicationID = "publication-first-release"
		releaseID     = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		serviceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
	)
	task.Type, task.Target = testtaskjournal.TaskDeploy, serviceID
	task.Params[testreleaserender.TaskReleasePublicationParam] = publicationID
	task.Steps = []testtaskjournal.TaskStepRecord{
		{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB0"},
		{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB1"},
		{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB2"},
		{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB3"},
		{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB4"},
	}
	member := testreleaserender.ReleaseTaskRenderMember{
		Intent: domain.Intent{ID: releaseID, EnvironmentID: reader.environment.ID, ServiceID: serviceID,
			OperationID: task.OperationID, OperationKind: domain.OperationDeploy, CandidateWorkload: releaseTestWorkload("example/api:first"),
			Strategy: domain.StrategyRecreate, OnFailure: domain.OnFailureSwitchBack},
		Render: testreleaserender.ReleaseRenderInput{ReleaseID: releaseID, PlanID: task.PlanID, ArtifactID: artifactID,
			ServiceID: serviceID, ServiceName: "api", CandidateWorkload: releaseTestWorkload("example/api:first"), Strategy: domain.StrategyRecreate,
			CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
			TenantID: reader.tenant.ID, TenantSlug: reader.tenant.Slug, ProjectID: reader.project.ID,
			ProjectSlug: reader.project.Slug, EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
			AuthorizedVolumeDir: reader.environment.VolumeDir, Projection: reader.projection},
	}
	if blueGreen {
		member.Intent.Strategy, member.Intent.Slot = domain.StrategyBlueGreen, domain.SlotBlue
		member.Render.Strategy, member.Render.Slot = domain.StrategyBlueGreen, domain.SlotBlue
		member.Render.PriorStrategy = domain.StrategyRecreate
		member.Render.CandidateTarget = domain.WorkloadBlue
		member.Render.ProxyPorts, member.Render.ProxyGeneration = []uint16{8080}, 2
		member.Render.ProxyImage = testServiceProxyImage()
		config, err := domain.RenderProxyConfig("api", releaseID, domain.WorkloadBlue, 2, []uint16{8080})
		if err != nil {
			t.Fatal(err)
		}
		member.Render.ProxyConfigDigest = hex.EncodeToString(config.SHA256[:])
		// Direct Deploy must persist this exact render input before preparing
		// its plan; bypassing the codec misses publication-only rejection.
		if _, err := testreleaserender.EncodeReleaseRenderInput(member.Render); err != nil {
			t.Fatalf("first blue-green publication render: %v", err)
		}
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	prepared, plan, err := resolver.PrepareReleaseTask(context.Background(), task, etcd.ReleaseTaskRenderInput{
		PublicationID: publicationID,
		Operation: testreleases.ReleaseOperationHead{OperationID: task.OperationID, PublicationID: publicationID,
			EnvironmentID: reader.environment.ID, FailurePolicy: domain.OnFailureSwitchBack},
		Members: []testreleaserender.ReleaseTaskRenderMember{member},
	})
	if err != nil {
		t.Fatalf("PrepareReleaseTask(first candidate) error = %v", err)
	}
	procedure := plan.GetCandidateReleaseProcedure()
	if procedure == nil || len(procedure.GetMembers()) != 1 || procedure.GetMembers()[0].GetCandidateAbsence() == nil ||
		procedure.GetMembers()[0].GetServingPredecessor() != nil || len(plan.GetArtifacts()) != 1 {
		t.Fatalf("first Release procedure = %#v", procedure)
	}
	descriptor, err := executionplan.DescribeCandidateRelease(plan)
	if err != nil || executionplan.CandidateReleaseDescriptorMatchesPlan(descriptor, plan) != nil ||
		prepared.PlanHash != hex.EncodeToString(plan.GetPlanHash()) {
		t.Fatalf("prepared publication did not bind the validated plan: %v", err)
	}
	if blueGreen {
		steps := plan.GetSteps()
		if len(steps) != 5 || steps[0].GetComposeWorkloadApply().GetTarget() != "blue" ||
			!steps[0].GetComposeWorkloadApply().
				GetEnsureProxy() ||
			steps[1].GetWaitWorkloadHealthy().GetTarget() != "blue" ||
			steps[2].GetServiceProxySwitch().GetReleaseId() != releaseID ||
			steps[3].GetCandidateRestorationProbe().GetCandidateReleaseId() != releaseID ||
			steps[4].GetCandidateRestorationCompensate().GetCandidateReleaseId() != releaseID {
			t.Fatalf("first blue-green has incorrect forward or absence recovery steps: %v", steps)
		}
		workloads, candidates, proxies := 0, 0, 0
		for _, service := range plan.GetArtifacts()[0].GetServices() {
			switch service.GetRole() {
			case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
				workloads++
				if service.GetSlot() == "blue" {
					candidates++
				}
				if service.GetImageReference() != member.Render.CandidateWorkload.LocalImageID ||
					service.GetExpectedReplicas() != 1 {
					t.Fatalf("candidate workload authority changed: %v", service)
				}
			case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY:
				proxies++
			}
		}
		// The artifact retains both static slot definitions; the forward step
		// above selects only blue, and no serving predecessor is authorized.
		if workloads != 2 || candidates != 1 || proxies != 1 {
			t.Fatalf("first blue-green rendered %d workloads and %d proxies", workloads, proxies)
		}
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
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			Desired: core.Service{ID: serviceID, Name: "api"},
		}},
		Components: []testcomponents.Record{{
			Desired: testcomponents.DesiredRecord{ID: componentID},
			Runtime: testcomponents.RuntimeRecord{GeneratedServices: []string{generatedID}},
		}},
	}
	if _, err := testcomposerender.ComposeIdentitySnapshotFromProjection(projection); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
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
	identities, err := testcomposerender.ComposeIdentitySnapshotFromProjection(releaseProjection)
	if err != nil {
		t.Fatalf("release identity snapshot error = %v", err)
	}
	if len(identities.Services) != 1 || identities.Services[0].ID != serviceID ||
		identities.Services[0].Name != "api" || len(releaseProjection.Components) != 0 ||
		len(projection.Components) != 1 || ids.Validate(ids.KindService, identities.Services[0].ID) != nil {
		t.Fatalf("release workload identities = %#v", identities.Services)
	}
}
