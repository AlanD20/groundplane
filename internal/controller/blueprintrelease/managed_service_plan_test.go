package blueprintrelease

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"

	registeredcaddy "github.com/AlanD20/groundplane-registered-components/caddy"
	registeredtunnel "github.com/AlanD20/groundplane-registered-components/cloudflaretunnel"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// A Tunnel-only Blueprint must run and observe its managed Service, even when
// no native Service Release or owned Zone exists. Restart uses persisted input.
func TestPrepareTunnelOnlyStartsManagedServiceAndReplaysPersistedPlan(t *testing.T) {
	testPrepareManagedService(t, false, false)
}

func TestPrepareManagedOnlyAcceptsReadOnlyRetainedNativeLabels(t *testing.T) {
	testPrepareManagedService(t, true, false)
}

func TestPrepareManagedOnlyFirstEnableDoesNotTeardownCandidate(t *testing.T) {
	testPrepareManagedService(t, false, true)
}

// Full preflight-to-publication coverage lives in the real Release publisher
// fixture; this focused test receives the artifact already merged before Stage.
func testPrepareManagedService(t *testing.T, retainedNative bool, firstEnableProjectionSource bool) {
	t.Helper()
	at := time.Date(2026, 9, 5, 13, 0, 0, 0, time.UTC)
	tenantID, projectID, environmentID := ids.New(
		ids.KindTenant,
	), ids.New(
		ids.KindProject,
	), ids.New(
		ids.KindEnvironment,
	)
	serviceID, componentID, artifactID, planID := ids.New(
		ids.KindService,
	), ids.New(
		ids.KindComponent,
	), ids.New(
		ids.KindConfig,
	), ids.New(
		ids.KindPlan,
	)
	taskID := ids.New(ids.KindTask)
	volumeDir := "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID
	platform, reference, found := registeredtunnel.Image.Select(runtime.GOOS, runtime.GOARCH)
	if !found {
		t.Fatal("unsupported test platform")
	}
	project := &composetypes.Project{
		Services: composetypes.Services{
			"cloudflare-tunnel": {
				Name:    "cloudflare-tunnel",
				Image:   reference,
				Command: []string{"tunnel", "--no-autoupdate", "run"},
			},
		},
	}
	render := controller.ComposeRenderInput{Project: project, ArtifactID: artifactID,
		ProjectOwnerKind: controller.ComposeProjectOwnerTenant, TenantID: tenantID, ProjectID: projectID, EnvironmentID: environmentID,
		PlanID: planID, RenderGeneration: 1, AuthorizedVolumeDir: volumeDir,
		Identities: controller.ComposeIdentitySnapshot{
			Services: []controller.ComposeResourceIdentity{
				{ID: serviceID, Name: "cloudflare-tunnel", ComponentID: componentID,
					ComponentImage: &controller.SelectedComponentImage{
						Repository:  registeredtunnel.Image.Repository,
						IndexDigest: registeredtunnel.Image.IndexDigest,
						Reference:   reference,
						Platform:    platform,
					}},
			},
		}}
	var previousRuntime *agentpb.ComposeArtifact
	var changes []etcd.EnvironmentBlueprintServiceChange
	if retainedNative {
		nativeID, releaseID := ids.New(ids.KindService), ids.New(ids.KindDeployment)
		replicas := 2
		project.Services["http-proof"] = composetypes.ServiceConfig{
			Name:   "http-proof",
			Image:  "example/http:1",
			Expose: []string{"8080"},
			Deploy: &composetypes.DeployConfig{Replicas: &replicas},
		}
		render.Identities.Services = append(
			render.Identities.Services,
			controller.ComposeResourceIdentity{ID: nativeID, Name: "http-proof"},
		)
		caddyPlatform, _, ok := registeredcaddy.Image.Select(runtime.GOOS, runtime.GOARCH)
		if !ok {
			t.Fatal("unsupported proxy platform")
		}
		render.Releases = map[string]controller.ComposeReleaseIdentity{nativeID: {
			ReleaseID: releaseID, Image: "sha256:" + strings.Repeat("d", 64), Strategy: domain.StrategyRecreate,
			Target: domain.WorkloadSingleton, ServingTarget: domain.WorkloadSingleton, ServingReleaseID: releaseID, ServingProxyGeneration: 1,
			ProxyImage: &etcd.ReleaseProxyImage{
				Repository:  registeredcaddy.Image.Repository,
				IndexDigest: registeredcaddy.Image.IndexDigest,
				Platform:    caddyPlatform,
			},
		}}
		var err error
		previousRuntime, err = controller.RenderCompose(render)
		if err != nil {
			t.Fatal(err)
		}
		render.Releases, render.RenderGeneration, render.PlanID = nil, 2, ids.New(ids.KindPlan)
		record := etcd.ServiceRecord{
			EnvironmentID: environmentID,
			Desired: core.Service{
				ID:       nativeID,
				Name:     "http-proof",
				Image:    "example/http:1",
				Replicas: 2,
				Expose:   []string{"8080"},
			},
			Runtime: core.ServiceRuntime{ServiceID: nativeID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
		}
		current := etcd.Versioned[etcd.ServiceRecord]{Record: record, Revision: 1, ReadRevision: 1}
		changes = []etcd.EnvironmentBlueprintServiceChange{{Current: &current, Record: record}}
	}
	artifact, err := controller.RenderCompose(render)
	if err != nil {
		t.Fatal(err)
	}
	if retainedNative {
		artifact, err = controller.RetainBlueprintNativeRuntime(
			artifact,
			previousRuntime,
			[]string{changes[0].Record.Desired.ID},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	artifactBytes, err := proto.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	planID = render.PlanID
	materialization := etcd.TaskMaterializationRecord{
		StepID:            ids.New(ids.KindStep),
		MaterializationID: ids.New(ids.KindConfig),
		EnvironmentID:     environmentID,
		Destination:       "blueprints/" + planID + "/blueprint.yaml",
		OutputKind:        etcd.TaskMaterializationOutputPlainFile,
		Mode:              0o444,
		SHA256:            hex.EncodeToString(make([]byte, sha256.Size)),
	}
	prefix, err := controller.BuildTaskMaterializationStep(materialization, artifactID, 120)
	if err != nil {
		t.Fatal(err)
	}
	projection := etcd.EnvironmentComposeProjection{
		EnvironmentID:     environmentID,
		RevisionID:        taskID,
		RenderGeneration:  render.RenderGeneration,
		ComposeArtifact:   artifactBytes,
		NormalizedCompose: []byte("services: {}\n"),
	}
	if firstEnableProjectionSource {
		digest := sha256.Sum256(artifactBytes)
		projection.ManagedComponentRuntimeSources = []etcd.ManagedComponentRuntimeSource{{
			ComponentKind: core.ComponentKindEdgeCloudflare, ComponentID: componentID, ServiceID: serviceID,
			ComposeName: "cloudflare-tunnel", RevisionID: taskID, ArtifactID: artifactID,
			ArtifactSHA256: hex.EncodeToString(digest[:]),
		}}
	}
	reader := &managedPlanReader{
		project: etcd.ProjectRecord{ID: projectID, TenantID: tenantID, Kind: etcd.ProjectKindTenant},
		environment: etcd.EnvironmentRecord{
			ID:                environmentID,
			ProjectID:         projectID,
			VolumeDir:         volumeDir,
			ProvisioningState: etcd.EnvironmentProvisioningReady,
		},
		projection: projection,
	}
	resolver, err := controller.NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	memberships, err := BuildNormalizedServiceMemberships(nil, &composetypes.Project{})
	if retainedNative {
		memberships, err = BuildNormalizedServiceMemberships(project, project)
	}
	if err != nil {
		t.Fatal(err)
	}
	task := etcd.TaskRecord{
		ID:               taskID,
		OperationID:      ids.New(ids.KindOperation),
		Executor:         etcd.TaskExecutorAgent,
		PlanID:           planID,
		RenderGeneration: int32(render.RenderGeneration),
		Type:             etcd.TaskUpdate,
		Target:           environmentID,
		TimeoutSeconds:   120,
		Materializations: []etcd.TaskMaterializationRecord{materialization},
		Params: map[string]string{
			taskcontract.EnvironmentBlueprintProcedureParam: string(
				taskcontract.BlueprintComposeProcedureNone,
			), etcd.EnvironmentDesiredRevisionParam: taskID,
			etcd.TaskMaterializationEnvironmentParam: environmentID, controller.EnvironmentBlueprintArtifactParam: artifactID},
	}
	producer := &Service{ledger: &etcd.ReleaseLedger{}, plans: resolver}
	prepared, err := producer.Prepare(
		context.Background(),
		PrepareInput{
			VolumeRoot:     "/var/lib/groundplane/vol",
			Projection:     projection,
			Memberships:    memberships,
			Task:           task,
			ServiceChanges: changes,
			PrefixSteps: []*agentpb.ExecutionStep{
				prefix,
			},
			Artifact:      artifact,
			CreatedAt:     at,
			AllocateNamed: func(kind ids.Kind, name string) string { return ids.DeriveAt(kind, at, taskID, name) },
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if retainedNative {
		for _, prior := range previousRuntime.Services {
			if prior.OwnerComponentId != "" {
				continue
			}
			matched := false
			for _, next := range prepared.Plan.Artifacts[0].Services {
				matched = matched || proto.Equal(prior, next)
			}
			if !matched {
				t.Fatalf(
					"managed-only producer lost serving native runtime authority for %s (role %s)",
					prior.ComposeName,
					prior.Role,
				)
			}
		}
	}
	if len(prepared.Plan.Steps) != 3 || prepared.Plan.Steps[1].GetComposeApply() == nil ||
		prepared.Plan.Steps[2].GetWaitHealthy() == nil {
		t.Fatalf("Tunnel-only producer omitted managed startup/observation: %v", prepared.Plan.Steps)
	}
	apply, health := prepared.Plan.Steps[1].GetComposeApply(), prepared.Plan.Steps[2].GetWaitHealthy()
	if apply.FullReconcile || !apply.NoDependencies || len(apply.ServiceIds) != 1 || apply.ServiceIds[0] != serviceID ||
		len(health.ServiceIds) != 1 ||
		health.ServiceIds[0] != serviceID {
		t.Fatal("managed lifecycle escaped exact service ownership")
	}
	encoded, err := json.Marshal(prepared.Task)
	if err != nil {
		t.Fatal(err)
	}
	var persisted etcd.TaskRecord
	if err := json.Unmarshal(encoded, &persisted); err != nil {
		t.Fatal(err)
	}
	replayed, err := resolver.ResolveExecutionPlan(context.Background(), persisted)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(prepared.Plan, replayed) {
		t.Fatal("persisted managed lifecycle replay changed sealed plan")
	}
	persisted.Steps[1].ID = ids.New(ids.KindStep)
	if _, err := resolver.ResolveExecutionPlan(context.Background(), persisted); err == nil {
		t.Fatal("replay accepted changed managed startup identity")
	}
}

type managedPlanReader struct {
	project     etcd.ProjectRecord
	environment etcd.EnvironmentRecord
	projection  etcd.EnvironmentComposeProjection
}

func (r *managedPlanReader) GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error) {
	return etcd.Versioned[etcd.TenantRecord]{}, nil
}
func (r *managedPlanReader) GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error) {
	return etcd.Versioned[etcd.ProjectRecord]{Record: r.project}, nil
}
func (r *managedPlanReader) GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return etcd.Versioned[etcd.EnvironmentRecord]{Record: r.environment}, nil
}

func (r *managedPlanReader) GetEnvironmentBlueprintRevision(
	context.Context,
	string,
	string,
) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error) {
	return etcd.Versioned[etcd.EnvironmentBlueprintRevision]{}, false, nil
}

func (r *managedPlanReader) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return etcd.Versioned[etcd.EnvironmentComposeProjection]{Record: r.projection}, true, nil
}

func (r *managedPlanReader) GetEnvironmentComposeProjectionRevision(
	context.Context,
	string,
	string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return etcd.Versioned[etcd.EnvironmentComposeProjection]{Record: r.projection}, true, nil
}
