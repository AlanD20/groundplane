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

	component "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterializationowner "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// A Tunnel-only Blueprint must run and observe its managed Service, even when
// no native Service Release or owned Zone exists. Restart uses persisted input.
func TestPrepareTunnelOnlyStartsManagedServiceAndReplaysPersistedPlan(t *testing.T) {
	testPrepareManagedService(t, false, false, false)
}

func TestPrepareManagedOnlyAcceptsReadOnlyRetainedNativeLabels(t *testing.T) {
	testPrepareManagedService(t, true, false, false)
}

func TestPrepareManagedOnlyFirstEnableDoesNotTeardownCandidate(t *testing.T) {
	testPrepareManagedService(t, false, true, false)
}

func TestPrepareManagedOnlyRetainsExactComponentOwnership(t *testing.T) {
	testPrepareManagedService(t, false, false, true)
}

// Full preflight-to-publication coverage lives in the real Release publisher
// fixture; this focused test receives the artifact already merged before Stage.
func testPrepareManagedService(
	t *testing.T,
	retainedNative bool,
	firstEnableProjectionSource bool,
	retainedComponent bool,
) {
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
	managedImage := managedServiceTestImage()
	platform, reference, found := managedImage.Select(runtime.GOOS, runtime.GOARCH)
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
	render := testcomposerender.ComposeRenderInput{Project: project, ArtifactID: artifactID,
		ProjectOwnerKind: testcomposerender.ComposeProjectOwnerTenant, TenantID: tenantID, ProjectID: projectID, EnvironmentID: environmentID,
		PlanID: planID, RenderGeneration: 1, AuthorizedVolumeDir: volumeDir,
		Identities: testcomposeidentity.Snapshot{
			Services: []testcomposeidentity.Resource{
				{ID: serviceID, Name: "cloudflare-tunnel", ComponentID: componentID,
					ComponentImage: &testcomposeidentity.ComponentImage{
						Repository:  managedImage.Repository,
						IndexDigest: managedImage.IndexDigest,
						Reference:   reference,
						Platform:    platform,
					}},
			},
		}}
	var previousRuntime *agentpb.ComposeArtifact
	var changes []testblueprints.EnvironmentBlueprintServiceChange
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
			render.Identities.Services, testcomposeidentity.Resource{ID: nativeID, Name: "http-proof"},
		)
		routerImage := routerServiceTestImage()
		caddyPlatform, _, ok := routerImage.Select(runtime.GOOS, runtime.GOARCH)
		if !ok {
			t.Fatal("unsupported proxy platform")
		}
		render.Releases = map[string]testcomposerender.ComposeReleaseIdentity{nativeID: {
			ReleaseID: releaseID, Image: "sha256:" + strings.Repeat("d", 64), Strategy: domain.StrategyRecreate,
			Target: domain.WorkloadSingleton, ServingTarget: domain.WorkloadSingleton, ServingReleaseID: releaseID, ServingProxyGeneration: 1,
			ProxyImage: &domain.ProxyImage{
				Repository:  routerImage.Repository,
				IndexDigest: routerImage.IndexDigest,
				Platform:    caddyPlatform,
			},
		}}
		var err error
		previousRuntime, err = testcomposerender.RenderCompose(render)
		if err != nil {
			t.Fatal(err)
		}
		render.Releases, render.RenderGeneration, render.PlanID = nil, 2, ids.New(ids.KindPlan)
		record := testservices.ServiceRecord{
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
		current := testkeyvalue.Versioned[testservices.ServiceRecord]{Record: record, Revision: 1, ReadRevision: 1}
		changes = []testblueprints.EnvironmentBlueprintServiceChange{{Current: &current, Record: record}}
	}
	artifact, err := testcomposerender.RenderCompose(render)
	if err != nil {
		t.Fatal(err)
	}
	if retainedComponent {
		previousRuntime = proto.CloneOf(artifact)
		render.RetainedComponentRuntime, err = proto.Marshal(artifact)
		if err != nil {
			t.Fatal(err)
		}
		render.PlanID, render.RenderGeneration = ids.New(ids.KindPlan), 2
		artifact, err = testcomposerender.RenderCompose(render)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(artifact.Services[0], previousRuntime.Services[0]) {
			t.Fatal("file-only Component render changed ownership")
		}
	}
	if retainedNative {
		artifact, err = testcomposerender.RetainBlueprintNativeRuntime(
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
	materialization := testtaskmaterializationowner.Record{
		StepID:            ids.New(ids.KindStep),
		MaterializationID: ids.New(ids.KindConfig),
		EnvironmentID:     environmentID,
		Destination:       "blueprints/" + planID + "/blueprint.yaml",
		OutputKind:        testtaskmaterializationowner.OutputPlainFile,
		Mode:              0o444,
		SHA256:            hex.EncodeToString(make([]byte, sha256.Size)),
	}
	prefix, err := testtaskmaterialization.BuildTaskMaterializationStep(materialization, artifactID, 120)
	if err != nil {
		t.Fatal(err)
	}
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID:     environmentID,
		RevisionID:        taskID,
		RenderGeneration:  render.RenderGeneration,
		ComposeArtifact:   artifactBytes,
		NormalizedCompose: []byte("services: {}\n"),
	}
	if firstEnableProjectionSource {
		digest := sha256.Sum256(artifactBytes)
		projection.ManagedComponentRuntimeSources = []testenvironmentprojection.ManagedComponentRuntimeSource{{
			ComponentKind: core.ComponentKindEdgeCloudflare, ComponentID: componentID, ServiceID: serviceID,
			ComposeName: "cloudflare-tunnel", RevisionID: taskID, ArtifactID: artifactID,
			ArtifactSHA256: hex.EncodeToString(digest[:]),
		}}
	}
	reader := &managedPlanReader{
		project: testhierarchy.ProjectRecord{ID: projectID, TenantID: tenantID, Kind: testhierarchy.ProjectKindTenant},
		environment: testhierarchy.EnvironmentRecord{
			ID:                environmentID,
			ProjectID:         projectID,
			VolumeDir:         volumeDir,
			ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		},
		projection: projection,
	}
	resolver, err := testtaskplanning.NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
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
		Executor:         testtaskjournal.TaskExecutorAgent,
		PlanID:           planID,
		RenderGeneration: int32(render.RenderGeneration),
		Type:             testtaskjournal.TaskUpdate,
		Target:           environmentID,
		TimeoutSeconds:   120,
		Materializations: []testtaskmaterializationowner.Record{materialization},
		Params: map[string]string{
			taskcontract.EnvironmentBlueprintProcedureParam: string(
				taskcontract.BlueprintComposeProcedureNone,
			), testblueprints.EnvironmentDesiredRevisionParam: taskID, testtaskjournal.TaskMaterializationEnvironmentParam: environmentID, taskcontract.EnvironmentBlueprintArtifactParam: artifactID},
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
			Artifact:  artifact,
			CreatedAt: at,
			AllocateNamed: func(kind ids.Kind, name string) string {
				return ids.DeriveAt(kind, at, taskID, name)
			},
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
	if retainedComponent {
		for _, mutation := range []string{"force", "dependencies", "broad", "native", "future-generation"} {
			invalid := proto.CloneOf(prepared.Plan)
			switch mutation {
			case "force":
				invalid.Steps[1].GetComposeApply().ForceRecreate = true
			case "dependencies":
				invalid.Steps[1].GetComposeApply().NoDependencies = false
			case "broad":
				invalid.Steps[1].GetComposeApply().FullReconcile = true
				invalid.Steps[1].GetComposeApply().ServiceIds = nil
			case "native":
				invalid.Artifacts[0].Services[0].OwnerComponentId = ""
			case "future-generation":
				for _, label := range invalid.Artifacts[0].Services[0].ExpectedLabels {
					if label.Key == "com.groundplane.render-generation" {
						label.Value = "3"
					}
				}
			}
			if _, err := executionplan.Seal(invalid); err == nil {
				t.Fatalf("retained Component accepted %s", mutation)
			}
		}
	}
	persisted.Steps[1].ID = ids.New(ids.KindStep)
	if _, err := resolver.ResolveExecutionPlan(context.Background(), persisted); err == nil {
		t.Fatal("replay accepted changed managed startup identity")
	}
}

const (
	testManagedServiceImageRepository     = "docker.io/cloudflare/cloudflared"
	testManagedServiceImageIndexDigest    = "4f6655284ab3d252b7f28fedb19fe6c8fc82ee5b1295c20ac74d475e5398a52d"
	testManagedServiceAMD64ManifestDigest = "18626b1baac4450214535cd5bc40ef44c0635244d585ebf707749c22b6f3408f"
	testManagedServiceAMD64ConfigDigest   = "e871921d7924ab4baa36da9938ecddb86025b5b1aa930500769456bb24f50a75"
	testManagedServiceARM64ManifestDigest = "a85d5a3d6f22cb3c7e78b2f0d05b0f0daeb72566e9426f656c60b357b7b89c95"
	testManagedServiceARM64ConfigDigest   = "5d249c08c07ddc00eb501917e030874347a8ea786bdb644bb2cff92a4cd2b843"
	testRouterServiceImageRepository      = "docker.io/library/caddy"
	testRouterServiceImageIndexDigest     = "5f5c8640aae01df9654968d946d8f1a56c497f1dd5c5cda4cf95ab7c14d58648"
	testRouterServiceAMD64ManifestDigest  = "98eb57d882ccd5213d1688764db10c1ca2c58a1ca3a6717a3411ad798f7a423a"
	testRouterServiceAMD64ConfigDigest    = "af555904a0961945f16bb323a501457b13a4f7e9bde969b145b97da80b38ecbe"
	testRouterServiceARM64ManifestDigest  = "1172d4213087d3fc30bafc7ff2c2896180eb0c41ff7f75f315568fb36cabdcba"
	testRouterServiceARM64ConfigDigest    = "6b08c1b9858ca9a7d99c1da13c3695081e0e604c6cf214ca26a7ce0e2c4fd9b4"
)

func managedServiceTestImage() component.OCIImage {
	return component.OCIImage{
		Repository:  testManagedServiceImageRepository,
		IndexDigest: testManagedServiceImageIndexDigest,
		Platforms: []component.OCIPlatform{
			{
				OS: "linux", Architecture: "amd64",
				ChildDigest:  testManagedServiceAMD64ManifestDigest,
				ConfigDigest: testManagedServiceAMD64ConfigDigest,
			},
			{
				OS: "linux", Architecture: "arm64",
				ChildDigest:  testManagedServiceARM64ManifestDigest,
				ConfigDigest: testManagedServiceARM64ConfigDigest,
			},
		},
	}
}

func routerServiceTestImage() component.OCIImage {
	return component.OCIImage{
		Repository:  testRouterServiceImageRepository,
		IndexDigest: testRouterServiceImageIndexDigest,
		Platforms: []component.OCIPlatform{
			{
				OS: "linux", Architecture: "amd64",
				ChildDigest:  testRouterServiceAMD64ManifestDigest,
				ConfigDigest: testRouterServiceAMD64ConfigDigest,
			},
			{
				OS: "linux", Architecture: "arm64", Variant: "v8",
				ChildDigest:  testRouterServiceARM64ManifestDigest,
				ConfigDigest: testRouterServiceARM64ConfigDigest,
			},
		},
	}
}

type managedPlanReader struct {
	project     testhierarchy.ProjectRecord
	environment testhierarchy.EnvironmentRecord
	projection  testenvironmentprojection.EnvironmentComposeProjection
}

func (r *managedPlanReader) GetTenant(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.TenantRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.TenantRecord]{}, nil
}

func (r *managedPlanReader) GetProject(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.ProjectRecord]{Record: r.project}, nil
}

func (r *managedPlanReader) GetEnvironment(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{Record: r.environment}, nil
}

func (r *managedPlanReader) GetEnvironmentBlueprintRevision(
	context.Context, string, string,

) (testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision], bool, error) {
	return testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision]{}, false, nil
}

func (r *managedPlanReader) GetEnvironmentComposeProjection(
	context.Context, string,

) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: r.projection,
	}, true, nil
}

func (r *managedPlanReader) GetEnvironmentComposeProjectionRevision(
	context.Context, string, string,

) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: r.projection,
	}, true, nil
}
