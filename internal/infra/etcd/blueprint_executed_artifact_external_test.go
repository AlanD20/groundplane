package etcd_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// Rationale: only the artifact in the published and executed candidate plan may
// become restoration authority. Desired rendering lacks its workload identity.
func TestBlueprintPublisherTerminalPreservesExecutedArtifact(t *testing.T) {
	for _, addressable := range []bool{false, true} {
		name := "portless"
		if addressable {
			name = "addressable"
		}
		t.Run(name, func(t *testing.T) { testBlueprintExecutedArtifact(t, addressable, false) })
	}
}

func TestBlueprintRetainedProducerRejectsServingPointerRace(t *testing.T) {
	testBlueprintExecutedArtifact(t, true, true)
}

// QA: BP-04, SVC-15, JOURNEY-02; real publisher and in-memory durable completion,
// not host execution or a live failed-rollout recovery pass.
// Rationale: Blueprint must seed the same runtime authority as Deploy; later
// configuration-only work cannot replace that acknowledgement.
func TestBlueprintSuccessRetainsAcknowledgedServiceRuntime(t *testing.T) {
	for _, addressable := range []bool{false, true} {
		name := "portless"
		if addressable {
			name = "with proxy"
		}
		t.Run(name, func(t *testing.T) {
			testBlueprintExecutedArtifact(t, addressable, false, func(fixture *etcd.ExecutedArtifactFixture,
				_ *controller.TaskPlanResolver, render etcd.ReleaseRenderInput, intent domain.Intent,
				_ *agentpb.ComposeArtifact) {
				receipt := fixture.AcknowledgedRuntime(t, render.ServiceID)
				if receipt.Record.Source.TaskID != intent.OriginatingTaskID || receipt.Record.Source.PlanID != render.PlanID ||
					receipt.Record.Source.ExecutionEpoch != 1 || receipt.Record.Source.RenderGeneration != 1 ||
					receipt.Record.Runtime.ReleaseID != render.ReleaseID ||
					receipt.Record.Runtime.Target != "singleton" {
					t.Fatal("acknowledgement lost Blueprint identity or was replaced by configuration-only work")
				}
				artifact := &agentpb.ComposeArtifact{}
				if err := proto.Unmarshal(receipt.Record.Runtime.CurrentArtifact, artifact); err != nil {
					t.Fatal(err)
				}
				workloads, proxies := 0, 0
				for _, service := range artifact.Services {
					if service.ServiceId != render.ServiceID {
						t.Fatal("per-Service runtime retained an unrelated Service")
					}
					if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
						proxies++
						continue
					}
					workloads++
					if service.ExpectedReplicas != 2 || service.ImageReference != "sha256:"+strings.Repeat("a", 64) {
						t.Fatal("runtime did not retain the executed two-replica image")
					}
				}
				if workloads != 1 || (proxies == 1) != addressable || proxies > 1 {
					t.Fatalf("runtime selection: workloads=%d proxies=%d", workloads, proxies)
				}
			})
		})
	}
}

func testBlueprintExecutedArtifact(
	t *testing.T,
	addressable, servingRace bool,
	afterProof ...func(*etcd.ExecutedArtifactFixture, *controller.TaskPlanResolver, etcd.ReleaseRenderInput, domain.Intent, *agentpb.ComposeArtifact),
) {
	testBlueprintExecutedArtifactConfigured(t, addressable, servingRace, nil, afterProof...)
}

func testBlueprintExecutedArtifactConfigured(
	t *testing.T,
	addressable, servingRace bool,
	configureProject func(*composetypes.Project),
	afterProof ...func(*etcd.ExecutedArtifactFixture, *controller.TaskPlanResolver, etcd.ReleaseRenderInput, domain.Intent, *agentpb.ComposeArtifact),
) {
	ctx := context.Background()
	fixture := etcd.NewExecutedArtifactFixture(t)
	task := fixture.Task(t, 950)
	serviceID, releaseID, artifactID := ids.New(ids.KindService), ids.New(ids.KindDeployment), ids.New(ids.KindConfig)
	task.Params[etcd.TaskComposeArtifactParam] = artifactID
	task.Params[etcd.TaskReleasePublicationParam] = ids.NewULID()
	workload := domain.WorkloadSeal{
		RequestedReference: "example/api:1",
		LocalImageID:       "sha256:" + strings.Repeat("a", 64),
		ReplicaCount:       2,
	}
	replicas := 2
	project := &composetypes.Project{
		Name: "test",
		Services: composetypes.Services{
			"api": {
				Name:        "api",
				Image:       workload.RequestedReference,
				NetworkMode: "none",
				Scale:       &replicas,
				HealthCheck: &composetypes.HealthCheckConfig{Test: []string{"CMD", "true"}},
			},
		},
	}
	if addressable {
		service := project.Services["api"]
		service.Expose = []string{"8080"}
		project.Services["api"] = service
	}
	if configureProject != nil {
		configureProject(project)
	}
	normalized, err := project.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := controller.RenderCompose(controller.ComposeRenderInput{Project: project, ArtifactID: artifactID,
		ProjectOwnerKind: controller.ComposeProjectOwnerTenant, TenantID: fixture.Project.Record.TenantID, ProjectID: fixture.Project.Record.ID,
		EnvironmentID: fixture.Environment.Record.ID, PlanID: task.PlanID, RenderGeneration: 1, AuthorizedVolumeDir: fixture.Environment.Record.VolumeDir,
		Identities: controller.ComposeIdentitySnapshot{
			Services: []controller.ComposeResourceIdentity{{ID: serviceID, Name: "api"}},
		}})
	if err != nil {
		t.Fatal(err)
	}
	desiredBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	projection := etcd.EnvironmentComposeProjection{
		EnvironmentID:     fixture.Environment.Record.ID,
		RevisionID:        task.ID,
		RenderGeneration:  1,
		ComposeArtifact:   desiredBytes,
		NormalizedCompose: normalized,
		DesiredServices: []etcd.EnvironmentServiceProjection{{EnvironmentID: fixture.Environment.Record.ID,
			Desired: core.Service{
				ID:       serviceID,
				Name:     "api",
				Image:    workload.RequestedReference,
				Replicas: 2,
				Strategy: core.StrategyRecreate,
			}}},
	}
	render := etcd.ReleaseRenderInput{
		ReleaseID:           releaseID,
		PlanID:              task.PlanID,
		ArtifactID:          artifactID,
		ServiceID:           serviceID,
		ServiceName:         "api",
		CandidateWorkload:   workload,
		Strategy:            domain.StrategyRecreate,
		PriorStrategy:       domain.StrategyRecreate,
		CandidateTarget:     domain.WorkloadSingleton,
		PriorTarget:         domain.WorkloadSingleton,
		TenantID:            fixture.Project.Record.TenantID,
		TenantSlug:          "acme",
		ProjectID:           fixture.Project.Record.ID,
		ProjectSlug:         fixture.Project.Record.Slug,
		EnvironmentID:       fixture.Environment.Record.ID,
		EnvironmentName:     fixture.Environment.Record.Name,
		AuthorizedVolumeDir: fixture.Environment.Record.VolumeDir,
		Projection:          projection,
	}
	resolver, err := controller.NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", fixture.Hierarchy, nil)
	if err != nil {
		t.Fatal(err)
	}
	if addressable {
		projection.DesiredServices[0].Desired.Expose = []string{"8080"}
		render.ProxyPorts, render.ProxyGeneration = []uint16{8080}, 1
		config, configErr := domain.RenderProxyConfig(
			render.ServiceName,
			render.ReleaseID,
			render.CandidateTarget,
			1,
			render.ProxyPorts,
		)
		if configErr != nil {
			t.Fatal(configErr)
		}
		render.ProxyConfigDigest = hex.EncodeToString(config.SHA256[:])
		if err := resolver.EnableServiceProxyImage(blueprintTestProxyCatalog("b")); err != nil {
			t.Fatal(err)
		}
		if err := resolver.PrepareReleaseProxyImage(&render, nil); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := etcd.EncodeReleaseRenderInput(render)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := domain.Digest(json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.Intent{
		ID:                releaseID,
		EnvironmentID:     render.EnvironmentID,
		ServiceID:         serviceID,
		OperationID:       task.OperationID,
		OperationKind:     domain.OperationBlueprintApply,
		CandidateWorkload: workload,
		Tag:               "1",
		Strategy:          domain.StrategyRecreate,
		OnFailure:         domain.OnFailureSwitchBack,
		RenderInputID:     artifactID,
		RenderInputDigest: digest,
		CreatedAt:         task.CreatedAt,
		Actor:             "operator",
		OriginatingTaskID: task.ID,
		Workspace: domain.Workspace{
			Kind:          domain.WorkspaceTenant,
			TenantID:      render.TenantID,
			ProjectID:     render.ProjectID,
			EnvironmentID: render.EnvironmentID,
		},
	}
	manifest, err := fixture.Ledger.Stage(
		ctx,
		etcd.ReleaseStage{
			PublicationID: task.Params[etcd.TaskReleasePublicationParam],
			OperationID:   task.OperationID,
			CreatedAt:     task.CreatedAt,
			Members: []etcd.ReleaseStageMember{
				{
					Intent:      intent,
					RenderInput: raw,
					Checkpoint: domain.Checkpoint{
						ReleaseID: releaseID,
						State:     domain.StatePending,
						UpdatedAt: task.CreatedAt,
					},
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	task, plan, err := resolver.PrepareBlueprintReleaseTask(
		ctx,
		task,
		controller.BlueprintReleasePlanInput{Members: []etcd.ReleaseTaskRenderMember{{Intent: intent, Render: render}},
			ApplyStepIDs: []string{
				ids.New(ids.KindStep),
			}, HealthStepIDs: []string{ids.New(ids.KindStep)}, RecoveryProbeStepIDs: []string{ids.New(ids.KindStep)}, RecoveryCompensateStepIDs: []string{ids.New(ids.KindStep)}, PostStepIDs: [][]string{nil}},
	)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := executionplan.DescribeCandidateRelease(plan)
	if err != nil {
		t.Fatal(err)
	}
	changedPlan := proto.CloneOf(plan)
	changedPlan.Artifacts[0].Services[0].ImageReference = "sha256:" + strings.Repeat("b", 64)
	if _, err := fixture.Ledger.PrepareBlueprintReleasePublication(ctx, nil, etcd.BlueprintReleasePublicationEvidence{
		Manifest: manifest, EnvironmentID: render.EnvironmentID, Task: task, CandidateReleaseDescriptor: descriptor,
		Plan: changedPlan, PublishedAt: task.CreatedAt,
	}); err == nil {
		t.Fatal("publisher accepted artifact bytes outside the exact prepared plan hash")
	}
	publication, err := fixture.Ledger.PrepareBlueprintReleasePublication(
		ctx,
		nil,
		etcd.BlueprintReleasePublicationEvidence{
			Manifest:                   manifest,
			EnvironmentID:              render.EnvironmentID,
			Task:                       task,
			CandidateReleaseDescriptor: descriptor,
			Plan:                       plan,
			PublishedAt:                task.CreatedAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Publish(t, task, projection, publication)
	executed, err := proto.MarshalOptions{Deterministic: true}.Marshal(plan.Artifacts[0])
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(executed, desiredBytes) {
		t.Fatal("regression fixture did not distinguish desired and executed artifacts")
	}
	agentID := ids.New(ids.KindAgent)
	ack := func(task etcd.TaskRecord) {
		claim, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
		if err != nil || !found {
			t.Fatalf("claim = %t, %v", found, err)
		}
		_, err = fixture.Tasks.AcknowledgeTask(
			ctx,
			agentID,
			1,
			task.ID,
			claim.Assignment.Record.AssignmentID,
			etcd.TaskStatusCompleted,
			etcd.TaskResultRecord{
				Kind:           etcd.TaskResultCompose,
				ExecutionEpoch: 1,
				Diagnostic:     etcd.TaskResultDiagnosticNone,
			},
			task.CreatedAt.Add(2*time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	ack(task)
	applied, found, err := fixture.Hierarchy.GetEnvironmentAppliedComposeProjection(ctx, render.EnvironmentID)
	if err != nil || !found || !bytes.Equal(applied.Record.ComposeArtifact, executed) {
		t.Fatalf("applied artifact differs from executed plan: found=%t err=%v", found, err)
	}
	actual := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(applied.Record.ComposeArtifact, actual); err != nil {
		t.Fatal(err)
	}
	var actualWorkload *agentpb.ComposeService
	for _, service := range actual.Services {
		if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
			actualWorkload = service
		}
	}
	if actualWorkload == nil || actualWorkload.ImageReference != workload.LocalImageID {
		t.Fatal("applied artifact lost workload identity")
	}
	labelFound := false
	for _, label := range actualWorkload.ExpectedLabels {
		if label.Key == "com.groundplane.release-id" && label.Value == releaseID {
			labelFound = true
		}
	}
	if !labelFound {
		t.Fatal("applied artifact lost exact serving Release label")
	}
	desired, found, err := fixture.Hierarchy.GetEnvironmentComposeProjectionRevision(ctx, render.EnvironmentID, task.ID)
	if err != nil || !found || !bytes.Equal(desired.Record.ComposeArtifact, desiredBytes) {
		t.Fatal("immutable desired projection was rewritten")
	}
	configuration := fixture.Task(t, 951)
	configuration.RenderGeneration = 2
	projection.RevisionID = configuration.ID
	projection.RenderGeneration = 2
	fixture.Publish(t, configuration, projection, etcd.BlueprintReleasePublication{})
	ack(configuration)
	after, found, err := fixture.Hierarchy.GetEnvironmentAppliedComposeProjection(ctx, render.EnvironmentID)
	if err != nil || !found || after.Revision != applied.Revision ||
		!bytes.Equal(after.Record.ComposeArtifact, executed) {
		t.Fatal("configuration-only Apply changed acknowledged workload authority")
	}
	// The next changed candidate's fixed-revision planning view must pair its
	// historical serving intent with those same acknowledged execution bytes.
	scope, err := fixture.Ledger.LoadPlanningScope(ctx, render.EnvironmentID)
	if err != nil {
		t.Fatal(err)
	}
	services, err := fixture.Ledger.LoadPlanningServices(ctx, scope, []string{serviceID})
	if err != nil {
		t.Fatal(err)
	}
	serving, found, err := fixture.Ledger.GetPlanningServingIntent(ctx, scope, services[0])
	if err != nil || !found || serving.ID != releaseID || serving.CandidateWorkload != workload {
		t.Fatalf("next candidate serving predecessor: found=%t err=%v", found, err)
	}
	predecessor, found, err := fixture.Ledger.GetPlanningAppliedProjection(ctx, scope)
	if err != nil || !found || predecessor.Revision != applied.Revision ||
		!bytes.Equal(predecessor.Record.ComposeArtifact, executed) {
		t.Fatal("next candidate did not capture exact acknowledged artifact")
	}
	if len(afterProof) != 0 {
		afterProof[0](fixture, resolver, render, intent, actual)
		return
	}
	if addressable {
		proveSecondAddressableBlueprint(t, fixture, resolver, render, intent, actual)
	}
	proveRetainedBlueprintProducer(t, fixture, resolver, project, serviceID, servingRace)
}

func blueprintTestProxyCatalog(child string) componentsdk.OCIImage {
	return componentsdk.OCIImage{Repository: "docker.io/library/caddy", IndexDigest: strings.Repeat("a", 64),
		Platforms: []componentsdk.OCIPlatform{
			{
				OS:           "linux",
				Architecture: "amd64",
				ChildDigest:  strings.Repeat(child, 64),
				ConfigDigest: strings.Repeat("c", 64),
			},
			{
				OS:           "linux",
				Architecture: "arm64",
				Variant:      "v8",
				ChildDigest:  strings.Repeat(child+"1", 32),
				ConfigDigest: strings.Repeat("f", 64),
			},
		}}
}

func proveSecondAddressableBlueprint(
	t *testing.T,
	fixture *etcd.ExecutedArtifactFixture,
	resolver *controller.TaskPlanResolver,
	prior etcd.ReleaseRenderInput,
	priorIntent domain.Intent,
	first *agentpb.ComposeArtifact,
) {
	t.Helper()
	ctx := context.Background()
	task := fixture.Task(t, 952)
	task.RenderGeneration = 3
	task.Params[etcd.TaskReleasePublicationParam] = ids.NewULID()
	task.Params[etcd.TaskComposeArtifactParam] = ids.New(ids.KindConfig)
	render := prior
	render.ReleaseID, render.PlanID, render.ArtifactID = ids.New(
		ids.KindDeployment,
	), task.PlanID, task.Params[etcd.TaskComposeArtifactParam]
	render.Projection.RevisionID, render.Projection.RenderGeneration = task.ID, 3
	render.PriorArtifactID, render.PriorWorkload = first.ArtifactId, &prior.CandidateWorkload
	render.PriorProxyGeneration, render.PriorProxyDigest = prior.ProxyGeneration, prior.ProxyConfigDigest
	render.ProxyGeneration = prior.ProxyGeneration + 1
	config, err := domain.RenderProxyConfig(
		render.ServiceName,
		render.ReleaseID,
		render.CandidateTarget,
		render.ProxyGeneration,
		render.ProxyPorts,
	)
	if err != nil {
		t.Fatal(err)
	}
	render.ProxyConfigDigest = hex.EncodeToString(config.SHA256[:])
	if err := resolver.EnableServiceProxyImage(blueprintTestProxyCatalog("d")); err != nil {
		t.Fatal(err)
	}
	if err := resolver.PrepareReleaseProxyImage(&render, &prior); err != nil {
		t.Fatal(err)
	}
	if *render.ProxyImage != *prior.ProxyImage {
		t.Fatal("successor selected today's proxy catalog")
	}
	raw, err := etcd.EncodeReleaseRenderInput(render)
	if err != nil {
		t.Fatal(err)
	}
	intent := priorIntent
	intent.ID, intent.OperationID, intent.OriginatingTaskID = render.ReleaseID, task.OperationID, task.ID
	intent.CreatedAt = task.CreatedAt
	intent.PriorServingReleaseID, intent.PriorSuccessfulReleaseID = prior.ReleaseID, prior.ReleaseID
	intent.RenderInputID = render.ArtifactID
	intent.RenderInputDigest, err = domain.Digest(json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := fixture.Ledger.Stage(
		ctx,
		etcd.ReleaseStage{
			PublicationID: task.Params[etcd.TaskReleasePublicationParam],
			OperationID:   task.OperationID,
			CreatedAt:     task.CreatedAt,
			Members: []etcd.ReleaseStageMember{
				{
					Intent:      intent,
					RenderInput: raw,
					Checkpoint: domain.Checkpoint{
						ReleaseID: intent.ID,
						State:     domain.StatePending,
						UpdatedAt: task.CreatedAt,
					},
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	task, plan, err := resolver.PrepareBlueprintReleaseTask(
		ctx,
		task,
		controller.BlueprintReleasePlanInput{Members: []etcd.ReleaseTaskRenderMember{{Intent: intent, Render: render}},
			ApplyStepIDs: []string{
				ids.New(ids.KindStep),
			}, HealthStepIDs: []string{ids.New(ids.KindStep)}, RecoveryProbeStepIDs: []string{ids.New(ids.KindStep)}, RecoveryCompensateStepIDs: []string{ids.New(ids.KindStep)}, PostStepIDs: [][]string{nil}},
	)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := executionplan.DescribeCandidateRelease(plan)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := fixture.Ledger.PrepareBlueprintReleasePublication(
		ctx,
		nil,
		etcd.BlueprintReleasePublicationEvidence{
			Manifest:                   manifest,
			EnvironmentID:              render.EnvironmentID,
			Task:                       task,
			Plan:                       plan,
			CandidateReleaseDescriptor: descriptor,
			PublishedAt:                task.CreatedAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Publish(t, task, render.Projection, publication)
	agentID := ids.New(ids.KindAgent)
	claim, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("successor claim=%t,%v", found, err)
	}
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, etcd.TaskStatusCompleted,
		etcd.TaskResultRecord{Kind: etcd.TaskResultCompose, ExecutionEpoch: 1, Diagnostic: etcd.TaskResultDiagnosticNone}, task.CreatedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	firstProxy, secondProxy := blueprintProxyMetadata(t, first), blueprintProxyMetadata(t, plan.Artifacts[0])
	if firstProxy.ImageReference != secondProxy.ImageReference ||
		!bytes.Equal(firstProxy.ImageConfigDigest, secondProxy.ImageConfigDigest) ||
		bytes.Equal(firstProxy.ProxyConfigSha256, secondProxy.ProxyConfigSha256) ||
		hex.EncodeToString(secondProxy.ProxyConfigSha256) != render.ProxyConfigDigest {
		t.Fatal("successor did not preserve frozen proxy image and advance exact config")
	}
	applied, found, err := fixture.Hierarchy.GetEnvironmentAppliedComposeProjection(ctx, render.EnvironmentID)
	if err != nil || !found {
		t.Fatalf("successor applied=%t,%v", found, err)
	}
	secondBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(plan.Artifacts[0])
	if err != nil || !bytes.Equal(applied.Record.ComposeArtifact, secondBytes) {
		t.Fatal("successor acknowledged artifact differs from published proxy plan")
	}
	stored, err := fixture.Ledger.GetReleaseRenderInputAt(ctx, intent.ID, applied.ReadRevision)
	if err != nil || stored.Record.ProxyImage == nil || *stored.Record.ProxyImage != *prior.ProxyImage ||
		stored.Record.PriorProxyDigest != prior.ProxyConfigDigest ||
		stored.Record.ProxyConfigDigest != render.ProxyConfigDigest {
		t.Fatalf("successor frozen proxy render diverges: %v", err)
	}
}

func blueprintProxyMetadata(t *testing.T, artifact *agentpb.ComposeArtifact) *agentpb.ComposeService {
	t.Helper()
	for _, service := range artifact.Services {
		if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			return service
		}
	}
	t.Fatal("stable proxy absent")
	return nil
}
