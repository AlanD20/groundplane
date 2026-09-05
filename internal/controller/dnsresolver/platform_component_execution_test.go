package dnsresolver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"runtime"
	"strings"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"google.golang.org/protobuf/proto"
)

type executionRepositoryStub struct {
	input    etcd.PlatformComponentTaskRenderInput
	current  etcd.Versioned[etcd.ComponentRecord]
	baseline etcd.Versioned[etcd.HostResolverBaselineRecord]
}

func (repository executionRepositoryStub) GetPlatformComponentTaskRenderInput(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.PlatformComponentTaskRenderInput], error) {
	return etcd.Versioned[etcd.PlatformComponentTaskRenderInput]{Record: repository.input}, nil
}

func (repository executionRepositoryStub) GetComponent(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.ComponentRecord], error) {
	return repository.current, nil
}

func (repository executionRepositoryStub) GetHostResolverBaseline(
	_ context.Context,
) (etcd.Versioned[etcd.HostResolverBaselineRecord], bool, error) {
	return repository.baseline, true, nil
}

type executionCatalogStub struct {
	plan componentsdk.EnvironmentPlan
}

// Rationale: a disable Task removes the already-serving Compose artifact, so
// dispatch must reconstruct the exact sealed predecessor ownership and hash.
func TestPlatformExecutionDisableReusesSealedPredecessorOwnership(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0).UTC()
	componentID := ids.NewAt(ids.KindComponent, now, 1)
	serviceID := ids.NewAt(ids.KindService, now, 2)
	taskPlanID := ids.NewAt(ids.KindPlan, now, 3)
	predecessorPlanID := ids.NewAt(ids.KindPlan, now, 10)
	taskID := ids.NewAt(ids.KindTask, now, 4)
	component := core.Component{
		ID: componentID, Owner: core.ComponentOwnerPlatform, Kind: core.ComponentKindCoreDNS,
		Enabled: true, GeneratedServices: []string{serviceID}, Healthy: true,
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
			CorefileTemplate: testCorefileTemplate,
			UpstreamAuto:     true,
		}},
	}
	record, err := etcd.NewComponentRecord(component)
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	desiredDigest, err := etcd.PlatformComponentDesiredDigest(record)
	if err != nil {
		t.Fatalf("PlatformComponentDesiredDigest() error = %v", err)
	}
	registeredPlan := executionPlanFixture(serviceID, "example/resolver@sha256:"+strings.Repeat("1", 64))
	selectedPlatform, selectedReference, selected := registeredPlan.Services[0].Image.Select(
		runtime.GOOS,
		runtime.GOARCH,
	)
	if !selected {
		t.Fatalf("execution fixture does not support %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	artifactDigest := sha256.Sum256(registeredPlan.Files[0].Content)
	registeredPlanDigest := componentsdk.DigestEnvironmentPlan(registeredPlan)
	input := etcd.PlatformComponentTaskRenderInput{
		PlanID:                         taskPlanID,
		TaskID:                         taskID,
		ComponentID:                    componentID,
		DesiredSHA256:                  desiredDigest,
		BaselineGeneration:             1,
		BaselineSHA256:                 strings.Repeat("2", 64),
		Config:                         *component.Config.CoreDNS,
		HostResolutionInputRevision:    1,
		HostResolutionSHA256:           strings.Repeat("5", 64),
		GeneratedServiceID:             serviceID,
		DisableService:                 true,
		DefinitionSHA256:               strings.Repeat("3", 64),
		CatalogSHA256:                  strings.Repeat("4", 64),
		ActionID:                       "activate-config",
		ArtifactID:                     ids.NewAt(ids.KindConfig, now, 6),
		ComposeArtifactID:              ids.NewAt(ids.KindConfig, now, 7),
		OwnershipPlanID:                predecessorPlanID,
		OwnershipGeneration:            7,
		PriorObservationModRevision:    11,
		PriorObservationRevision:       11,
		PredecessorTaskID:              ids.NewAt(ids.KindTask, now, 11),
		ExpectedPreviousArtifactSHA256: hex.EncodeToString(artifactDigest[:]),
		ExpectedPreviousArtifactID:     ids.NewAt(ids.KindConfig, now, 12),
		ExpectedPreviousGeneration:     6,
		ImageRepository:                registeredPlan.Services[0].Image.Repository,
		ImageIndexDigest:               registeredPlan.Services[0].Image.IndexDigest,
		ImageOS:                        selectedPlatform.OS,
		ImageArchitecture:              selectedPlatform.Architecture,
		ImageVariant:                   selectedPlatform.Variant,
		ImageChildDigest:               selectedPlatform.ChildDigest,
		ImageConfigDigest:              selectedPlatform.ConfigDigest,
		ImageReference:                 selectedReference,
		ArtifactSHA256:                 hex.EncodeToString(artifactDigest[:]),
		ArtifactLength:                 uint64(len(registeredPlan.Files[0].Content)),
		PlanSHA256:                     hex.EncodeToString(registeredPlanDigest[:]),
	}
	task := etcd.TaskRecord{
		ID:               taskID,
		PlanID:           taskPlanID,
		RenderGeneration: 8,
		Executor:         etcd.TaskExecutorAgent,
		Type:             etcd.TaskUpdate,
		Target:           componentID,
		Actor:            etcd.TaskActorOperator,
		Params: map[string]string{
			etcd.TaskResourceKindParam:                   etcd.TaskResourceComponent,
			etcd.TaskPlatformComponentDesiredSHA256Param: desiredDigest,
		},
		Steps: []etcd.TaskStepRecord{
			{Kind: etcd.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 5)},
			{Kind: etcd.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 9)},
		},
	}
	input.ComposeArtifact, err = controllerpkg.RenderPlatformComponentCompose(
		controllerpkg.PlatformComponentComposeInput{
			ComponentID: componentID, PlanID: input.OwnershipPlanID,
			RenderGeneration: input.OwnershipGeneration,
			ArtifactID:       input.ComposeArtifactID, Plan: registeredPlan,
			ImageRepository: input.ImageRepository, ImageIndexDigest: input.ImageIndexDigest,
			ImageChildDigest: input.ImageChildDigest, ImageReference: input.ImageReference,
			ImageConfigDigest: input.ImageConfigDigest,
			ImageOS:           input.ImageOS, ImageArchitecture: input.ImageArchitecture, ImageVariant: input.ImageVariant,
		},
	)
	if err != nil {
		t.Fatalf("RenderPlatformComponentCompose() error = %v", err)
	}
	input, err = sealPlatformComponentTaskPlanHash(task, input, registeredPlan.Services[0].ObservationAction)
	if err != nil {
		t.Fatalf("sealPlatformComponentTaskPlanHash() error = %v", err)
	}
	task.PlanHash = input.ExecutionPlanSHA256
	baseline := etcd.Versioned[etcd.HostResolverBaselineRecord]{Record: etcd.HostResolverBaselineRecord{
		Generation: 1, Content: []byte("nameserver 1.1.1.1\n"), SHA256: input.BaselineSHA256,
	}}
	planner, err := NewPlatformComponentExecutionPlanner(
		"/var/lib/groundplane",
		executionRepositoryStub{
			input: input, current: etcd.Versioned[etcd.ComponentRecord]{Record: record}, baseline: baseline,
		},
		&executionCatalogStub{plan: registeredPlan},
	)
	if err != nil {
		t.Fatalf("NewPlatformComponentExecutionPlanner() error = %v", err)
	}

	execution, err := planner.ResolveComponentExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveComponentExecutionPlan() error = %v", err)
	}
	if len(execution.GetArtifacts()) != 1 || !proto.Equal(execution.GetArtifacts()[0], input.ComposeArtifact) {
		t.Fatal("disable dispatch changed sealed predecessor Compose artifact")
	}
	if hex.EncodeToString(execution.GetPlanHash()) != input.ExecutionPlanSHA256 {
		t.Fatal("disable dispatch changed sealed execution plan hash")
	}
}

func (catalog *executionCatalogStub) ResolveActionEnvelope(
	componentsdk.ActionEnvelope,
) (componentsdk.Definition, componentsdk.ActionDefinition, error) {
	return componentsdk.Definition{}, componentsdk.ActionDefinition{}, nil
}

func (catalog *executionCatalogStub) Plan(
	_ componentsdk.ImplementationKey,
	_ string,
	_ componentdns.RenderInput,
) (componentsdk.EnvironmentPlan, error) {
	plan := catalog.plan
	plan.Files = make([]componentsdk.ManagedFile, len(catalog.plan.Files))
	for index, file := range catalog.plan.Files {
		plan.Files[index] = file
		plan.Files[index].Content = append([]byte(nil), file.Content...)
	}
	return plan, nil
}

// Rationale: the Controller must carry a generic action reference for
// observation rather than resolving runtime argv or Compose health authority.
func TestPlatformExecutionRejectsFullPlanDriftForExecutionAndManagedConfig(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0).UTC()
	componentID := ids.NewAt(ids.KindComponent, now, 1)
	serviceID := ids.NewAt(ids.KindService, now, 2)
	planID := ids.NewAt(ids.KindPlan, now, 3)
	taskID := ids.NewAt(ids.KindTask, now, 4)
	stepID := ids.NewAt(ids.KindStep, now, 5)
	waitStepID := ids.NewAt(ids.KindStep, now, 9)
	component := core.Component{
		ID: componentID, Owner: core.ComponentOwnerPlatform, Kind: core.ComponentKindCoreDNS,
		Enabled: true, GeneratedServices: []string{serviceID}, Healthy: true,
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
			CorefileTemplate: testCorefileTemplate,
			UpstreamAuto:     true,
		}},
	}
	record, err := etcd.NewComponentRecord(component)
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	desiredDigest, err := etcd.PlatformComponentDesiredDigest(record)
	if err != nil {
		t.Fatalf("PlatformComponentDesiredDigest() error = %v", err)
	}
	firstPlan := executionPlanFixture(serviceID, "example/resolver@sha256:"+strings.Repeat("1", 64))
	secondPlan := executionPlanFixture(serviceID, "example/resolver@sha256:"+strings.Repeat("2", 64))
	selectedPlatform, selectedReference, selected := firstPlan.Services[0].Image.Select(
		runtime.GOOS,
		runtime.GOARCH,
	)
	if !selected {
		t.Fatalf("execution fixture does not support %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	artifactDigest := sha256.Sum256(firstPlan.Files[0].Content)
	registeredPlanDigest := componentsdk.DigestEnvironmentPlan(firstPlan)
	input := etcd.PlatformComponentTaskRenderInput{
		PlanID:                         planID,
		TaskID:                         taskID,
		ComponentID:                    componentID,
		DesiredSHA256:                  desiredDigest,
		BaselineGeneration:             1,
		BaselineSHA256:                 strings.Repeat("2", 64),
		Config:                         *component.Config.CoreDNS,
		HostResolutionInputRevision:    1,
		HostResolutionSHA256:           strings.Repeat("5", 64),
		GeneratedServiceID:             serviceID,
		DefinitionSHA256:               strings.Repeat("3", 64),
		CatalogSHA256:                  strings.Repeat("4", 64),
		ActionID:                       "activate-config",
		ArtifactID:                     ids.NewAt(ids.KindConfig, now, 6),
		ComposeArtifactID:              ids.NewAt(ids.KindConfig, now, 7),
		OwnershipPlanID:                ids.NewAt(ids.KindPlan, now, 10),
		OwnershipGeneration:            7,
		PriorObservationModRevision:    11,
		PriorObservationRevision:       11,
		PredecessorTaskID:              ids.NewAt(ids.KindTask, now, 11),
		ExpectedPreviousArtifactSHA256: strings.Repeat("6", 64),
		ExpectedPreviousArtifactID:     ids.NewAt(ids.KindConfig, now, 12),
		ExpectedPreviousGeneration:     6,
		ImageRepository:                firstPlan.Services[0].Image.Repository,
		ImageIndexDigest:               firstPlan.Services[0].Image.IndexDigest,
		ImageOS:                        selectedPlatform.OS,
		ImageArchitecture:              selectedPlatform.Architecture,
		ImageVariant:                   selectedPlatform.Variant,
		ImageChildDigest:               selectedPlatform.ChildDigest,
		ImageConfigDigest:              selectedPlatform.ConfigDigest,
		ImageReference:                 selectedReference,
		ArtifactSHA256:                 hex.EncodeToString(artifactDigest[:]),
		ArtifactLength:                 uint64(len(firstPlan.Files[0].Content)),
		PlanSHA256:                     hex.EncodeToString(registeredPlanDigest[:]),
	}
	task := etcd.TaskRecord{
		ID:               taskID,
		PlanID:           planID,
		RenderGeneration: 1,
		Executor:         etcd.TaskExecutorAgent,
		Type:             etcd.TaskUpdate,
		Target:           componentID,
		Actor:            etcd.TaskActorOperator,
		Params: map[string]string{
			etcd.TaskResourceKindParam:                   etcd.TaskResourceComponent,
			etcd.TaskPlatformComponentDesiredSHA256Param: desiredDigest,
		},
		Steps: []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: stepID}, {Kind: etcd.TaskStepOperation, ID: waitStepID}},
	}
	input.ComposeArtifact, err = controllerpkg.RenderPlatformComponentCompose(
		controllerpkg.PlatformComponentComposeInput{
			ComponentID: componentID, PlanID: input.OwnershipPlanID,
			RenderGeneration: input.OwnershipGeneration,
			ArtifactID:       input.ComposeArtifactID, Plan: firstPlan,
			ImageRepository: input.ImageRepository, ImageIndexDigest: input.ImageIndexDigest,
			ImageChildDigest: input.ImageChildDigest, ImageReference: input.ImageReference,
			ImageConfigDigest: input.ImageConfigDigest,
			ImageOS:           input.ImageOS, ImageArchitecture: input.ImageArchitecture, ImageVariant: input.ImageVariant,
		},
	)
	if err != nil {
		t.Fatalf("RenderPlatformComponentCompose() error = %v", err)
	}
	input, err = sealPlatformComponentTaskPlanHash(task, input, firstPlan.Services[0].ObservationAction)
	if err != nil {
		t.Fatalf("sealPlatformComponentTaskPlanHash() error = %v", err)
	}
	task.PlanHash = input.ExecutionPlanSHA256
	baseline := etcd.Versioned[etcd.HostResolverBaselineRecord]{Record: etcd.HostResolverBaselineRecord{
		Generation: 1, Content: []byte("nameserver 1.1.1.1\n"), SHA256: input.BaselineSHA256,
	}}
	catalog := &executionCatalogStub{plan: firstPlan}
	planner, err := NewPlatformComponentExecutionPlanner(
		"/var/lib/groundplane",
		executionRepositoryStub{
			input:    input,
			current:  etcd.Versioned[etcd.ComponentRecord]{Record: record},
			baseline: baseline,
		},
		catalog,
	)
	if err != nil {
		t.Fatalf("NewPlatformComponentExecutionPlanner() error = %v", err)
	}
	execution, err := planner.ResolveComponentExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveComponentExecutionPlan() error = %v", err)
	}
	activation := execution.GetSteps()[0].GetComponentApply()
	observation := execution.GetSteps()[1].GetComponentApply()
	if len(execution.GetSteps()) != 2 || activation == nil || observation == nil ||
		observation.GetActionId() != "observe-serving" ||
		observation.GetArtifactId() != activation.GetArtifactId() ||
		!strings.EqualFold(
			hex.EncodeToString(observation.GetArtifactDigest()),
			hex.EncodeToString(activation.GetArtifactDigest()),
		) ||
		len(execution.GetArtifacts()) != 1 || execution.GetArtifacts()[0].GetServices()[0].GetHasHealthcheck() {
		t.Fatalf("execution procedure = %#v", execution)
	}
	labels := execution.GetArtifacts()[0].GetServices()[0].GetExpectedLabels()
	foundOwnership := false
	for _, label := range labels {
		foundOwnership = foundOwnership ||
			label.GetKey() == "com.groundplane.plan-id" && label.GetValue() == input.OwnershipPlanID
	}
	if !foundOwnership || execution.GetArtifacts()[0].GetArtifactId() != input.ComposeArtifactID {
		t.Fatal("reload observation did not reuse sealed baseline ownership")
	}
	customRootPlanner, err := NewPlatformComponentExecutionPlanner(
		"/srv/groundplane/vol",
		executionRepositoryStub{
			input: input, current: etcd.Versioned[etcd.ComponentRecord]{Record: record}, baseline: baseline,
		},
		catalog,
	)
	if err != nil {
		t.Fatalf("NewPlatformComponentExecutionPlanner(custom root) error = %v", err)
	}
	customRootExecution, err := customRootPlanner.ResolveComponentExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveComponentExecutionPlan(custom root) error = %v", err)
	}
	if !strings.EqualFold(
		hex.EncodeToString(customRootExecution.GetPlanHash()),
		hex.EncodeToString(execution.GetPlanHash()),
	) {
		t.Fatal("configured volume root changed platform Component execution bytes")
	}
	retry := task
	retry.ID = ids.NewAt(ids.KindTask, now, 8)
	retry.RetryOf = task.ID
	replayed, err := planner.ResolveComponentExecutionPlan(context.Background(), retry)
	if err != nil {
		t.Fatalf("ResolveComponentExecutionPlan(retry) error = %v", err)
	}
	if replayed.GetPlanId() != execution.GetPlanId() ||
		!strings.EqualFold(hex.EncodeToString(replayed.GetPlanHash()), hex.EncodeToString(execution.GetPlanHash())) {
		t.Fatal("retry changed the sealed plan identity")
	}
	registeredPlanOnlyDrift := firstPlan
	registeredPlanOnlyDrift.Files = append([]componentsdk.ManagedFile(nil), firstPlan.Files...)
	registeredPlanOnlyDrift.Files[0].Path = "config/renamed-Corefile"
	catalog.plan = registeredPlanOnlyDrift
	if _, err := planner.ResolveComponentExecutionPlan(context.Background(), task); err == nil {
		t.Fatal("ResolveComponentExecutionPlan() accepted registered plan-only drift")
	}
	catalog.plan = secondPlan
	if _, err := planner.ResolveComponentExecutionPlan(context.Background(), task); err == nil {
		t.Fatal("ResolveComponentExecutionPlan() accepted changed service behavior")
	}
	if _, err := planner.ResolveManagedConfig(
		context.Background(),
		task,
		execution,
		execution.GetSteps()[0],
	); err == nil {
		t.Fatal("ResolveManagedConfig() accepted changed service behavior")
	}
}

func executionPlanFixture(serviceID, image string) componentsdk.EnvironmentPlan {
	return componentsdk.EnvironmentPlan{
		Services: []componentsdk.ManagedService{{
			ID: serviceID, Name: "resolver", Image: testPlatformImage(image),
			NetworkMode: componentsdk.ManagedNetworkModeHost,
			Command:     []string{"--config", "/etc/resolver/config"}, Restart: "unless-stopped", Replicas: 1,
			Mounts: []componentsdk.ManagedMount{{
				Kind: componentsdk.ManagedMountKindDirectory, Source: "config", Target: "/etc/resolver", ReadOnly: true,
			}},
			ObservationAction: "observe-serving",
		}},
		Files: []componentsdk.ManagedFile{{Path: "config/config", Content: []byte("same bytes\n")}},
	}
}
