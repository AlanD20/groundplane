package dnsresolver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type executionRepositoryStub struct {
	input    etcd.PlatformComponentTaskRenderInput
	current  etcd.Versioned[etcd.ComponentRecord]
	baseline etcd.Versioned[etcd.HostResolverBaselineRecord]
}

func (repository executionRepositoryStub) GetPlatformComponentTaskRenderInput(_ context.Context, _ string) (etcd.Versioned[etcd.PlatformComponentTaskRenderInput], error) {
	return etcd.Versioned[etcd.PlatformComponentTaskRenderInput]{Record: repository.input}, nil
}

func (repository executionRepositoryStub) GetComponent(_ context.Context, _ string) (etcd.Versioned[etcd.ComponentRecord], error) {
	return repository.current, nil
}

func (repository executionRepositoryStub) GetHostResolverBaseline(_ context.Context) (etcd.Versioned[etcd.HostResolverBaselineRecord], bool, error) {
	return repository.baseline, true, nil
}

type executionCatalogStub struct {
	plan componentsdk.EnvironmentPlan
}

func (catalog *executionCatalogStub) ResolveActionEnvelope(componentsdk.ActionEnvelope) (componentsdk.Definition, componentsdk.ActionDefinition, error) {
	return componentsdk.Definition{}, componentsdk.ActionDefinition{}, nil
}

func (catalog *executionCatalogStub) Plan(_ componentsdk.ImplementationKey, _ string, _ componentdns.RenderInput) (componentsdk.EnvironmentPlan, error) {
	plan := catalog.plan
	plan.Files = make([]componentsdk.ManagedFile, len(catalog.plan.Files))
	for index, file := range catalog.plan.Files {
		plan.Files[index] = file
		plan.Files[index].Content = append([]byte(nil), file.Content...)
	}
	return plan, nil
}

func TestPlatformExecutionRejectsFullPlanDriftForExecutionAndManagedConfig(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0).UTC()
	componentID := ids.NewAt(ids.KindComponent, now, 1)
	serviceID := ids.NewAt(ids.KindService, now, 2)
	planID := ids.NewAt(ids.KindPlan, now, 3)
	taskID := ids.NewAt(ids.KindTask, now, 4)
	stepID := ids.NewAt(ids.KindStep, now, 5)
	component := core.Component{
		ID: componentID, Owner: core.ComponentOwnerPlatform, Kind: core.ComponentKindCoreDNS,
		Enabled: true, GeneratedServices: []string{serviceID}, Healthy: true,
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{UpstreamAuto: true}},
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
	planDigest := componentsdk.DigestEnvironmentPlan(firstPlan)
	artifactDigest := sha256.Sum256(firstPlan.Files[0].Content)
	input := etcd.PlatformComponentTaskRenderInput{
		PlanID: planID, TaskID: taskID, ComponentID: componentID, DesiredSHA256: desiredDigest,
		BaselineGeneration: 1, BaselineSHA256: strings.Repeat("2", 64), Config: *component.Config.CoreDNS,
		GeneratedServiceID: serviceID, DefinitionSHA256: strings.Repeat("3", 64), CatalogSHA256: strings.Repeat("4", 64),
		ActionID: "activate-config", ArtifactID: ids.NewAt(ids.KindConfig, now, 6), ComposeArtifactID: ids.NewAt(ids.KindConfig, now, 7),
		ArtifactSHA256: hex.EncodeToString(artifactDigest[:]), ArtifactLength: uint64(len(firstPlan.Files[0].Content)),
		PlanSHA256: hex.EncodeToString(planDigest[:]),
	}
	task := etcd.TaskRecord{
		ID: taskID, PlanID: planID, RenderGeneration: 1, Executor: etcd.TaskExecutorAgent,
		Type: etcd.TaskUpdate, Target: componentID,
		Params: map[string]string{etcd.TaskResourceKindParam: etcd.TaskResourceComponent, etcd.TaskPlatformComponentDesiredSHA256Param: desiredDigest},
		Steps:  []etcd.TaskStepRecord{{ID: stepID}},
	}
	baseline := etcd.Versioned[etcd.HostResolverBaselineRecord]{Record: etcd.HostResolverBaselineRecord{
		Generation: 1, Content: []byte("nameserver 1.1.1.1\n"), SHA256: input.BaselineSHA256,
	}}
	catalog := &executionCatalogStub{plan: firstPlan}
	planner, err := NewPlatformComponentExecutionPlanner(
		"/var/lib/groundplane",
		executionRepositoryStub{input: input, current: etcd.Versioned[etcd.ComponentRecord]{Record: record}, baseline: baseline}, catalog,
	)
	if err != nil {
		t.Fatalf("NewPlatformComponentExecutionPlanner() error = %v", err)
	}
	execution, err := planner.ResolveComponentExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveComponentExecutionPlan() error = %v", err)
	}
	if len(execution.GetSteps()) != 1 {
		t.Fatalf("execution steps = %d, want 1", len(execution.GetSteps()))
	}
	catalog.plan = secondPlan
	if _, err := planner.ResolveComponentExecutionPlan(context.Background(), task); err == nil {
		t.Fatal("ResolveComponentExecutionPlan() accepted changed service behavior")
	}
	if _, err := planner.ResolveManagedConfig(context.Background(), task, execution, execution.GetSteps()[0]); err == nil {
		t.Fatal("ResolveManagedConfig() accepted changed service behavior")
	}
}

func executionPlanFixture(serviceID, image string) componentsdk.EnvironmentPlan {
	return componentsdk.EnvironmentPlan{
		Services: []componentsdk.ManagedService{{
			ID: serviceID, Name: "resolver", Image: image,
			Command: []string{"--config", "/etc/resolver/config"}, Restart: "unless-stopped", Replicas: 1,
			Mounts: []componentsdk.ManagedMount{{Source: "config", Target: "/etc/resolver/config", ReadOnly: true}},
		}},
		Files: []componentsdk.ManagedFile{{Path: "config", Content: []byte("same bytes\n")}},
	}
}
