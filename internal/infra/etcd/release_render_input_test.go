package etcd

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
)

func TestGetReleaseRenderInputAtDecodesStoredEnvelope(t *testing.T) {
	t.Parallel()
	input := portlessReleaseRenderInput(domain.StrategyRecreate)
	input.PriorArtifactID = ids.NewAt(ids.KindConfig, time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC), 10)
	input.PriorImage = "registry.example/worker:previous"
	raw, err := EncodeReleaseRenderInput(input)
	if err != nil {
		t.Fatalf("EncodeReleaseRenderInput() error = %v", err)
	}
	stored, err := encodeReleaseRecord("release-render-input", json.RawMessage(raw))
	if err != nil {
		t.Fatalf("encodeReleaseRecord() error = %v", err)
	}
	store := &releaseRenderInputTestStore{memoryTaskStore: newMemoryTaskStore()}
	seedTaskRepositoryValue(t, store.memoryTaskStore, releaseRenderInputStagingKey("", input.ReleaseID), stored)
	revision := store.currentRevision()

	got, err := (&ReleaseLedger{store: store}).GetReleaseRenderInputAt(
		context.Background(), input.ReleaseID, revision,
	)
	if err != nil {
		t.Fatalf("GetReleaseRenderInputAt() error = %v", err)
	}
	if got.Record.ReleaseID != input.ReleaseID || got.ReadRevision != revision {
		t.Fatalf("GetReleaseRenderInputAt() = %#v", got)
	}
}

type releaseRenderInputTestStore struct {
	*memoryTaskStore
}

func (*releaseRenderInputTestStore) Close() error { return nil }

func (*releaseRenderInputTestStore) Health(context.Context) error { return nil }

func (store *releaseRenderInputTestStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut, Key: key, Value: value}})
	return result.Revision, err
}

func (store *releaseRenderInputTestStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationDelete, Key: key}})
	return result.Revision, err
}

func (*releaseRenderInputTestStore) Snapshot(context.Context, io.Writer) error { return nil }

// Rationale: both dependency-plan copies can be independently valid while
// naming different authorities; durable decode must reject that tampering.
func TestValidateReleaseRenderInputRejectsMismatchedDependencyAuthority(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	apiID := ids.NewAt(ids.KindService, now, 1)
	workerID := ids.NewAt(ids.KindService, now, 2)
	projectionPlans := core.ServiceDependencyPlans{DeployDependencyPlan: core.ServiceDependencyPhasePlan{
		Phase: core.ServiceLifecycleDeploy, OrderedServices: []string{"api", "worker"},
		Edges: []core.ServiceDependencyEdge{{Service: "worker", Dependency: "api", Condition: core.ServiceDependencyStarted}},
	}}
	tamperedPlans := core.ServiceDependencyPlans{DeployDependencyPlan: core.ServiceDependencyPhasePlan{
		Phase: core.ServiceLifecycleDeploy, OrderedServices: []string{"worker", "api"},
		Edges: []core.ServiceDependencyEdge{{Service: "api", Dependency: "worker", Condition: core.ServiceDependencyStarted}},
	}}
	input := ReleaseRenderInput{
		ReleaseID: ids.NewAt(ids.KindDeployment, now, 3), PlanID: ids.NewAt(ids.KindPlan, now, 4),
		ArtifactID: ids.NewAt(ids.KindConfig, now, 5), ServiceID: workerID, ServiceName: "worker",
		Image:    "registry.example/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Strategy: domain.StrategyBlueGreen, PriorStrategy: domain.StrategyBlueGreen,
		Slot: domain.SlotGreen, PriorSlot: domain.SlotBlue,
		CandidateTarget: domain.WorkloadGreen, PriorTarget: domain.WorkloadBlue,
		ProxyGeneration: 2, PriorProxyGeneration: 1, ProxyPorts: []uint16{8080},
		ProxyConfigDigest: "candidate", PriorProxyDigest: "prior", ServiceDependencyPlans: tamperedPlans,
		TenantID: ids.NewAt(ids.KindTenant, now, 6), TenantSlug: "tenant",
		ProjectID: ids.NewAt(ids.KindProject, now, 7), ProjectSlug: "project",
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 8), EnvironmentName: "production",
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes",
	}
	input.Projection = EnvironmentComposeProjection{
		EnvironmentID: input.EnvironmentID, RevisionID: ids.NewAt(ids.KindTask, now, 9), RenderGeneration: 1,
		Services:               []EnvironmentComposeIdentity{{ID: apiID, Name: "api"}, {ID: workerID, Name: "worker"}},
		ServiceDependencyPlans: projectionPlans,
	}
	if err := validateReleaseRenderInput(input); err == nil {
		t.Fatal("validateReleaseRenderInput() accepted mismatched dependency authorities")
	}
}

func TestValidateReleaseRenderInputAcceptsPortlessRecreate(t *testing.T) {
	t.Parallel()
	input := portlessReleaseRenderInput(domain.StrategyRecreate)
	input.PriorArtifactID = ids.NewAt(ids.KindConfig, time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC), 10)
	input.PriorImage = "registry.example/worker:previous"
	if err := validateReleaseRenderInput(input); err != nil {
		t.Fatalf("validateReleaseRenderInput() error = %v", err)
	}
}

func TestValidateReleaseRenderInputRejectsPortlessBlueGreen(t *testing.T) {
	t.Parallel()
	input := portlessReleaseRenderInput(domain.StrategyBlueGreen)
	input.Strategy, input.PriorStrategy = domain.StrategyBlueGreen, domain.StrategyRecreate
	input.Slot, input.PriorSlot = domain.SlotGreen, ""
	input.CandidateTarget, input.PriorTarget = domain.WorkloadGreen, domain.WorkloadSingleton
	input.PriorArtifactID = ids.NewAt(ids.KindConfig, time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC), 11)
	input.PriorImage = "registry.example/worker:previous"
	input.ProxyGeneration, input.PriorProxyGeneration = 2, 1
	if err := validateReleaseRenderInput(input); err == nil {
		t.Fatal("validateReleaseRenderInput() accepted blue-green without an addressable proxy port")
	}
}

func TestValidateReleaseRenderInputAcceptsAddressableRecreateWithoutDurableSlot(t *testing.T) {
	t.Parallel()
	input := portlessReleaseRenderInput(domain.StrategyRecreate)
	input.PriorStrategy = domain.StrategyBlueGreen
	input.PriorSlot = domain.SlotBlue
	input.CandidateTarget, input.PriorTarget = domain.WorkloadSingleton, domain.WorkloadBlue
	input.ProxyGeneration, input.PriorProxyGeneration = 2, 1
	input.ProxyPorts = []uint16{8080}
	input.ProxyConfigDigest, input.PriorProxyDigest = "candidate", "prior"
	input.PriorArtifactID = ids.NewAt(ids.KindConfig, time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC), 12)
	input.PriorImage = "registry.example/worker:previous"
	if err := validateReleaseRenderInput(input); err != nil {
		t.Fatalf("validateReleaseRenderInput() error = %v", err)
	}
	if input.Slot != "" {
		t.Fatalf("addressable recreate durable slot = %q", input.Slot)
	}
}

func portlessReleaseRenderInput(strategy domain.Strategy) ReleaseRenderInput {
	now := time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, now, 1)
	input := ReleaseRenderInput{
		ReleaseID: ids.NewAt(ids.KindDeployment, now, 2), PlanID: ids.NewAt(ids.KindPlan, now, 3),
		ArtifactID: ids.NewAt(ids.KindConfig, now, 4), ServiceID: serviceID, ServiceName: "worker",
		Image: "registry.example/worker:next", Strategy: strategy, PriorStrategy: domain.StrategyRecreate,
		CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
		TenantID: ids.NewAt(ids.KindTenant, now, 5), TenantSlug: "tenant",
		ProjectID: ids.NewAt(ids.KindProject, now, 6), ProjectSlug: "project",
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 7), EnvironmentName: "production",
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes",
	}
	input.Projection = EnvironmentComposeProjection{
		EnvironmentID: input.EnvironmentID, RevisionID: ids.NewAt(ids.KindTask, now, 8), RenderGeneration: 1,
		Services: []EnvironmentComposeIdentity{{ID: serviceID, Name: "worker"}},
	}
	input.Projection = withTestEnvironmentComposeArtifact(input.Projection)
	return input
}
