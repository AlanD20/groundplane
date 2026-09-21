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
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

func TestGetReleaseRenderInputAtDecodesStoredEnvelope(t *testing.T) {
	t.Parallel()
	input := portlessReleaseRenderInput(domain.StrategyRecreate)
	input.PriorArtifactID = ids.NewAt(ids.KindConfig, time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC), 10)
	input.PriorWorkload = releaseTestPriorWorkload("registry.example/worker:previous")
	input.CandidateWorkload.ReplicaCount = 3
	input.PriorWorkload.ReplicaCount = 2
	raw, err := testreleaserender.EncodeReleaseRenderInput(input)
	if err != nil {
		t.Fatalf("EncodeReleaseRenderInput() error = %v", err)
	}
	stored, err := testreleases.EncodeReleaseRecord("release-render-input", json.RawMessage(raw))
	if err != nil {
		t.Fatalf("encodeReleaseRecord() error = %v", err)
	}
	store := &releaseRenderInputTestStore{memoryTaskStore: newMemoryTaskStore()}
	seedTaskRepositoryValue(
		t,
		store.memoryTaskStore,
		testreleases.ReleaseRenderInputStagingKey("", input.ReleaseID),
		stored,
	)
	revision := store.currentRevision()

	got, err := (releaseLedgerFixture(t, store)).GetReleaseRenderInputAt(
		context.Background(), input.ReleaseID, revision,
	)
	if err != nil {
		t.Fatalf("GetReleaseRenderInputAt() error = %v", err)
	}
	if got.Record.ReleaseID != input.ReleaseID || got.ReadRevision != revision {
		t.Fatalf("GetReleaseRenderInputAt() = %#v", got)
	}
	if got.Record.CandidateWorkload != input.CandidateWorkload || got.Record.PriorWorkload == nil ||
		*got.Record.PriorWorkload != *input.PriorWorkload {
		t.Fatalf("stored candidate/prior seals changed: %#v", got.Record)
	}
}

type releaseRenderInputTestStore struct {
	*memoryTaskStore
}

func (*releaseRenderInputTestStore) Close() error { return nil }

func (*releaseRenderInputTestStore) Health(context.Context) error { return nil }

func (store *releaseRenderInputTestStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
	)
	return result.Revision, err
}

func (store *releaseRenderInputTestStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: key}})
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
		Edges: []core.ServiceDependencyEdge{
			{Service: "worker", Dependency: "api", Condition: core.ServiceDependencyStarted},
		},
	}}
	tamperedPlans := core.ServiceDependencyPlans{DeployDependencyPlan: core.ServiceDependencyPhasePlan{
		Phase: core.ServiceLifecycleDeploy, OrderedServices: []string{"worker", "api"},
		Edges: []core.ServiceDependencyEdge{
			{Service: "api", Dependency: "worker", Condition: core.ServiceDependencyStarted},
		},
	}}
	input := testreleaserender.ReleaseRenderInput{
		ReleaseID: ids.NewAt(ids.KindDeployment, now, 3), PlanID: ids.NewAt(ids.KindPlan, now, 4),
		ArtifactID: ids.NewAt(ids.KindConfig, now, 5), ServiceID: workerID, ServiceName: "worker",
		CandidateWorkload: releaseTestWorkloadSeal(
			"registry.example/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		),
		Strategy: domain.StrategyBlueGreen, PriorStrategy: domain.StrategyBlueGreen,
		Slot: domain.SlotGreen, PriorSlot: domain.SlotBlue,
		CandidateTarget: domain.WorkloadGreen, PriorTarget: domain.WorkloadBlue,
		ProxyGeneration: 2, PriorProxyGeneration: 1, ProxyPorts: []uint16{8080},
		ProxyImage: releaseProxyImageFixture(),
		PriorArtifactID: ids.NewAt(
			ids.KindConfig,
			now,
			10,
		), PriorWorkload: releaseTestPriorWorkload("registry.example/worker:prior"),
		ProxyConfigDigest: "candidate", PriorProxyDigest: "prior", ServiceDependencyPlans: tamperedPlans,
		TenantID: ids.NewAt(ids.KindTenant, now, 6), TenantSlug: "tenant",
		ProjectID: ids.NewAt(ids.KindProject, now, 7), ProjectSlug: "project",
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 8), EnvironmentName: "production",
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes",
	}
	input.Projection = testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: input.EnvironmentID, RevisionID: ids.NewAt(ids.KindTask, now, 9), RenderGeneration: 1,
		DesiredServices: []testservices.EnvironmentServiceProjection{
			{
				EnvironmentID: input.EnvironmentID,
				Desired: core.Service{
					ID:       apiID,
					Name:     "api",
					Image:    "registry.example/api:latest",
					Strategy: core.StrategyRecreate,
				},
			},
			{
				EnvironmentID: input.EnvironmentID,
				Desired: core.Service{
					ID:       workerID,
					Name:     "worker",
					Image:    "registry.example/worker:latest",
					Strategy: core.StrategyRecreate,
				},
			},
		},
		ServiceDependencyPlans: projectionPlans,
	}
	input.Projection = withTestEnvironmentComposeArtifact(input.Projection)
	input.ServiceDependencyPlans = projectionPlans
	if err := testreleaserender.ValidateReleaseRenderInput(input); err != nil {
		t.Fatalf("valid dependency authority baseline rejected: %v", err)
	}
	input.ServiceDependencyPlans = tamperedPlans
	if err := testreleaserender.ValidateReleaseRenderInput(input); err == nil {
		t.Fatal("validateReleaseRenderInput() accepted mismatched dependency authorities")
	}
}

func TestValidateReleaseRenderInputAcceptsPortlessRecreate(t *testing.T) {
	t.Parallel()
	input := portlessReleaseRenderInput(domain.StrategyRecreate)
	input.PriorArtifactID = ids.NewAt(ids.KindConfig, time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC), 10)
	input.PriorWorkload = releaseTestPriorWorkload("registry.example/worker:previous")
	if err := testreleaserender.ValidateReleaseRenderInput(input); err != nil {
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
	input.PriorWorkload = releaseTestPriorWorkload("registry.example/worker:previous")
	input.ProxyGeneration, input.PriorProxyGeneration = 2, 1
	if err := testreleaserender.ValidateReleaseRenderInput(input); err == nil {
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
	input.ProxyImage = releaseProxyImageFixture()
	input.ProxyConfigDigest, input.PriorProxyDigest = "candidate", "prior"
	input.PriorArtifactID = ids.NewAt(ids.KindConfig, time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC), 12)
	input.PriorWorkload = releaseTestPriorWorkload("registry.example/worker:previous")
	if err := testreleaserender.ValidateReleaseRenderInput(input); err != nil {
		t.Fatalf("validateReleaseRenderInput() error = %v", err)
	}
	if input.Slot != "" {
		t.Fatalf("addressable recreate durable slot = %q", input.Slot)
	}
}

func TestFirstBlueGreenRenderPublicationRoundTrip(t *testing.T) {
	input := portlessReleaseRenderInput(domain.StrategyBlueGreen)
	input.Slot, input.CandidateTarget = domain.SlotBlue, domain.WorkloadBlue
	input.ProxyGeneration, input.ProxyPorts, input.ProxyConfigDigest = 2, []uint16{8080}, "candidate"
	input.ProxyImage = releaseProxyImageFixture()
	encoded, err := testreleaserender.EncodeReleaseRenderInput(input)
	if err != nil {
		t.Fatalf("first blue-green publication: %v", err)
	}
	decoded, err := testreleaserender.DecodeReleaseRenderInput(encoded)
	if err != nil || decoded.PriorWorkload != nil || decoded.PriorArtifactID != "" ||
		decoded.PriorProxyGeneration != 0 ||
		decoded.PriorProxyDigest != "" {
		t.Fatalf("first blue-green changed absent predecessor: %v", err)
	}
	for name, mutate := range map[string]func(*testreleaserender.ReleaseRenderInput){
		"stray-generation": func(value *testreleaserender.ReleaseRenderInput) { value.PriorProxyGeneration = 1 },
		"stray-digest":     func(value *testreleaserender.ReleaseRenderInput) { value.PriorProxyDigest = "prior" },
		"missing-artifact": func(value *testreleaserender.ReleaseRenderInput) {
			value.PriorWorkload = releaseTestPriorWorkload("app:prior")
		},
		"missing-workload": func(value *testreleaserender.ReleaseRenderInput) { value.PriorArtifactID = value.ArtifactID },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := input
			mutate(&invalid)
			if _, err := testreleaserender.EncodeReleaseRenderInput(invalid); err == nil {
				t.Fatal("incomplete or invented predecessor authority accepted")
			}
		})
	}
}

func portlessReleaseRenderInput(strategy domain.Strategy) testreleaserender.ReleaseRenderInput {
	now := time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, now, 1)
	input := testreleaserender.ReleaseRenderInput{
		ReleaseID: ids.NewAt(ids.KindDeployment, now, 2), PlanID: ids.NewAt(ids.KindPlan, now, 3),
		ArtifactID: ids.NewAt(ids.KindConfig, now, 4), ServiceID: serviceID, ServiceName: "worker",
		CandidateWorkload: releaseTestWorkloadSeal(
			"registry.example/worker:next",
		), Strategy: strategy, PriorStrategy: domain.StrategyRecreate,
		CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
		TenantID: ids.NewAt(ids.KindTenant, now, 5), TenantSlug: "tenant",
		ProjectID: ids.NewAt(ids.KindProject, now, 6), ProjectSlug: "project",
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 7), EnvironmentName: "production",
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes",
	}
	input.Projection = testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: input.EnvironmentID, RevisionID: ids.NewAt(ids.KindTask, now, 8), RenderGeneration: 1,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: input.EnvironmentID,
			Desired: core.Service{
				ID:       serviceID,
				Name:     "worker",
				Image:    input.CandidateWorkload.RequestedReference,
				Strategy: core.StrategyRecreate,
			},
		}},
	}
	input.Projection = withTestEnvironmentComposeArtifact(input.Projection)
	return input
}
