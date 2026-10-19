package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/core/serviceports"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestGetReleaseRenderInputAtDecodesStoredEnvelope(t *testing.T) {
	t.Parallel()
	input := portlessReleaseRenderInput(domain.StrategyRecreate)
	input.PriorArtifactID = ids.NewAt(ids.KindConfig, time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC), 10)
	input.PriorWorkload = releaseTestPriorWorkload("registry.example/worker:previous")
	input.CandidateWorkload.ReplicaCount = 3
	input.PriorWorkload.ReplicaCount = 2
	input = withNativeRenderPredecessor(t, input)
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
		ProxyImplementation: serviceproxy.Caddy,
		ProxyGeneration:     2, PriorProxyGeneration: 1, ProxyPorts: []domain.ProxyPort{{Port: 8080, Protocol: serviceports.TCP}},
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
		Components: releaseRenderProxyComponents(now, input.EnvironmentID),
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
	input = withNativeRenderPredecessor(t, input)
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
	input = withNativeRenderPredecessor(t, input)
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
	input = withNativeRenderPredecessor(t, input)
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
	input.ProxyPorts = []domain.ProxyPort{{Port: 8080, Protocol: serviceports.TCP}}
	input.ProxyImplementation = serviceproxy.Caddy
	input.ProxyImage = releaseProxyImageFixture()
	input.ProxyConfigDigest, input.PriorProxyDigest = "candidate", "prior"
	input.PriorArtifactID = ids.NewAt(ids.KindConfig, time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC), 12)
	input.PriorWorkload = releaseTestPriorWorkload("registry.example/worker:previous")
	input = withNativeRenderPredecessor(t, input)
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
	input.ProxyImplementation = serviceproxy.Caddy
	input.ProxyGeneration, input.ProxyPorts, input.ProxyConfigDigest = 2, []domain.ProxyPort{
		{Port: 8080, Protocol: serviceports.TCP},
	}, "candidate"
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
		Components: releaseRenderProxyComponents(now, input.EnvironmentID),
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

// These storage checks supply an acknowledged predecessor artifact. They do
// not execute a prior deployment; absence of this native witness is corruption,
// not permission to reconstruct runtime from the current desired projection.
func withNativeRenderPredecessor(
	t *testing.T,
	input testreleaserender.ReleaseRenderInput,
) testreleaserender.ReleaseRenderInput {
	t.Helper()
	priorID := ids.New(ids.KindDeployment)
	role, label := agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON, "singleton"
	if input.PriorSlot != "" {
		role, label = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT, "slot"
	}
	workload := &agentpb.ComposeService{
		ServiceId: input.ServiceID, ComposeName: "prior-worker", ExpectedReplicas: input.PriorWorkload.ReplicaCount,
		ImageReference: input.PriorWorkload.LocalImageID, Role: role, Slot: string(input.PriorSlot), HasHealthcheck: true,
		ExpectedLabels: []*agentpb.LabelPair{
			{Key: "com.groundplane.release-id", Value: priorID},
			{Key: "com.groundplane.runtime-role", Value: label},
		},
	}
	if input.PriorSlot != "" {
		workload.ExpectedLabels = append(
			workload.ExpectedLabels,
			&agentpb.LabelPair{Key: "com.groundplane.slot", Value: string(input.PriorSlot)},
		)
	}
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: input.PriorArtifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: input.EnvironmentID, ProjectName: "gp-" + strings.ToLower(input.EnvironmentID),
		Services: []*agentpb.ComposeService{workload},
	}
	if len(input.ProxyPorts) != 0 {
		ports := make([]serviceproxy.Port, len(input.ProxyPorts))
		for index, port := range input.ProxyPorts {
			ports[index] = serviceproxy.Port{Number: port.Port, Protocol: serviceproxy.Protocol(port.Protocol)}
		}
		configuration, err := serviceproxy.Render(serviceproxy.Configuration{
			Implementation: input.ProxyImplementation, ReleaseID: priorID, Generation: input.PriorProxyGeneration,
			Ports: ports, Upstreams: []string{workload.ComposeName},
		})
		if err != nil {
			t.Fatal(err)
		}
		implementation, err := executionplan.SealProxyImplementation(input.ProxyImplementation)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(configuration)
		input.PriorProxyDigest = hex.EncodeToString(digest[:])
		artifact.Services = append(artifact.Services, &agentpb.ComposeService{
			ServiceId: input.ServiceID, ComposeName: "prior-proxy", Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			ExpectedLabels:      []*agentpb.LabelPair{{Key: "com.groundplane.runtime-role", Value: "proxy"}},
			ProxyImplementation: implementation, ProxyConfigJson: configuration, ProxyConfigSha256: digest[:],
		})
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	input.PriorRuntime = &testreleaserender.ServingRuntimeCapture{ServiceID: input.ServiceID, CurrentArtifact: raw}
	return input
}

func releaseRenderProxyComponents(now time.Time, environmentID string) []testcomponents.Record {
	return []testcomponents.Record{
		{
			Desired: testcomponents.DesiredRecord{
				ID: ids.NewAt(ids.KindComponent, now, 20), Owner: core.ComponentOwnerEnvironment,
				OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy,
			},
		},
		{
			Desired: testcomponents.DesiredRecord{
				ID: ids.NewAt(ids.KindComponent, now, 21), Owner: core.ComponentOwnerEnvironment,
				OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare,
			},
		},
	}
}
