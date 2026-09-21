package etcd

import (
	"strconv"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testattachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	testenvironmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: capturing the actual runtime for a realistic multi-Service
// Environment must remain within the Attach record's durable size boundary.
func TestAttachTaskRenderInputFitsCapturedMultiServiceRuntime(t *testing.T) {
	t.Parallel()
	input := capturedAttachRenderInputFixture(t)
	value, err := testattachrender.EncodeAttachTaskRenderInput(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.RuntimeProjection.DesiredServices) < 11 ||
		len(value) >= testattachrender.MaximumAttachTaskRenderInputBytes {
		t.Fatalf("captured %d-Service Attach input size = %d", len(input.RuntimeProjection.DesiredServices), len(value))
	}
}

// Rationale: a native Deploy does not advance desired state. Its one serving
// slot is valid captured runtime at the current generation, not a fresh desired
// topology with a missing slot. The durable Attach must preserve that distinction.
func TestAttachTaskRenderInputRetainsCurrentGenerationSlot(t *testing.T) {
	for _, test := range []struct {
		name       string
		generation uint64
		otherPlan  bool
		wantError  bool
	}{
		{name: "current", generation: 2},
		{name: "older", generation: 1},
		{name: "future", generation: 3, wantError: true},
		{name: "foreign proxy plan", generation: 2, otherPlan: true, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := capturedAttachRenderInputFixture(t)
			input.RenderGeneration, input.RuntimeProjection.RenderGeneration = 2, 2
			artifact := &agentpb.ComposeArtifact{}
			if err := proto.Unmarshal(input.RuntimeProjection.ComposeArtifact, artifact); err != nil {
				t.Fatal(err)
			}
			service := input.RuntimeProjection.DesiredServices[0].Desired
			planID := ids.New(ids.KindPlan)
			physical := func(name string, role agentpb.ComposeServiceRole, slot string) *agentpb.ComposeService {
				return &agentpb.ComposeService{ServiceId: service.ID, ComposeName: name, Role: role, Slot: slot,
					ExpectedLabels: []*agentpb.LabelPair{
						{Key: "com.groundplane.plan-id", Value: planID},
						{Key: "com.groundplane.render-generation", Value: strconv.FormatUint(test.generation, 10)},
					}}
			}
			proxy := physical(service.Name, agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY, "")
			if test.otherPlan {
				proxy.ExpectedLabels[0].Value = ids.New(ids.KindPlan)
			}
			workload := physical(
				service.Name+"--blue",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
				"blue",
			)
			artifact.Services = append([]*agentpb.ComposeService{proxy, workload}, artifact.Services[1:]...)
			var err error
			input.RuntimeProjection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
			if err != nil {
				t.Fatal(err)
			}
			if test.generation == 2 &&
				testenvironmentprojection.ValidateEnvironmentComposeProjection(input.RuntimeProjection) == nil {
				t.Fatal("desired-state validator accepted incomplete fresh slot topology")
			}
			encoded, err := testattachrender.EncodeAttachTaskRenderInput(input)
			if (err != nil) != test.wantError {
				t.Fatalf("encode captured runtime = %v, want error %t", err, test.wantError)
			}
			if !test.wantError {
				decoded, err := testattachrender.DecodeAttachTaskRenderInput(encoded)
				if err != nil || !proto.Equal(artifact, mustAttachRuntimeArtifact(t, decoded)) {
					t.Fatalf("captured runtime changed in durable roundtrip: %v", err)
				}
			}
		})
	}
}

func mustAttachRuntimeArtifact(t *testing.T, input testattachrender.AttachTaskRenderInput) *agentpb.ComposeArtifact {
	t.Helper()
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(input.RuntimeProjection.ComposeArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	return artifact
}

func capturedAttachRenderInputFixture(t *testing.T) testattachrender.AttachTaskRenderInput {
	t.Helper()
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	runtime := desiredTopologyProjectionFixture(t)
	consumerID := runtime.DesiredServices[0].Desired.ID
	return testattachrender.AttachTaskRenderInput{
		PlanID: ids.NewAt(ids.KindPlan, now, 1), AttachID: ids.NewAt(ids.KindAttach, now, 2),
		AttachName: "database", TenantID: ids.NewAt(ids.KindTenant, now, 3), TenantSlug: "tenant",
		ProjectID: ids.NewAt(ids.KindProject, now, 4), ProjectSlug: "project",
		EnvironmentID: runtime.EnvironmentID, EnvironmentName: "main",
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/tenant/project/main",
		BackingServiceID:    ids.NewAt(ids.KindService, now, 5),
		BackingProjectID:    ids.NewAt(ids.KindProject, now, 6), AdapterKey: "custom",
		DesiredRevisionID: runtime.RevisionID, ArtifactID: ids.NewAt(ids.KindConfig, now, 7),
		RenderGeneration: runtime.RenderGeneration, EnvironmentEpochRevision: 1,
		RuntimeProjection: runtime, RunningServiceIDs: []string{consumerID},
		Services: testattachrender.AttachTaskServiceSnapshots(runtime.DesiredServices),
		Networks: testattachrender.AttachTaskOwnedNetworkSnapshots(runtime.DesiredZones),
		Volumes:  runtime.Volumes, VolumeMounts: runtime.VolumeMounts,
		NetworkJoins: []testattachrender.AttachTaskNetworkJoin{{
			NetworkID: ids.NewAt(ids.KindNetwork, now, 8), ServiceIDs: []string{consumerID},
		}},
		ConsumerServiceIDs:     []string{consumerID},
		ServiceDependencyPlans: runtime.ServiceDependencyPlans.Clone(),
	}
}

// AssertAttachRuntimeRoundTrip crosses capture and durable encoding in the
// Controller renderer fixture without adding a production persistence API.
func AssertAttachRuntimeRoundTrip(
	t *testing.T,
	runtime testenvironmentprojection.EnvironmentComposeProjection,
	epoch int64,
	running []string,
) {
	t.Helper()
	input := capturedAttachRenderInputFixture(t)
	input.EnvironmentID, input.DesiredRevisionID = runtime.EnvironmentID, runtime.RevisionID
	input.RenderGeneration, input.EnvironmentEpochRevision = runtime.RenderGeneration, epoch
	input.RuntimeProjection = runtime
	input.ArtifactID = mustAttachRuntimeArtifact(t, input).ArtifactId
	input.Services, input.Networks = testattachrender.AttachTaskServiceSnapshots(
		runtime.DesiredServices,
	), testattachrender.AttachTaskOwnedNetworkSnapshots(
		runtime.DesiredZones,
	)
	input.Volumes, input.VolumeMounts = runtime.Volumes, runtime.VolumeMounts
	input.ServiceDependencyPlans = runtime.ServiceDependencyPlans.Clone()
	input.ConsumerServiceIDs = []string{runtime.DesiredServices[0].Desired.ID}
	input.RunningServiceIDs = running
	input.NetworkJoins[0].ServiceIDs = input.ConsumerServiceIDs
	encoded, err := testattachrender.EncodeAttachTaskRenderInput(input)
	if err != nil {
		t.Fatal("captured Attach runtime cannot be persisted", err)
	}
	decoded, err := testattachrender.DecodeAttachTaskRenderInput(encoded)
	if err != nil || !proto.Equal(mustAttachRuntimeArtifact(t, input), mustAttachRuntimeArtifact(t, decoded)) {
		t.Fatalf("captured Attach runtime changed through persistence: %v", err)
	}
}

// Rationale: unchanged desired Blueprint state cannot hide a serving-runtime
// change between capture and Attach publication.
func TestAttachRuntimeEpochRejectsStaleCapture(t *testing.T) {
	store := newAttachTestStore()
	scope := seedAttachScope(t, t.Context(), store)
	load := func() *testenvironmentfence.MutationContext {
		context, err := testenvironmentfence.LoadMutationContext(
			t.Context(),
			store,
			scope.Environment.Record.ID,
			testhierarchy.EnvironmentKey(scope.Environment.Record.ID),
			scope.Project.Record.ID,
			scope.Tenant.Record.ID,
		)
		if err != nil {
			t.Fatal(err)
		}
		return context
	}
	key := testhierarchy.EnvironmentMutationEpochKey(scope.Environment.Record.ID)
	captured := load()
	epoch, found := captured.RevisionForKey(key)
	if !found {
		t.Fatal("capture lacks epoch")
	}
	input := testattachrender.AttachTaskRenderInput{EnvironmentEpochRevision: epoch}
	if err := validateAttachRuntimeEpoch(captured, scope.Environment.Record.ID, input); err != nil {
		t.Fatal(err)
	}
	current, err := store.Get(t.Context(), key)
	if err != nil || current.Entry == nil {
		t.Fatal("epoch read", err)
	}
	if _, err := store.Put(t.Context(), key, current.Entry.Value); err != nil {
		t.Fatal(err)
	}
	if err := validateAttachRuntimeEpoch(load(), scope.Environment.Record.ID, input); !isKind(
		err,
		errs.KindStateConflict,
	) {
		t.Fatalf("stale Attach capture accepted: %v", err)
	}
}
