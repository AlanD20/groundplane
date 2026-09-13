package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: capturing the actual runtime for a realistic multi-Service
// Environment must remain within the Attach record's durable size boundary.
func TestAttachTaskRenderInputFitsCapturedMultiServiceRuntime(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	runtime := desiredTopologyProjectionFixture(t)
	consumerID := runtime.DesiredServices[0].Desired.ID
	input := AttachTaskRenderInput{
		PlanID: ids.NewAt(ids.KindPlan, now, 1), AttachID: ids.NewAt(ids.KindAttach, now, 2),
		AttachName: "database", TenantID: ids.NewAt(ids.KindTenant, now, 3), TenantSlug: "tenant",
		ProjectID: ids.NewAt(ids.KindProject, now, 4), ProjectSlug: "project",
		EnvironmentID: runtime.EnvironmentID, EnvironmentName: "main",
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/tenant/project/main",
		BackingServiceID:    ids.NewAt(ids.KindService, now, 5),
		BackingProjectID:    ids.NewAt(ids.KindProject, now, 6), AdapterKey: "manual",
		DesiredRevisionID: runtime.RevisionID, ArtifactID: ids.NewAt(ids.KindConfig, now, 7),
		RenderGeneration: runtime.RenderGeneration, EnvironmentEpochRevision: 1,
		RuntimeProjection: runtime, RunningServiceIDs: []string{consumerID},
		Services: attachTaskServiceSnapshots(runtime.DesiredServices),
		Networks: attachTaskOwnedNetworkSnapshots(runtime.DesiredZones),
		Volumes:  runtime.Volumes, VolumeMounts: runtime.VolumeMounts,
		NetworkJoins: []AttachTaskNetworkJoin{{
			NetworkID: ids.NewAt(ids.KindNetwork, now, 8), ServiceIDs: []string{consumerID},
		}},
		ConsumerServiceIDs:     []string{consumerID},
		ServiceDependencyPlans: runtime.ServiceDependencyPlans.Clone(),
	}
	value, err := encodeAttachTaskRenderInput(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.DesiredServices) < 11 || len(value) >= MaximumAttachTaskRenderInputBytes {
		t.Fatalf("captured %d-Service Attach input size = %d", len(runtime.DesiredServices), len(value))
	}
}

// Rationale: unchanged desired Blueprint state cannot hide a serving-runtime
// change between capture and Attach publication.
func TestAttachRuntimeEpochRejectsStaleCapture(t *testing.T) {
	store := newAttachTestStore()
	scope := seedAttachScope(t, t.Context(), store)
	load := func() *ordinaryEnvironmentMutationContext {
		context, err := loadOrdinaryEnvironmentMutationContext(t.Context(), store,
			scope.Environment.Record.ID, environmentKey(scope.Environment.Record.ID),
			scope.Project.Record.ID, scope.Tenant.Record.ID)
		if err != nil {
			t.Fatal(err)
		}
		return context
	}
	key := environmentMutationEpochKey(scope.Environment.Record.ID)
	captured := load()
	epoch, found := captured.revisionForKey(key)
	if !found {
		t.Fatal("capture lacks epoch")
	}
	input := AttachTaskRenderInput{EnvironmentEpochRevision: epoch}
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
