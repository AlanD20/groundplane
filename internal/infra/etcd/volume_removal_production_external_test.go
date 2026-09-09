package etcd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	"github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/internal/volume"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the public mutation path must preserve prepared Script sources,
// not merely pass the separately tested desired-publication condition builder.
func TestVolumeRemovalProductionProtectsPreparedScriptSource(t *testing.T) {
	fixture, mutations, reads, created := newVolumeRemovalProductionJourney(t)
	ctx := context.Background()
	before, _, err := fixture.Blueprint.GetEnvironmentBlueprintHead(ctx, fixture.EnvironmentID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.ReserveVolume(t, created.Volume.ID)
	impact, err := reads.GetVolumeDeletionImpact(ctx, created.Volume.ID, "", 40)
	if err != nil || !impact.Complete || impact.ImpactToken == "" {
		t.Fatalf("read complete removal impact: %v", err)
	}
	_, err = mutations.RemoveVolume(
		ctx,
		created.Volume.ID,
		impact.ImpactToken,
		created.Volume.Key,
		"018f3111-0000-7000-8000-000000000002",
	)
	if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("prepared Script source did not block real removal: %v", err)
	}
	after, _, err := fixture.Blueprint.GetEnvironmentBlueprintHead(ctx, fixture.EnvironmentID)
	if err != nil || after.Revision != before.Revision {
		t.Fatalf("rejected removal changed desired head: %v", err)
	}
	fixture.ReleaseVolume(t)
	response, err := mutations.RemoveVolume(
		ctx,
		created.Volume.ID,
		impact.ImpactToken,
		created.Volume.Key,
		"018f3111-0000-7000-8000-000000000003",
	)
	if err != nil || response.Status != http.StatusAccepted {
		t.Fatalf("unreserved removal control: status=%d error=%v", response.Status, err)
	}
}

// Rationale: an accepted DELETE must publish its durable removal operation
// with the Task. A standalone runtime implementation is not production wiring.
func TestVolumeRemovalProductionPublishesDurableOperation(t *testing.T) {
	fixture, mutations, reads, created := newVolumeRemovalProductionJourney(t)
	ctx := context.Background()
	impact, err := reads.GetVolumeDeletionImpact(ctx, created.Volume.ID, "", 40)
	if err != nil || !impact.Complete {
		t.Fatalf("read complete removal impact: %v", err)
	}
	response, err := mutations.RemoveVolume(
		ctx,
		created.Volume.ID,
		impact.ImpactToken,
		created.Volume.Key,
		"018f3111-0000-7000-8000-000000000004",
	)
	if err != nil || response.Status != http.StatusAccepted {
		t.Fatalf("accept Volume removal: status=%d error=%v", response.Status, err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	task, err := fixture.Tasks.GetTask(ctx, accepted.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := volumeremoval.NewEnvironmentVolumeRemovalRuntimeRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	state, err := runtime.Resume(ctx, task.Record.OperationID)
	if err != nil {
		t.Fatalf("accepted DELETE has no resumable Volume operation: %v", err)
	}
	if state.Runtime.Revision != task.Revision || state.Runtime.Record.OriginTaskID != task.Record.ID ||
		state.Runtime.Record.VolumeID != created.Volume.ID ||
		state.Runtime.Record.Checkpoint != volumeremovalrecord.DesiredPublished {
		t.Fatal("removal runtime was not published atomically with its origin Task")
	}
	if task.Record.TimeoutSeconds != volumeremovalrecord.TimeoutSeconds ||
		state.Runtime.Record.StepID != task.Record.Steps[len(task.Record.Steps)-1].ID {
		t.Fatal("removal attempt did not bind its six-hour deadline and final path step")
	}
	resolver, err := controller.NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", fixture.Blueprint, nil)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := volumeremoval.NewEvidenceRepository(fixture.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := resolver.EnableVolumeRemovalPlans(evidence); err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.ResolveExecutionPlan(ctx, task.Record)
	if err != nil || len(plan.GetSteps()) != 2 {
		t.Fatalf("reconstruct accepted DELETE plan: %v", err)
	}
}

// Rationale: retrying a lost private-staging response must preserve the original
// accepted read boundary, even though staging itself advanced the MVCC clock.
func TestVolumeRemovalProductionResumesLostEvidenceResponse(t *testing.T) {
	fixture, mutations, reads, created := newVolumeRemovalProductionJourney(t)
	ctx := context.Background()
	impact, err := reads.GetVolumeDeletionImpact(ctx, created.Volume.ID, "", 40)
	if err != nil {
		t.Fatal(err)
	}
	head, _, err := fixture.Blueprint.GetEnvironmentBlueprintHead(ctx, fixture.EnvironmentID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Evidence.LoseResponse = true
	key := "018f3111-0000-7000-8000-000000000005"
	_, err = mutations.RemoveVolume(ctx, created.Volume.ID, impact.ImpactToken, created.Volume.Key, key)
	if !errors.Is(err, errs.New(errs.KindRequestFailed, "")) || len(fixture.Evidence.WrittenKeys) != 2 {
		t.Fatalf("lose committed manifest response: %v", err)
	}
	manifestKey := fixture.Evidence.WrittenKeys[0]
	before, err := fixture.Store.Get(ctx, manifestKey)
	if err != nil || before == nil || before.Entry == nil {
		t.Fatalf("read committed manifest: %v", err)
	}
	afterHead, _, err := fixture.Blueprint.GetEnvironmentBlueprintHead(ctx, fixture.EnvironmentID)
	if err != nil || afterHead.Revision != head.Revision {
		t.Fatalf("private staging changed desired head: %v", err)
	}
	response, err := mutations.RemoveVolume(ctx, created.Volume.ID, impact.ImpactToken, created.Volume.Key, key)
	if err != nil || response.Status != http.StatusAccepted {
		t.Fatalf("resume DELETE after lost staging response: %v", err)
	}
	after, err := fixture.Store.Get(ctx, manifestKey)
	if err != nil || after == nil || after.Entry == nil || after.Entry.ModRevision != before.Entry.ModRevision ||
		!bytes.Equal(after.Entry.Value, before.Entry.Value) {
		t.Fatalf("resumed DELETE changed its accepted manifest: %v", err)
	}
	replayed, err := mutations.RemoveVolume(ctx, created.Volume.ID, impact.ImpactToken, created.Volume.Key, key)
	if err != nil || replayed.Status != response.Status || !bytes.Equal(replayed.Body, response.Body) {
		t.Fatalf("accepted DELETE replay changed: %v", err)
	}
}

type volumeRemovalProductionRepository struct {
	*etcd.EnvironmentBlueprintRepository
	*desiredrevision.Repository
}

func newVolumeRemovalProductionJourney(t *testing.T) (
	*etcd.VolumeRemovalProductionFixture, *volume.MutationService, *volume.ReadService, api.VolumeMutationResponse,
) {
	t.Helper()
	fixture := etcd.NewVolumeRemovalProductionFixture(t)
	protector, ciphertext := manualJourneyEncryptedValue(t, "volume-removal-test-intent")
	clear(ciphertext)
	coordinator, err := idempotentintent.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	reads, err := volume.NewReadService(fixture.Blueprint)
	if err != nil {
		t.Fatal(err)
	}
	desired, err := desiredrevision.NewRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	repository := volumeRemovalProductionRepository{fixture.Blueprint, desired}
	policies, err := etcd.NewBackupPolicyRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := volumeremoval.NewEvidenceRepository(fixture.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	mutations, err := volume.NewMutationService(
		"/var/lib/groundplane/vol",
		repository,
		coordinator,
		fixture.Idempotency,
		reads,
		policies,
		evidence,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := mutations.CreateVolume(context.Background(), api.VolumeCreate{
		EnvironmentID: fixture.EnvironmentID, Slug: "data", Key: "data",
	}, "018f3111-0000-7000-8000-000000000001")
	if err != nil || response.Status != http.StatusCreated {
		t.Fatalf("create production Volume fixture: status=%d error=%v", response.Status, err)
	}
	var created api.VolumeMutationResponse
	if err := json.Unmarshal(response.Body, &created); err != nil {
		t.Fatal(err)
	}
	fixture.CompleteCreate(t, created.TaskID)
	return fixture, mutations, reads, created
}
