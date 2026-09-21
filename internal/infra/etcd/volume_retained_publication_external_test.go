package etcd_test

import (
	"encoding/json"
	"testing"

	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: QA retained a native runtime through a later Blueprint Apply.
// Direct Volume mutation uses the sealed desired source, not a new Blueprint
// Release fragment. Exercise the actual publisher after that exact sequence.
func TestVolumeMutationProductionAfterRetainedBlueprint(t *testing.T) {
	testBlueprintExecutedArtifact(t, true, false, func(fixture *etcd.ExecutedArtifactFixture,
		resolver *testtaskplanning.TaskPlanResolver, prior testreleaserender.ReleaseRenderInput, _ domain.Intent, _ *agentpb.ComposeArtifact) {
		ctx := t.Context()
		project, err := testcomposerender.LoadNormalizedEnvironmentProject(ctx, prior.Projection)
		if err != nil {
			t.Fatal(err)
		}
		proveRetainedBlueprintProducer(t, fixture, resolver, project, prior.ServiceID, false)
		volumes, mutations, reads, created := newPendingVolumeProductionJourney(t, fixture.VolumeMutationFixture(t))
		volumes.CompleteCreate(t, created.TaskID)
		response, err := mutations.EditVolume(ctx, created.Volume.ID, api.VolumeEdit{Slug: "renamed"},
			"018f3111-0000-7000-8000-000000000081")
		if err != nil {
			t.Fatal("retained Volume edit", err)
		}
		var edited api.VolumeMutationResponse
		if err := json.Unmarshal(response.Body, &edited); err != nil {
			t.Fatal(err)
		}
		volumes.CompleteCreate(t, edited.TaskID)
		impact, err := reads.GetVolumeDeletionImpact(ctx, created.Volume.ID, "", 40)
		if err != nil {
			t.Fatal(err)
		}
		response, err = mutations.RemoveVolume(ctx, created.Volume.ID, impact.ImpactToken, created.Volume.Key,
			"018f3111-0000-7000-8000-000000000082")
		if err != nil {
			t.Fatal("retained Volume DELETE", err)
		}
		var removed api.TaskAccepted
		if err := json.Unmarshal(response.Body, &removed); err != nil {
			t.Fatal(err)
		}
		task, err := volumes.Tasks.GetTask(ctx, removed.TaskID)
		if err != nil {
			t.Fatal(err)
		}
		evidence, err := volumeremoval.NewEvidenceRepository(volumes.Evidence)
		if err != nil {
			t.Fatal(err)
		}
		if err := resolver.EnableVolumeRemovalPlans(evidence); err != nil {
			t.Fatal(err)
		}
		if _, err := resolver.ResolveExecutionPlan(ctx, task.Record); err != nil {
			t.Fatal("retained DELETE plan reconstruction", err)
		}
	})
}
