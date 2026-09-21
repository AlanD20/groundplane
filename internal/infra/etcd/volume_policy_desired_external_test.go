package etcd_test

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a policy fragment is not sufficient. The actual desired publisher
// must commit the last-source replacement and scheduling floor with the Task.
func TestVolumePolicyDesiredPublicationIsAtomic(t *testing.T) {
	ctx := context.Background()
	fixture := etcd.NewVolumePolicyDesiredFixture(t)
	stageVolumePolicyDesired(t, fixture)
	earliest := time.Now().UTC()
	result, err := fixture.Publish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
		t.Fatalf("publish: %v/%v/%v", outcome, conflict, err)
	}
	fixture.AssertAtomicPolicy(t, earliest)
	before := fixture.Revision()
	result, err = fixture.Publish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err = result.Classify()
	if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownExisting || fixture.Revision() != before {
		t.Fatalf("equal publication replay rewrote policy or Task: %v/%v/%v", outcome, conflict, err)
	}
}

func stageVolumePolicyDesired(t *testing.T, fixture *etcd.VolumePolicyDesiredFixture) {
	t.Helper()
	ctx := context.Background()
	staging, err := desiredrevision.NewRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Request.Claim, err = staging.ClaimEnvironmentBlueprintStage(
		ctx,
		testblueprints.EnvironmentBlueprintStageClaimRequest{
			EnvironmentID: fixture.Task.Owner.EnvironmentID, CandidateRevisionID: fixture.Task.ID, CandidateTaskID: fixture.Task.ID,
			Locator: fixture.Marker.Locator, Intent: fixture.Marker.Intent, BaselineHeadRevision: fixture.HeadRevision,
			SourceKind: testblueprints.EnvironmentBlueprintSourceMutation, RenderGeneration: 2,
			ProjectionSchema: testblueprints.EnvironmentDesiredProjectionSchema, CreatedAt: fixture.Task.CreatedAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := staging.StageEnvironmentBlueprintRevision(ctx, fixture.Request); err != nil {
		t.Fatal(err)
	}
}

// Rationale: each policy authority remains fenced in the combined transaction;
// an epoch overlap must not silently discard stale preparation evidence.
func TestVolumePolicyDesiredPublicationRejectsRaces(t *testing.T) {
	for _, authority := range []string{"policy", "coordination", "epoch", "source", "connector reference"} {
		t.Run(authority, func(t *testing.T) {
			fixture := etcd.NewVolumePolicyDesiredFixture(t)
			stageVolumePolicyDesired(t, fixture)
			fixture.Race(t, authority)
			before := fixture.Revision()
			result, err := fixture.Publish(context.Background())
			if err == nil {
				outcome, _, conflict, classifyErr := result.Classify()
				if classifyErr != nil || outcome != etcd.IdempotencyKnownConflict {
					t.Fatalf("race published: %v/%v", outcome, classifyErr)
				}
				err = conflict
			}
			if kind, _ := errs.KindOf(err); kind != errs.KindStateConflict {
				t.Fatalf("race classification: %v", err)
			}
			fixture.AssertUnpublished(t, before)
		})
	}
}

// Rationale: valid sealed desired decisions cannot substitute a different
// policy replacement for the exact fixed-revision Volume preparation.
func TestVolumePolicyDesiredPublicationRejectsDifferentStagedPolicy(t *testing.T) {
	fixture := etcd.NewVolumePolicyDesiredFixture(t)
	fixture.Request.Projection.Backup.Keep = 8
	digest, err := testblueprints.EnvironmentBlueprintDependencyDigest(fixture.Request.Projection)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Request.DependencyDigest = digest
	stageVolumePolicyDesired(t, fixture)
	before := fixture.Revision()
	_, err = fixture.Publish(context.Background())
	if kind, _ := errs.KindOf(err); kind != errs.KindValidationFailed {
		t.Fatalf("substituted policy published: %v", err)
	}
	fixture.AssertUnpublished(t, before)
}

// Rationale: ADR0049 inherits ADR0051's bounded final-publication envelope;
// the supported 12-source policy must not be rejected by an ordinary-mutation
// partition guard, nor made to fit by dropping selected-source comparisons.
func TestVolumePolicyDesiredPublicationMaximumSelection(t *testing.T) {
	fixture := etcd.NewVolumePolicyDesiredFixture(t)
	fixture.UseMaximumSelection(t)
	stageVolumePolicyDesired(t, fixture)
	result, err := fixture.Publish(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
		t.Fatalf("maximum publication: %v/%v/%v", outcome, conflict, err)
	}
	fixture.AssertMaximumSelectionPublished(t)
}
