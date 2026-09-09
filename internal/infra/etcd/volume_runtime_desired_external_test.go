package etcd_test

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: records prepared independently are not an operation. The existing
// desired publisher must commit the runtime, owner, policy, Task and root response
// together, with no standalone Create call needed by the runtime repository.
func TestVolumeRuntimeDesiredPublicationIsAtomic(t *testing.T) {
	ctx := context.Background()
	fixture := etcd.NewVolumePolicyDesiredFixture(t)
	want := fixture.PrepareRemovalRecords(t)
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
	runtime, err := volumeremoval.NewEnvironmentVolumeRemovalRuntimeRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := runtime.Resume(ctx, want.OperationID)
	if err != nil {
		t.Fatalf("accepted desired publication has no resumable removal: %v", err)
	}
	if resumed.Runtime.Record != want || resumed.Runtime.Revision != fixture.Revision() ||
		resumed.Progress.Revision != fixture.Revision() || resumed.Attempt.TaskID != fixture.Task.ID ||
		resumed.Attempt.Ordinal != 1 || resumed.Pending != nil {
		t.Fatal("publisher did not commit the exact initial runtime")
	}
	markerKey, err := etcd.CapabilityIdempotencyMarkerKey(fixture.Marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{removalrecord.OwnerKey(want.VolumeID), removalrecord.AttemptKey(want.OperationID, 1),
		etcd.CapabilityTaskKey(fixture.Task.ID), markerKey} {
		read, err := fixture.Store.Get(ctx, key)
		if err != nil || read.Entry == nil || read.Entry.ModRevision != resumed.Runtime.Revision {
			t.Fatalf("removal companion was not atomic: %s: %v", key, err)
		}
	}
	fixture.AssertAtomicPolicy(t, earliest)
	before := fixture.Revision()
	result, err = fixture.Publish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err = result.Classify()
	if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownExisting || fixture.Revision() != before {
		t.Fatalf("equal publication rewrote removal state: %v/%v/%v", outcome, conflict, err)
	}
}

// Rationale: a competing owner can appear after all baseline reads. Its CAS
// must defeat the full publication without changing policy, desired head or Task.
func TestVolumeRuntimeDesiredPublicationRejectsLateOwner(t *testing.T) {
	fixture := etcd.NewVolumePolicyDesiredFixture(t)
	want := fixture.PrepareRemovalRecords(t)
	stageVolumePolicyDesired(t, fixture)
	fixture.OwnerBeforePublication = &removalrecord.Owner{
		VolumeID: want.VolumeID, EnvironmentID: want.EnvironmentID, OperationID: ids.New(ids.KindOperation),
	}
	before := fixture.Revision()
	result, err := fixture.Publish(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if kind, _ := errs.KindOf(conflict); err != nil || kind != errs.KindStateConflict ||
		outcome != etcd.IdempotencyKnownConflict {
		t.Fatalf("late owner published competing removal: %v/%v/%v", outcome, conflict, err)
	}
	fixture.AssertUnpublished(t, before+1)
	read, err := fixture.Store.Get(context.Background(), removalrecord.OwnerKey(want.VolumeID))
	if err != nil || read.Entry == nil {
		t.Fatalf("winning owner lost: %v", err)
	}
	owner, err := removalrecord.DecodeOwner(read.Entry.Value)
	if err != nil || owner != *fixture.OwnerBeforePublication {
		t.Fatalf("winning owner overwritten: %v", err)
	}
}

// Rationale: prior ownership or orphaned operation evidence must defeat the
// entire desired/policy/Task publication, not just a later runtime write.
func TestVolumeRuntimeDesiredPublicationRejectsOccupiedRecords(t *testing.T) {
	for _, occupied := range []string{"owner", "completion"} {
		t.Run(occupied, func(t *testing.T) {
			ctx := context.Background()
			fixture := etcd.NewVolumePolicyDesiredFixture(t)
			runtime := fixture.PrepareRemovalRecords(t)
			stageVolumePolicyDesired(t, fixture)
			key := removalrecord.OwnerKey(runtime.VolumeID)
			if occupied == "completion" {
				key = removalrecord.CompletionKey(runtime.OperationID, 1)
			}
			if _, err := fixture.Store.Put(ctx, key, []byte("occupied")); err != nil {
				t.Fatal(err)
			}
			before := fixture.Revision()
			result, err := fixture.Publish(ctx)
			if err == nil {
				outcome, _, conflict, classifyErr := result.Classify()
				if classifyErr != nil || outcome != etcd.IdempotencyKnownConflict {
					t.Fatalf("occupied records did not defeat publication: %v/%v", outcome, classifyErr)
				}
				err = conflict
			}
			if kind, _ := errs.KindOf(err); kind != errs.KindStateConflict {
				t.Fatalf("occupied records classification: %v", err)
			}
			fixture.AssertUnpublished(t, before)
		})
	}
}

// Rationale: a self-consistent Task/runtime key must still match the immutable
// key of the Volume removed from the desired baseline.
func TestVolumeRuntimeDesiredPublicationRejectsSubstitutedKey(t *testing.T) {
	fixture := etcd.NewVolumePolicyDesiredFixture(t)
	runtime := fixture.PrepareRemovalRecords(t)
	runtime.Key = "unrelated-directory"
	initial, err := removalrecord.PrepareInitialPublication(runtime)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Initial = &initial
	fixture.Task.Params = etcd.EnvironmentVolumeRemovalTaskParams(runtime, 1)
	stageVolumePolicyDesired(t, fixture)
	before := fixture.Revision()
	_, err = fixture.Publish(context.Background())
	if kind, _ := errs.KindOf(err); kind != errs.KindValidationFailed {
		t.Fatalf("substituted immutable Volume key published: %v", err)
	}
	fixture.AssertUnpublished(t, before)
}

// Rationale: adding runtime ownership must not drop policy source comparisons
// or push the maximum legal selection outside the existing publication envelope.
func TestVolumeRuntimeDesiredPublicationMaximumSelection(t *testing.T) {
	fixture := etcd.NewVolumePolicyDesiredFixture(t)
	fixture.UseMaximumSelection(t)
	want := fixture.PrepareRemovalRecords(t)
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
	runtime, err := volumeremoval.NewEnvironmentVolumeRemovalRuntimeRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := runtime.Resume(context.Background(), want.OperationID)
	if err != nil || resumed.Runtime.Record != want || resumed.Runtime.Revision != fixture.Revision() {
		t.Fatalf("maximum publication did not include runtime: %v", err)
	}
}

// Rationale: an existing materialization writer may still be applying the old
// desired state. Removal must not publish over it, even if it claims after reads.
func TestVolumeRuntimeDesiredPublicationExcludesMaterializationWriter(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(map[bool]string{false: "held", true: "late claim"}[late], func(t *testing.T) {
			fixture := etcd.NewVolumePolicyDesiredFixture(t)
			fixture.PrepareRemovalRecords(t)
			stageVolumePolicyDesired(t, fixture)
			fixture.HoldMaterializationWriter(t, late)
			before := fixture.Revision()
			result, err := fixture.Publish(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			outcome, _, conflict, err := result.Classify()
			if kind, _ := errs.KindOf(conflict); err != nil || outcome != etcd.IdempotencyKnownConflict ||
				kind != errs.KindStateConflict {
				t.Fatalf("removal published over active writer: %v/%v/%v", outcome, conflict, err)
			}
			if late {
				before++ // Only the competing writer acquisition commits.
			}
			fixture.AssertUnpublished(t, before)
		})
	}
}
