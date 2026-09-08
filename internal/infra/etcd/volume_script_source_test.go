package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: an explicitly confirmed Volume removal still cannot discard a
// mount source reserved by a Script, even before that Script Task is visible.
func TestDesiredVolumeRemovalProtectsPreparedScriptSource(t *testing.T) {
	ctx := context.Background()
	store, operationID, members, _, _, _ := scriptRunnerSnapshotSourceFixture(t)
	member := members[2]
	if member.Reference.Source.Kind != ScriptSourceVolume {
		t.Fatal("fixture has no Volume source")
	}
	environmentID, volumeID := member.Reference.SourceOwnerID, member.Reference.Source.VolumeID
	previous := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.New(ids.KindTask), RenderGeneration: 1,
		Volumes: []EnvironmentVolumeIdentity{{ID: volumeID, Slug: "data", Key: "data"}},
	})
	seedServiceRepositoryTestDesiredProjection(t, store, previous)
	head := store.valueAt(environmentBlueprintHeadKey(environmentID), store.revision)
	if head == nil {
		t.Fatal("fixture has no desired head")
	}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := newScriptSourceReferenceAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	reserved := []ScriptSourcePreparationMember{member}
	if _, err := authority.Prepare(ctx, operationID, reserved); err != nil {
		t.Fatal(err)
	}
	renamed := cloneEnvironmentComposeProjection(previous)
	renamed.RevisionID, renamed.RenderGeneration = ids.New(ids.KindTask), 2
	renamed.Volumes[0].Slug = "renamed"
	renamed = withTestEnvironmentComposeArtifact(renamed)
	edit, err := hierarchy.prepareDesiredScriptRemoval(ctx, environmentID, head.ModRevision,
		store.revision, renamed, TaskRecord{Type: TaskUpdate})
	if err != nil || len(edit.conditions) != 0 {
		t.Fatalf("Script reservation blocked a label edit: %v", err)
	}
	next := cloneEnvironmentComposeProjection(previous)
	next.RevisionID, next.RenderGeneration = ids.New(ids.KindTask), 2
	next.Volumes = nil
	next = withTestEnvironmentComposeArtifact(next)
	task := TaskRecord{Type: TaskRemove, Target: volumeID, Status: TaskStatusPending,
		Params: map[string]string{TaskResourceKindParam: TaskResourceVolume}}
	before := store.revision
	if _, err := hierarchy.prepareDesiredScriptRemoval(ctx, environmentID, head.ModRevision,
		store.revision, next, task); !isKind(err, errs.KindResourceInUse) || store.revision != before {
		t.Fatalf("Volume removal ignored a prepared Script mount: %v", err)
	}
	if err := authority.Abandon(ctx, operationID, reserved); err != nil {
		t.Fatal(err)
	}
	removal, err := hierarchy.prepareDesiredScriptRemoval(ctx, environmentID, head.ModRevision,
		store.revision, next, task)
	if err != nil || removal.volumeID != volumeID || len(removal.conditions) != 2 {
		t.Fatalf("Volume removal remained blocked after reference release: %v", err)
	}
	transactions := &entryScriptReservationRaceStore{
		connectorReferenceRaceStore: &connectorReferenceRaceStore{memoryHierarchyStore: store, injected: true},
		member:                      member,
	}
	mutation := Mutation{Type: MutationPut, Key: "/test/volume-removal-committed", Value: []byte("1")}
	result, err := transactions.Transact(ctx, removal.conditions, []Mutation{mutation})
	if err != nil || result.Succeeded || !transactions.reserved || store.valueAt(mutation.Key, store.revision) != nil {
		t.Fatalf("Volume removal lost a late source reservation: %v", err)
	}
	classified := removal.classifyConflict(0, func(_ int64, _ []*KeyValue) error {
		return errs.New(errs.KindStateConflict, "unexpected baseline conflict")
	})
	if err := classified(result.Revision, result.FailureReads); !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("Volume removal misclassified a late source reservation: %v", err)
	}
	if err := authority.Abandon(ctx, operationID, reserved); err != nil {
		t.Fatal(err)
	}
	result, err = transactions.Transact(ctx, removal.conditions, []Mutation{mutation})
	if err != nil || !result.Succeeded {
		t.Fatalf("Volume absence conditions stayed blocked after release: %v", err)
	}
}
