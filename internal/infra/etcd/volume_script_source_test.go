package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: an explicitly confirmed Volume removal still cannot discard a
// mount source reserved by a Script, even before that Script Task is visible.
func TestDesiredVolumeRemovalProtectsPreparedScriptSource(t *testing.T) {
	ctx := context.Background()
	store, operationID, members, _, _, _ := scriptRunnerSnapshotSourceFixture(t)
	member := members[2]
	if member.Reference.Source.Kind != testscriptsourcereference.SourceVolume {
		t.Fatal("fixture has no Volume source")
	}
	environmentID, volumeID := member.Reference.SourceOwnerID, member.Reference.Source.VolumeID
	previous := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.New(ids.KindTask), RenderGeneration: 1,
		Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{{ID: volumeID, Slug: "data", Key: "data"}},
	})
	seedServiceRepositoryTestDesiredProjection(t, store, previous)
	head := store.valueAt(testblueprints.EnvironmentBlueprintHeadKey(environmentID), store.revision)
	if head == nil {
		t.Fatal("fixture has no desired head")
	}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := testscriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	reserved := []testscriptsourceevidence.ScriptSourcePreparationMember{member}
	if _, err := authority.Prepare(ctx, operationID, reserved); err != nil {
		t.Fatal(err)
	}
	renamed := testenvironmentprojection.CloneEnvironmentComposeProjection(previous)
	renamed.RevisionID, renamed.RenderGeneration = ids.New(ids.KindTask), 2
	renamed.Volumes[0].Slug = "renamed"
	renamed = withTestEnvironmentComposeArtifact(renamed)
	task := workexecution.Task{Action: workexecution.ActionUpdate, TargetID: volumeID,
		Scope: workexecution.Scope{EnvironmentID: environmentID}}
	edit, err := hierarchy.prepareVolumeDesiredReferences(ctx, task, renamed, head.ModRevision, store.revision)
	if err != nil || len(edit) != 0 {
		t.Fatalf("Script reservation blocked a label edit: %v", err)
	}
	next := testenvironmentprojection.CloneEnvironmentComposeProjection(previous)
	next.RevisionID, next.RenderGeneration = ids.New(ids.KindTask), 2
	next.Volumes = nil
	next = withTestEnvironmentComposeArtifact(next)
	task.Action = workexecution.ActionRemove
	before := store.revision
	if _, err := hierarchy.prepareVolumeDesiredReferences(ctx, task, next, head.ModRevision,
		store.revision); !isKind(err, errs.KindResourceInUse) || store.revision != before {
		t.Fatalf("Volume removal ignored a prepared Script mount: %v", err)
	}
	if err := authority.Abandon(ctx, operationID, reserved); err != nil {
		t.Fatal(err)
	}
	removal, err := hierarchy.prepareVolumeDesiredReferences(ctx, task, next, head.ModRevision, store.revision)
	if err != nil {
		t.Fatalf("Volume removal remained blocked after reference release: %v", err)
	}
	transactions := &entryScriptReservationRaceStore{
		memoryHierarchyStore: store,
		member:               member,
	}
	mutation := testkeyvalue.Mutation{
		Type:  testkeyvalue.MutationPut,
		Key:   "/test/volume-removal-committed",
		Value: []byte("1"),
	}
	result, err := transactions.Transact(ctx, removal, []testkeyvalue.Mutation{mutation})
	if err != nil || result.Succeeded || !transactions.reserved || store.valueAt(mutation.Key, store.revision) != nil {
		t.Fatalf("Volume removal lost a late source reservation: %v", err)
	}
	if err := authority.Abandon(ctx, operationID, reserved); err != nil {
		t.Fatal(err)
	}
	result, err = transactions.Transact(ctx, removal, []testkeyvalue.Mutation{mutation})
	if err != nil || !result.Succeeded {
		t.Fatalf("Volume absence conditions stayed blocked after release: %v", err)
	}
}
