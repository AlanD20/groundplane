package etcd

import (
	"bytes"
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// VOL-07/BP-04: Rationale: the authored Blueprint entry point uses the same desired head as
// direct mutations and must not invalidate a Volume removal's pinned revision.
func TestBlueprintPublicationExcludesVolumeRemovalLock(t *testing.T) {
	for _, locked := range []bool{false, true} {
		t.Run(map[bool]string{false: "unlocked control", true: "held removal lock"}[locked], func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryHierarchyStore()
			repository, err := newHierarchyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			project, environment := createEnvironmentBlueprintOwners(t, repository)
			task := environmentBlueprintTestTask(t, project.Record, environment.Record, 32100)
			projection := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
			marker := environmentBlueprintTestMarker(task, environment.Record.ID)
			zones := environmentBlueprintTestZoneChanges(t, repository, projection)
			services := environmentBlueprintTestServiceChanges(t, repository, projection)
			routes := environmentBlueprintTestRouteChanges(t, repository, projection)
			// Only the sealed baseline records and storage are fixtures; publication
			// goes through the real authored Blueprint repository entry point.
			claim := stageEnvironmentBlueprintForPublicationTest(t, repository, 0,
				environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n"), projection, marker)
			if locked {
				value, err := removalrecord.EncodeOwner(removalrecord.Owner{
					VolumeID: projection.Volumes[0].ID, EnvironmentID: environment.Record.ID, OperationID: ids.New(ids.KindOperation),
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut,
					Key: removalrecord.EnvironmentLockKey(environment.Record.ID), Value: value}}); err != nil {
					t.Fatal(err)
				}
			}
			before := store.revision
			lockKey := removalrecord.EnvironmentLockKey(environment.Record.ID)
			lockBefore := store.valueAt(lockKey, before)
			result, err := publishEnvironmentBlueprintClaimTest(
				repository,
				ctx,
				project,
				environment,
				0,
				claim,
				EnvironmentDesiredRevisionIdentity{
					EnvironmentID: environment.Record.ID,
					RevisionID:    task.ID,
				},
				projection,
				zones,
				services,
				routes,
				ReleaseGroupBlueprintPreparedMutation{},
				ComponentTaskPreparation{},
				BlueprintAttachTaskPreparation{},
				task,
				marker,
			)
			if err != nil {
				t.Fatal(err)
			}
			outcome, _, conflict, err := result.Classify()
			if !locked {
				if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
					t.Fatalf("unlocked Blueprint rejected: %v/%v/%v", outcome, conflict, err)
				}
				return
			}
			if err != nil || outcome != IdempotencyKnownConflict || !isKind(conflict, errs.KindStateConflict) {
				t.Fatalf("Blueprint published across removal lock: %v/%v/%v", outcome, conflict, err)
			}
			// Private source staging may advance storage revision; no public
			// authority may appear, and the original removal lock must survive.
			for _, key := range []string{taskKey(task.ID), taskQueueKey(task.Executor, task.ID),
				environmentBlueprintHeadKey(environment.Record.ID), runtimeConfigurationHeadKey(environment.Record.ID)} {
				if store.valueAt(key, store.revision) != nil {
					t.Fatalf("rejected Blueprint published %q", key)
				}
			}
			lockAfter := store.valueAt(lockKey, store.revision)
			if lockBefore == nil || lockAfter == nil || lockBefore.ModRevision != lockAfter.ModRevision ||
				!bytes.Equal(lockBefore.Value, lockAfter.Value) {
				t.Fatal("rejected Blueprint changed the removal lock")
			}
		})
	}
}
