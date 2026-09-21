package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testhierarchydeletionfinalization "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the shared hierarchy engine's Environment finalizer must retain the
// same lock fence as the Task acknowledgement path, even at the mutation commit.
func TestHierarchyEnvironmentFinalizerExcludesVolumeRemovalLock(t *testing.T) {
	for _, mode := range []string{"unlocked", "held", "late"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			fixture := newEnvironmentDeletionLockFixture(t)
			backend := &specializedRemovalLockRaceStore{memoryHierarchyStore: fixture.store}
			journal, err := newHierarchyDeletionRepository(backend)
			if err != nil {
				t.Fatal(err)
			}
			begin := hierarchyDeletionCreationTestBegin(
				fixture.now,
				testhierarchydeletion.HierarchyDeletionTargetEnvironment,
				fixture.environment.Record.ID,
				testhierarchydeletion.HierarchyDeletionOperationEnvironment,
				"6",
			)
			created, err := journal.Begin(ctx, begin)
			if err != nil {
				t.Fatal(err)
			}
			owner := removalrecord.Owner{EnvironmentID: fixture.environment.Record.ID,
				VolumeID: ids.New(ids.KindVolume), OperationID: ids.New(ids.KindOperation)}
			if mode == "held" {
				putSpecializedRemovalLock(t, fixture.store, owner)
			} else if mode == "late" {
				backend.owner = &owner
			}
			before := fixture.store.revision
			effects, err := testhierarchydeletionfinalization.NewPreparer(backend).Prepare(
				ctx, created.Operation, testhierarchydeletion.HierarchyDeletionAction{
					ActionKind: testhierarchydeletion.HierarchyDeletionEnvironmentFinalize,
					TargetID:   owner.EnvironmentID, TargetRevision: fixture.environment.Revision,
				},
			)
			defer testkeyvalue.ClearByteSlices(effects.Values())
			if mode == "held" {
				if !isKind(err, errs.KindStateConflict) || fixture.store.revision != before {
					t.Fatalf("hierarchy finalizer ignored held Volume removal lock: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := backend.Transact(ctx, effects.Conditions(), effects.Mutations())
			if err != nil {
				t.Fatal(err)
			}
			if mode == "unlocked" {
				if !result.Succeeded {
					t.Fatal("unlocked hierarchy finalizer rejected")
				}
				assertEnvironmentDeletionCompanion(
					t,
					fixture.store,
					testhierarchy.EnvironmentKey(owner.EnvironmentID),
					false,
				)
				return
			}
			if result.Succeeded || fixture.store.revision != before+1 {
				t.Fatal("hierarchy finalizer committed over late Volume removal ownership")
			}
			assertEnvironmentDeletionCompanion(
				t,
				fixture.store,
				testhierarchy.EnvironmentKey(owner.EnvironmentID),
				true,
			)
		})
	}
}

// Rationale: deleting the Environment must not orphan a separately owned Volume
// removal, including when its lock appears after the finalizer's snapshot read.
func TestEnvironmentDeletionCompletionExcludesVolumeRemovalLock(t *testing.T) {
	for _, mode := range []string{"unlocked", "held", "late"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			fixture := newEnvironmentDeletionLockFixture(t)
			fixture.mustBegin(t)
			agentID := ids.New(ids.KindAgent)
			if _, found, err := fixture.tasks.ClaimNextTask(ctx, agentID, 1,
				fixture.now.Add(time.Second)); err != nil || !found {
				t.Fatalf("claim: %v/%v", found, err)
			}
			owner := removalrecord.Owner{EnvironmentID: fixture.environment.Record.ID,
				VolumeID: ids.New(ids.KindVolume), OperationID: ids.New(ids.KindOperation)}
			backend := &specializedRemovalLockRaceStore{memoryHierarchyStore: fixture.store}
			if mode == "held" {
				putSpecializedRemovalLock(t, fixture.store, owner)
			} else if mode == "late" {
				backend.owner = &owner
			}
			tasks, err := newTaskRepository(backend)
			if err != nil {
				t.Fatal(err)
			}
			before := fixture.store.revision
			_, err = tasks.AcknowledgeTask(
				ctx,
				agentID,
				1,
				fixture.task.ID,
				taskAssignmentIDForTest(
					t,
					fixture.tasks,
					fixture.task.ID,
				),
				testtaskjournal.TaskStatusCompleted,
				testtaskjournal.TaskResultRecord{
					Kind:       testtaskjournal.TaskResultEnvironmentDirectory,
					Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
				},
				fixture.now.Add(2*time.Second),
			)
			if mode == "unlocked" {
				if err != nil {
					t.Fatal(err)
				}
				assertEnvironmentDeletionCompanion(
					t,
					fixture.store,
					testhierarchy.EnvironmentKey(owner.EnvironmentID),
					false,
				)
				return
			}
			if !isKind(err, errs.KindStateConflict) {
				t.Fatalf("Environment finalization ignored Volume removal: %v", err)
			}
			if mode == "late" {
				before++
			}
			if fixture.store.revision != before {
				t.Fatal("rejected Environment finalization wrote state")
			}
			assertEnvironmentDeletionCompanion(
				t,
				fixture.store,
				testhierarchy.EnvironmentKey(owner.EnvironmentID),
				true,
			)
			assertEnvironmentDeletionCompanion(
				t,
				fixture.store,
				removalrecord.EnvironmentLockKey(owner.EnvironmentID),
				true,
			)
		})
	}
}
