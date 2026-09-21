package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: rejecting head promotion must not reject a read-only acknowledgement
// replay for an already failed Route removal when a later Volume removal owns the Environment.
func TestRouteFailedRemovalReplayIgnoresLaterVolumeRemovalLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1170, "/replay/*")
	projection := routeDeletionTestProjection(
		t,
		store,
		environment,
		project,
		target,
		testkeyvalue.Versioned[testroutes.Record]{Record: record},
	)
	current, err := repository.GetRoute(ctx, record.Desired.ID)
	if err != nil {
		t.Fatal(err)
	}
	task, marker, tombstone, intent := routeDeletionTestRecords(t, project, environment, current, projection)
	result, err := repository.BeginRouteDeletionWithTask(ctx, environment, project, target,
		current, &projection, tombstone, intent, task, marker)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("begin: %v/%v/%v", outcome, conflict, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("claim: %v/%v", found, err)
	}
	at := task.CreatedAt.Add(2 * time.Second)
	if _, err := tasks.AcknowledgeControllerTask(ctx, task.ID, testtaskjournal.TaskStatusFailed, at); err != nil {
		t.Fatal(err)
	}
	putSpecializedRemovalLock(t, store, removalrecord.Owner{EnvironmentID: environment.Record.ID,
		VolumeID: ids.New(ids.KindVolume), OperationID: ids.New(ids.KindOperation)})
	before := store.revision
	replayed, err := tasks.AcknowledgeControllerTask(ctx, task.ID, testtaskjournal.TaskStatusFailed, at)
	if err != nil || replayed.Record.Status != testtaskjournal.TaskStatusFailed || store.revision != before {
		t.Fatalf("failed acknowledgement replay changed state or rejected later lock: %v", err)
	}
}

// Rationale: direct Route creation advances the Environment head immediately,
// so admission must compare removal ownership in the final transaction too.
func TestRouteMutationPublicationExcludesVolumeRemovalLock(t *testing.T) {
	for _, mode := range []string{"unlocked", "held", "late"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			_, store, environment, project, target := routeRepositoryTestHierarchy(t)
			record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1160, "/lock/*")
			// Match the existing Route mutation fixture's independent Service authority.
			fenceKey := "/v1/test/route-service-fences/" + target.Record.Desired.ID
			fence, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
				Type: testkeyvalue.MutationPut, Key: fenceKey, Value: []byte(target.Record.Desired.ID),
			}})
			if err != nil || !fence.Succeeded {
				t.Fatalf("Service fence: %v", err)
			}
			target = refenceServiceFixture(t, store, target, fenceKey, fence.Revision)
			target.Revision, target.ReadRevision = fence.Revision, fence.Revision
			at := serviceRecordTestTime().Add(3 * time.Hour)
			task := validTaskRecord(at)
			task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
			task.Executor, task.Type, task.Target = testtaskjournal.TaskExecutorController, testtaskjournal.TaskCreate, record.Desired.ID
			task.Params = map[string]string{
				testtaskjournal.TaskResourceKindParam:     testtaskjournal.TaskResourceRoute,
				testtaskjournal.TaskRouteEnvironmentParam: environment.Record.ID,
			}
			task.IdempotencyKey = "route-create-removal-lock"
			intent, err := testenvironmentchanges.NewRouteMutationIntent(
				task.ID,
				task.OperationID,
				environment.Record.ID,
				record,
				nil,
				nil,
				at,
			)
			if err != nil {
				t.Fatal(err)
			}
			marker, err := testidempotency.NewCompletedDirectIdempotencyMarker(testidempotency.IdempotencyLocator{
				ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
				Method: http.MethodPost, Route: "/routes", Key: task.IdempotencyKey,
			}, testDirectMarker().Intent, testidempotency.IdempotencyResponse{
				Status: http.StatusAccepted, ContentKind: "application/json",
				Body: []byte(`{"route":{"id":"` + record.Desired.ID + `"},"task_id":"` + task.ID + `"}`),
			}, at)
			if err != nil {
				t.Fatal(err)
			}
			owner := removalrecord.Owner{EnvironmentID: environment.Record.ID,
				VolumeID: ids.New(ids.KindVolume), OperationID: ids.New(ids.KindOperation)}
			backend := &specializedRemovalLockRaceStore{memoryHierarchyStore: store}
			if mode == "held" {
				putSpecializedRemovalLock(t, store, owner)
			} else if mode == "late" {
				backend.owner = &owner
			}
			repository, err := newRouteRepository(backend)
			if err != nil {
				t.Fatal(err)
			}
			before := store.revision
			result, err := repository.BeginRouteMutationWithTask(ctx, environment, project, target,
				nil, record, intent, task, marker)
			if err != nil {
				t.Fatal(err)
			}
			outcome, _, conflict, err := result.Classify()
			if mode == "unlocked" {
				if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
					t.Fatalf("unlocked mutation: %v/%v/%v", outcome, conflict, err)
				}
				return
			}
			if err != nil || outcome != IdempotencyKnownConflict || !isKind(conflict, errs.KindStateConflict) {
				t.Fatalf("Route mutation ignored removal lock: %v/%v/%v", outcome, conflict, err)
			}
			if mode == "late" {
				before++
			}
			if store.revision != before {
				t.Fatal("rejected Route mutation wrote state")
			}
		})
	}
}

// Rationale: Route removal stages its candidate at admission but promotes the
// desired head at completion; both commits must exclude Volume-removal ownership.
func TestRouteRemovalPublicationsExcludeVolumeRemovalLock(t *testing.T) {
	for _, phase := range []string{"begin", "complete"} {
		for _, mode := range []string{"unlocked", "held", "late"} {
			t.Run(phase+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
				record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1150, "/lock/*")
				projection := routeDeletionTestProjection(
					t,
					store,
					environment,
					project,
					target,
					testkeyvalue.Versioned[testroutes.Record]{Record: record},
				)
				current, err := repository.GetRoute(ctx, record.Desired.ID)
				if err != nil {
					t.Fatal(err)
				}
				task, marker, tombstone, intent := routeDeletionTestRecords(
					t,
					project,
					environment,
					current,
					projection,
				)
				backend := &specializedRemovalLockRaceStore{memoryHierarchyStore: store}
				repository, err = newRouteRepository(backend)
				if err != nil {
					t.Fatal(err)
				}
				begin := func() error {
					result, err := repository.BeginRouteDeletionWithTask(ctx, environment, project, target,
						current, &projection, tombstone, intent, task, marker)
					if err != nil {
						return err
					}
					outcome, _, conflict, err := result.Classify()
					if err != nil {
						return err
					}
					if conflict != nil {
						return conflict
					}
					if outcome != IdempotencyKnownApplied {
						t.Fatalf("unexpected outcome: %v", outcome)
					}
					return nil
				}
				publish := begin
				if phase == "complete" {
					if err := begin(); err != nil {
						t.Fatal(err)
					}
					tasks, err := newTaskRepository(backend)
					if err != nil {
						t.Fatal(err)
					}
					claim, found, err := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second))
					if err != nil || !found || claim.Task.Record.ID != task.ID {
						t.Fatalf("claim: %v/%v", found, err)
					}
					publish = func() error {
						_, err := tasks.AcknowledgeControllerTask(
							ctx,
							task.ID,
							testtaskjournal.TaskStatusCompleted,
							task.CreatedAt.Add(2*time.Second),
						)
						return err
					}
				}
				owner := removalrecord.Owner{EnvironmentID: environment.Record.ID,
					VolumeID: ids.New(ids.KindVolume), OperationID: ids.New(ids.KindOperation)}
				if mode == "held" {
					putSpecializedRemovalLock(t, store, owner)
				} else if mode == "late" {
					backend.owner = &owner
				}
				before := store.revision
				err = publish()
				if mode == "unlocked" {
					if err != nil || store.revision <= before {
						t.Fatalf("unlocked publication: %v", err)
					}
					return
				}
				if !isKind(err, errs.KindStateConflict) {
					t.Fatalf("Route publication ignored removal lock: %v", err)
				}
				if mode == "late" {
					before++
				}
				if store.revision != before {
					t.Fatal("rejected Route publication wrote state")
				}
			})
		}
	}
}
