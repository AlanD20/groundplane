package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// DNS-04: Environment-scoped Component completion must promote the candidate
// into DNS planning and publish its projection, just like a direct Component Task.
func TestBlueprintComponentCompletionReconcilesHostResolution(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	task := validTaskRecord(now)
	task.Type = taskjournal.TaskUpdate
	task.Target = ids.NewAt(ids.KindEnvironment, now, 2101)
	pinComponentTaskDesiredRevision(&task)
	task.Params[componentTaskBlueprintProcedureParam] = componentTaskBlueprintProcedureNone
	delete(task.Params, taskjournal.TaskResourceKindParam)
	createLifecycleTask(t, repository, task)
	records := componentTaskLifecycleRecords(t, task.Target, now, true)
	seedComponentTaskLifecycle(t, store, task, records)
	read, err := store.Get(ctx, resolutionrecord.StorageKey)
	if err != nil {
		t.Fatal(err)
	}
	_, _, components, err := repository.hostResolutionTerminalOverlay(
		ctx,
		task,
		taskjournal.TaskStatusCompleted,
		read.ReadRevision,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	component, found := components[records.candidate.Desired.ID]
	if !found || !component.Runtime.Healthy || component.Runtime.PinnedIPv4 != records.candidate.Runtime.PinnedIPv4 {
		t.Fatal("Blueprint completion did not promote the exact router candidate into DNS input")
	}
	change, err := repository.prepareHostResolutionReconciliation(
		ctx,
		task,
		taskjournal.TaskStatusCompleted,
		read.ReadRevision,
		nil,
		platformComponentTaskChange{},
		taskMaterializationProjectionChange{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clearHostResolutionReconciliationChange(change)
	for _, mutation := range change.mutations {
		if mutation.Key == resolutionrecord.StorageKey {
			projection, err := resolutionrecord.DecodeHostResolutionProjectionRecord(mutation.Value)
			if err != nil || projection.InputRevision != read.ReadRevision {
				t.Fatalf("DNS projection not bound to terminal snapshot: %v", err)
			}
			return
		}
	}
	t.Fatal("Blueprint Component completion did not publish DNS projection")
}

// DNS-04: completion must reconcile newly introduced Routes before promotion,
// replace omitted predecessor Routes, and leave other Environments untouched.
func TestBlueprintCompletionUsesPendingAppliedRoutes(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	task := validTaskRecord(now)
	task.Type = taskjournal.TaskUpdate
	task.Target = ids.NewAt(ids.KindEnvironment, now, 2301)
	task.Owner.EnvironmentID = task.Target
	pinComponentTaskDesiredRevision(&task)
	task.Params[componentTaskBlueprintProcedureParam] = componentTaskBlueprintProcedureCandidateReleases
	task.Params[releaserender.TaskReleasePublicationParam] = ids.NewULID()
	delete(task.Params, taskjournal.TaskResourceKindParam)
	records := componentTaskLifecycleRecords(t, task.Target, now, true)
	seedComponentTaskLifecycle(t, store, task, records)
	newRoute := core.Route{
		ID: ids.NewAt(ids.KindRoute, now, 2302), Host: "new.example.test", Path: "/",
		TargetServiceID: records.candidate.Runtime.GeneratedServices[0], TargetPort: 8080, Exposure: "public",
	}
	projection := withTestEnvironmentComposeArtifact(projectionrecord.EnvironmentComposeProjection{
		EnvironmentID: task.Target, RevisionID: task.ID, RenderGeneration: 2,
		DesiredRoutes: []projectionrecord.EnvironmentRouteProjection{{
			EnvironmentID: task.Target, Desired: newRoute, DesiredGeneration: 1,
		}},
	})
	value, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatal(err)
	}
	change := taskMaterializationProjectionChange{applies: true, mutations: []etcdstore.Mutation{{
		Type: etcdstore.MutationPut,
		Key:  projectionrecord.EnvironmentComposeProjectionStorageKey(task.Target), Value: value,
	}}}
	previous := projection
	previous.RenderGeneration = 1
	previous.DesiredRoutes = append([]projectionrecord.EnvironmentRouteProjection(nil), projection.DesiredRoutes...)
	previous.DesiredRoutes[0].Desired.ID = ids.NewAt(ids.KindRoute, now, 2303)
	previous.DesiredRoutes[0].Desired.Host = "old.example.test"
	oldValue, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(previous)
	if err != nil {
		t.Fatal(err)
	}
	seedTaskRepositoryValue(t, store, projectionrecord.EnvironmentComposeProjectionStorageKey(task.Target), oldValue)
	siblingID := ids.NewAt(ids.KindEnvironment, now, 2304)
	sibling := withTestEnvironmentComposeArtifact(projectionrecord.EnvironmentComposeProjection{
		EnvironmentID: siblingID, RevisionID: task.ID, RenderGeneration: 1,
		DesiredRoutes: []projectionrecord.EnvironmentRouteProjection{{
			EnvironmentID: siblingID, Desired: previous.DesiredRoutes[0].Desired, DesiredGeneration: 1,
		}},
	})
	sibling.DesiredRoutes[0].Desired.ID = ids.NewAt(ids.KindRoute, now, 2305)
	siblingValue, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(sibling)
	if err != nil {
		t.Fatal(err)
	}
	seedTaskRepositoryValue(t, store, projectionrecord.EnvironmentComposeProjectionStorageKey(siblingID), siblingValue)
	key := environmentchanges.ComponentTaskIntentKey(task.ID)
	read, err := store.Get(ctx, key)
	if err != nil || read.Entry == nil {
		t.Fatalf("read Component intent: %v", err)
	}
	intent, err := environmentchanges.DecodeComponentTaskIntent(read.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	intent.RouteProjection = &environmentchanges.ComponentTaskRouteProjection{
		Routes: []environmentchanges.ComponentTaskRouteCandidate{{Desired: newRoute, DesiredGeneration: 1}},
	}
	encoded, err := environmentchanges.EncodeComponentTaskIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	seedTaskRepositoryValue(t, store, key, encoded)
	read, err = store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := hostResolutionTerminalProjection(task, taskjournal.TaskStatusCompleted, change)
	if err != nil || selected == nil {
		t.Fatalf("select pending applied projection: %v", err)
	}
	routes, err := repository.scanRoutesAtRevision(ctx, read.ReadRevision, selected)
	if err != nil || len(routes) != 2 {
		t.Fatalf("scan pending plus sibling Routes: count=%d error=%v", len(routes), err)
	}
	if _, err := hostResolutionRouteAtSnapshot(routes, previous.DesiredRoutes[0].Desired.ID); err == nil {
		t.Fatal("omitted predecessor Route remained in DNS input")
	}
	if _, err := hostResolutionRouteAtSnapshot(routes, newRoute.ID); err != nil {
		t.Fatal("new candidate Route missing from DNS input")
	}
	if _, err := hostResolutionRouteAtSnapshot(routes, sibling.DesiredRoutes[0].Desired.ID); err != nil {
		t.Fatal("other Environment Route removed from DNS input")
	}
	result, err := repository.prepareHostResolutionReconciliation(ctx, task, taskjournal.TaskStatusCompleted,
		read.ReadRevision, nil, platformComponentTaskChange{}, change)
	if err != nil {
		t.Fatalf("complete Blueprint introducing a Route: %v", err)
	}
	defer clearHostResolutionReconciliationChange(result)
}

// DNS-04, TASK-03: restoring a failed Blueprint must not require its newly
// introduced Routes to exist in the applied predecessor or publish them to DNS.
func TestBlueprintRestorationKeepsAppliedRoutes(t *testing.T) {
	for _, status := range []taskjournal.TaskStatus{
		taskjournal.TaskStatusFailed, taskjournal.TaskStatusAborted, taskjournal.TaskStatusTimedOut,
	} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryTaskStore()
			repository, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			now := taskJournalTime()
			task := validTaskRecord(now)
			task.Type = taskjournal.TaskUpdate
			task.Target = ids.NewAt(ids.KindEnvironment, now, 2201)
			pinComponentTaskDesiredRevision(&task)
			task.Params[componentTaskBlueprintProcedureParam] = componentTaskBlueprintProcedureCandidateReleases
			task.Params[releaserender.TaskReleasePublicationParam] = ids.NewULID()
			delete(task.Params, taskjournal.TaskResourceKindParam)
			records := componentTaskLifecycleRecords(t, task.Target, now, true)
			seedComponentTaskLifecycle(t, store, task, records)
			key := environmentchanges.ComponentTaskIntentKey(task.ID)
			read, err := store.Get(ctx, key)
			if err != nil || read.Entry == nil {
				t.Fatalf("read Component intent: %v", err)
			}
			intent, err := environmentchanges.DecodeComponentTaskIntent(read.Entry.Value)
			if err != nil {
				t.Fatal(err)
			}
			intent.RouteProjection = &environmentchanges.ComponentTaskRouteProjection{
				Routes: []environmentchanges.ComponentTaskRouteCandidate{{
					Desired: core.Route{
						ID: ids.NewAt(ids.KindRoute, now, 2202), Host: "new.example.test", Path: "/",
						TargetServiceID: records.candidate.Runtime.GeneratedServices[0],
						TargetPort:      8080, Exposure: "public",
					}, DesiredGeneration: 1,
				}},
			}
			encoded, err := environmentchanges.EncodeComponentTaskIntent(intent)
			if err != nil {
				t.Fatal(err)
			}
			seedTaskRepositoryValue(t, store, key, encoded)
			read, err = store.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			_, routes, components, err := repository.hostResolutionTerminalOverlay(
				ctx,
				task,
				status,
				read.ReadRevision,
				nil,
			)
			if err != nil {
				t.Fatalf("restored Blueprint DNS reconciliation: %v", err)
			}
			if len(routes) != 0 ||
				components[records.current.Desired.ID].Runtime.PinnedIPv4 != records.current.Runtime.PinnedIPv4 {
				t.Fatal("restoration projected candidate Routes or replaced the previous router address")
			}
		})
	}
}
