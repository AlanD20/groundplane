package etcd

import (
	"context"
	"net/netip"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: publishing desired state must expose only the parent as the head
// and replay target while the independently queued Agent child retains the
// exact same authored revision and its own terminal marker.
func TestBlueprintPublicationSeparatesVisibleParentFromPrivateEffectTask(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	parent := environmentBlueprintTestTask(t, project.Record, environment.Record, 900)
	parent.Executor = taskjournal.TaskExecutorBlueprint
	parent.Steps = nil
	parent.Params = map[string]string{blueprints.EnvironmentDesiredRevisionParam: parent.ID}
	child := environmentBlueprintTestTask(t, project.Record, environment.Record, 910)
	child.Actor = taskjournal.TaskActorSystem
	child.Params[blueprints.EnvironmentDesiredRevisionParam] = parent.ID
	child.Params[taskjournal.TaskBlueprintParentParam] = parent.ID
	childMarker := environmentBlueprintTestMarker(child, environment.Record.ID)
	childMarker.Locator.Key = child.IdempotencyKey
	parentMarker := environmentBlueprintTestMarker(parent, environment.Record.ID)
	projection := environmentBlueprintTestProjection(environment.Record.ID, parent, 1)
	revision := environmentBlueprintTestRevision(environment.Record.ID, parent, "services: {}\n")
	claim := stageEnvironmentBlueprintForPublicationTest(t, repository, 0, revision, projection, parentMarker)
	zones := environmentBlueprintTestZoneChanges(t, repository, projection)
	services := environmentBlueprintTestServiceChanges(t, repository, projection)
	routes := environmentBlueprintTestRouteChanges(t, repository, projection)
	result, err := repository.publishEnvironmentDesiredRevisionWithTask(
		ctx, netip.Prefix{}, environment.Record.NetworkPool,
		project, environment, 0, claim,
		blueprints.EnvironmentDesiredRevisionIdentity{EnvironmentID: environment.Record.ID, RevisionID: parent.ID},
		projection, zones, services, routes,
		releasegroups.ReleaseGroupBlueprintPreparedMutation{}, componentplanning.ComponentTaskPreparation{},
		blueprintplanning.BlueprintAttachTaskPreparation{}, blueprintplanning.BlueprintBackupPolicyPreparation{},
		BlueprintScriptPublication{}, BlueprintReleasePublication{}, BlueprintRequirementGate{},
		VolumeRemovalBackupPolicyPreparation{}, nil, child, parentMarker,
		environmentBlueprintTestTransactionStore{hierarchyStore: store},
		&blueprintTaskPair{parent: parent, childMarker: childMarker},
	)
	if err != nil {
		t.Fatalf("publish parent and child = %v", err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("publication = %v/%v/%v", outcome, conflict, err)
	}
	head, found, err := repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil || !found || head.Record.RevisionID != parent.ID {
		t.Fatalf("public desired head = %#v/%t, %v", head, found, err)
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
		taskjournal.TaskStorageKey(parent.ID), taskjournal.TaskQueueKey(parent.Executor, parent.ID),
		taskjournal.TaskStorageKey(child.ID), taskjournal.TaskQueueKey(child.Executor, child.ID),
	}})
	if err != nil || read == nil || len(read.Values) != 4 {
		t.Fatalf("parent/child publication = %#v, %v", read, err)
	}
	for index, value := range read.Values {
		if value == nil {
			t.Fatalf("publication record %d is absent", index)
		}
	}
	parentRef, err := idempotency.DecodeTaskReference(read.Values[1].Value)
	if err != nil || parentRef != parent.ID {
		t.Fatalf("parent queue = %s, %v", parentRef, err)
	}
	childRef, err := idempotency.DecodeTaskReference(read.Values[3].Value)
	if err != nil || childRef != child.ID {
		t.Fatalf("private child queue = %s, %v", childRef, err)
	}
}
