package etcd

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestBeginZoneDeletionFencesDistinctDesiredAndAppliedProjections(t *testing.T) {
	fixture := newZoneDeletionProjectionFixture(t, false)
	result, err := fixture.zones.BeginZoneDeletionWithTask(
		context.Background(), fixture.environment, fixture.project, fixture.zone, fixture.authorities,
		fixture.tombstone, fixture.intent, fixture.task, fixture.marker,
	)
	assertZoneDeletionApplied(t, result, err)
	fixture.assertZoneDeletionTombstone(t)
	fixture.assertCurrentHead(t)
}

func TestBackingZoneHandoffUsesSelectedProjection(t *testing.T) {
	fixture := newZoneDeletionProjectionFixture(t, true)
	result, err := fixture.zones.BeginZoneDeletionWithTask(
		context.Background(), fixture.environment, fixture.project, fixture.zone, fixture.authorities,
		fixture.tombstone, fixture.intent, fixture.task, fixture.marker,
	)
	assertZoneDeletionApplied(t, result, err)
	tasks, err := newTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	claimed, found, err := tasks.ClaimNextControllerTask(context.Background(), fixture.task.CreatedAt.Add(time.Second))
	if err != nil || !found || claimed.Task.Record.ID != fixture.task.ID {
		t.Fatalf("ClaimNextControllerTask() = %#v/%v/%v", claimed, found, err)
	}
	tombstoneRead, err := fixture.store.Get(
		context.Background(), deletionTombstoneKey(string(DeletionTargetZone), fixture.zone.Record.Desired.ID),
	)
	if err != nil || tombstoneRead.Entry == nil {
		t.Fatalf("read backing Zone tombstone = %#v/%v", tombstoneRead, err)
	}
	tombstone, err := decodeDeletionTombstone(tombstoneRead.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	storedIntent, found, err := fixture.hierarchy.GetZoneRemovalIntent(context.Background(), fixture.intent.OperationID)
	if err != nil || !found {
		t.Fatalf("GetZoneRemovalIntent() = %#v/%v/%v", storedIntent, found, err)
	}
	childAt := fixture.task.CreatedAt.Add(2 * time.Second)
	childID := ids.NewAt(ids.KindTask, childAt, 90)
	intent, err := TransferZoneRemovalIntent(storedIntent.Record, childID, childAt)
	if err != nil {
		t.Fatal(err)
	}
	child := zoneDeletionAgentTask(t, fixture.project.Record, fixture.environment.Record, intent, childAt)
	child.ID = childID
	child.OperationID = ids.NewAt(ids.KindOperation, childAt, 91)
	child.IdempotencyKey = "zone-handoff-key-0001"
	child.Actor = TaskActorSystem
	marker := pendingTaskMarker(child)
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: fixture.environment.Record.ID,
		Method: http.MethodDelete, Route: "/zones/{id}", Key: child.IdempotencyKey,
	}
	result, err = fixture.zones.HandoffBackingZoneDeletion(
		context.Background(), fixture.zone, fixture.task.ID,
		Versioned[DeletionTombstoneRecord]{
			Record: tombstone, Revision: tombstoneRead.Entry.ModRevision, ReadRevision: tombstoneRead.ReadRevision,
		},
		intent, child, marker,
	)
	assertZoneDeletionApplied(t, result, err)
	fixture.assertCurrentHead(t)
}

type zoneDeletionProjectionFixture struct {
	store       *memoryHierarchyStore
	hierarchy   *HierarchyRepository
	zones       *ZoneRepository
	project     Versioned[ProjectRecord]
	environment Versioned[EnvironmentRecord]
	zone        Versioned[ZoneRecord]
	authorities EnvironmentZoneRemovalAuthorities
	projection  Versioned[EnvironmentComposeProjection]
	tombstone   DeletionTombstoneRecord
	intent      ZoneRemovalIntent
	task        TaskRecord
	marker      IdempotencyMarker
}

func newZoneDeletionProjectionFixture(t *testing.T, backing bool) *zoneDeletionProjectionFixture {
	t.Helper()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := zoneDeletionOwners(t, hierarchy, backing)
	baseTask := environmentBlueprintTestTask(t, project.Record, environment.Record, 15000)
	base := environmentBlueprintTestProjection(environment.Record.ID, baseTask, 1)
	ownerKind := core.ZoneOwnerEnvironment
	ownerID := environment.Record.ID
	if backing {
		ownerKind = core.ZoneOwnerBackingProject
		ownerID = project.Record.ID
	}
	record, err := NewZoneRecord(environment.Record.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, baseTask.CreatedAt, 80), Name: "frontend", Subnet: "10.40.20.0/24",
		Internal: true, OwnerKind: ownerKind, OwnerID: ownerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	current := cloneEnvironmentComposeProjection(base)
	current.DesiredServices[0].Desired.Image = "example/api:2"
	current.DesiredZones = append(current.DesiredZones, EnvironmentZoneProjection{
		EnvironmentID: environment.Record.ID, Desired: record.Desired,
	})
	current = zonePublicationTestArtifact(t, current, record)
	seedServiceRepositoryTestDesiredProjection(t, store, current)
	projectionValue, err := encodeEnvironmentComposeProjection(base)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(projectionValue)
	poolValue, err := encodeEnvelope("zone_pool_registry", zonePoolRegistry{Reservations: map[string]string{
		record.Desired.ID: record.Desired.Subnet,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(poolValue)
	seed, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: environmentComposeProjectionKey(environment.Record.ID), Value: projectionValue},
		{Type: MutationPut, Key: zonePoolRegistryKey(environment.Record.ID), Value: poolValue},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed selected Zone projection = %#v/%v", seed, err)
	}
	authorities, found, err := hierarchy.GetEnvironmentZoneRemovalAuthorities(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("GetEnvironmentZoneRemovalAuthorities() = %#v/%v/%v", authorities, found, err)
	}
	selected := authorities.Desired
	if authorities.Desired.Revision == authorities.Applied.Revision ||
		projectionContainsZone(authorities.Applied.Record, record.Desired.ID) ||
		authorities.Desired.Record.DesiredServices[0].Desired.Image != "example/api:2" ||
		authorities.Applied.Record.DesiredServices[0].Desired.Image != "example/api:1" {
		t.Fatalf("Zone removal authorities = %#v", authorities)
	}
	var target *EnvironmentZoneProjection
	for index := range selected.Record.DesiredZones {
		if selected.Record.DesiredZones[index].Desired.ID == record.Desired.ID {
			target = &selected.Record.DesiredZones[index]
			break
		}
	}
	if target == nil {
		t.Fatalf("selected projection is missing Zone %q", record.Desired.ID)
	}
	zone, err := joinEnvironmentZone(selected, *target)
	if err != nil {
		t.Fatal(err)
	}
	at := baseTask.CreatedAt.Add(time.Minute)
	candidateID := ids.NewAt(ids.KindTask, at, 81)
	candidate := cloneEnvironmentComposeProjection(base)
	candidate.DesiredServices[0].Desired.Image = current.DesiredServices[0].Desired.Image
	candidate.RevisionID = candidateID
	candidate.RenderGeneration = selected.Record.RenderGeneration + 1
	locator := IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodDelete, Route: "/zones/{id}", Key: "zone-deletion-key-0001",
	}
	claim := stageZoneDesiredPublicationTest(t, store, EnvironmentBlueprintStageClaim{
		DescriptorID: strings.TrimPrefix(candidateID, "task_"), EnvironmentID: environment.Record.ID,
		RevisionID: candidateID, TaskID: candidateID, Locator: locator,
		Intent:               validEnvironmentBlueprintProtectedIntentForTest("zone-deletion"),
		BaselineHeadRevision: selected.Revision, SourceKind: EnvironmentBlueprintSourceMutation,
		RenderGeneration: candidate.RenderGeneration, ProjectionSchema: EnvironmentDesiredProjectionSchema,
		CreatedAt: at,
	}, selected.Record.RevisionID, candidate, record)
	task := zoneDeletionAgentTask(t, project.Record, environment.Record, ZoneRemovalIntent{
		OperationID: ids.NewAt(ids.KindOperation, at, 82), EnvironmentID: environment.Record.ID,
		ZoneID: record.Desired.ID, Claim: claim, CandidateProjection: candidate,
	}, at)
	task.ID = candidateID
	task.OperationID = ids.NewAt(ids.KindOperation, at, 82)
	task.IdempotencyKey = locator.Key
	if backing {
		task.Executor = TaskExecutorController
		task.Params = map[string]string{
			TaskResourceKindParam: TaskResourceBackingZone, TaskZoneEnvironmentParam: environment.Record.ID,
			TaskZoneImpactTokenParam: strings.Repeat("b", 64), TaskZoneRemovalOperationParam: task.OperationID,
			EnvironmentDesiredRevisionParam: claim.RevisionID,
		}
	}
	intent, err := NewZoneRemovalIntent(
		task.OperationID, task.ID, zone, authorities, claim, candidate, nil, at,
	)
	if err != nil {
		t.Fatal(err)
	}
	if intent.CandidateProjection.DesiredServices[0].Desired.Image != "example/api:2" {
		t.Fatalf("Zone removal candidate lost unapplied Service edit: %#v", intent.CandidateProjection.DesiredServices)
	}
	marker := pendingTaskMarker(task)
	marker.Locator = locator
	marker.Intent = claim.Intent
	marker.ReplayTarget = &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetZone, ID: record.Desired.ID}
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetZone, TargetID: record.Desired.ID, TargetRevision: zone.Revision,
		TaskID: task.ID, Phase: DeletionPhaseHostEffects, CreatedAt: at, UpdatedAt: at,
	}
	zones, err := newZoneRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	return &zoneDeletionProjectionFixture{
		store: store, hierarchy: hierarchy, zones: zones, project: project, environment: environment,
		zone: zone, authorities: authorities, projection: selected,
		tombstone: tombstone, intent: intent, task: task, marker: marker,
	}
}

func zoneDeletionOwners(
	t *testing.T,
	hierarchy *HierarchyRepository,
	backing bool,
) (Versioned[ProjectRecord], Versioned[EnvironmentRecord]) {
	t.Helper()
	if !backing {
		return createEnvironmentBlueprintOwners(t, hierarchy)
	}
	at := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	projectID := ids.NewAt(ids.KindProject, at, 200)
	project, err := hierarchy.CreateProject(context.Background(), ProjectRecord{
		ID: projectID, Slug: "postgres", Name: "Postgres", Kind: ProjectKindBacking,
	})
	if err != nil {
		t.Fatal(err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, at, 201)
	environmentRecord := EnvironmentRecord{
		ID: environmentID, ProjectID: projectID, Name: "backing", NetworkPool: "10.40.0.0/16",
		VolumeDir:         "/var/lib/groundplane/vol/platform/" + projectID + "/" + environmentID,
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, at, 202), CreatedAt: at,
	}
	environmentValue, err := encodeEnvironment(environmentRecord)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(environmentValue)
	seed, err := hierarchy.store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: environmentKey(environmentID), Value: environmentValue},
		{Type: MutationPut, Key: environmentNameKey(projectID, environmentRecord.Name), Value: []byte(environmentID)},
		{Type: MutationPut, Key: environmentOwnerKey(projectID, environmentID), Value: []byte(environmentID)},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed backing Environment = %#v/%v", seed, err)
	}
	environment := Versioned[EnvironmentRecord]{
		Record: environmentRecord, Revision: seed.Revision, ReadRevision: seed.Revision,
	}
	return project, environment
}

func zoneDeletionAgentTask(
	t *testing.T,
	project ProjectRecord,
	environment EnvironmentRecord,
	intent ZoneRemovalIntent,
	at time.Time,
) TaskRecord {
	t.Helper()
	owner, err := EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatal(err)
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		intent.CandidateProjection.ComposeArtifact, artifact,
	); err != nil {
		t.Fatal(err)
	}
	return TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 83), OperationID: intent.OperationID,
		Owner: owner, Actor: TaskActorOperator, Executor: TaskExecutorAgent,
		PlanID: ids.NewAt(ids.KindPlan, at, 84), PlanHash: strings.Repeat("a", 64),
		RenderGeneration: int32(intent.CandidateProjection.RenderGeneration), Type: TaskRemove, Target: intent.ZoneID,
		Params: map[string]string{
			TaskZoneEnvironmentParam: environment.ID, TaskZoneRemovalOperationParam: intent.OperationID,
			EnvironmentDesiredRevisionParam: intent.Claim.RevisionID, TaskComposeArtifactParam: artifact.GetArtifactId(),
		},
		Steps: []TaskStepRecord{{Kind: TaskStepOperation, ID: ids.NewAt(ids.KindStep, at, 85)}}, TimeoutSeconds: 300,
		Status: TaskStatusPending, NextEventSequence: 1, CreatedAt: at, UpdatedAt: at,
	}
}

func assertZoneDeletionApplied(t *testing.T, result IdempotencyTransactionResult, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("Zone deletion result = %v/%v/%v", outcome, conflict, classifyErr)
	}
}

func (fixture *zoneDeletionProjectionFixture) assertZoneDeletionTombstone(t *testing.T) {
	t.Helper()
	read, err := fixture.store.Get(
		context.Background(), deletionTombstoneKey(string(DeletionTargetZone), fixture.zone.Record.Desired.ID),
	)
	if err != nil || read.Entry == nil {
		t.Fatalf("Zone deletion tombstone = %#v/%v", read, err)
	}
	tombstone, err := decodeDeletionTombstone(read.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	if tombstone.TargetKind != DeletionTargetZone || tombstone.TargetID != fixture.zone.Record.Desired.ID {
		t.Fatalf("Zone deletion tombstone = %#v", tombstone)
	}
}

func (fixture *zoneDeletionProjectionFixture) assertCurrentHead(t *testing.T) {
	t.Helper()
	current, found, err := fixture.hierarchy.GetEnvironmentComposeProjection(
		context.Background(), fixture.environment.Record.ID,
	)
	if err != nil || !found || current.Record.RevisionID != fixture.projection.Record.RevisionID {
		t.Fatalf("current desired head = %#v/%v/%v", current, found, err)
	}
}
