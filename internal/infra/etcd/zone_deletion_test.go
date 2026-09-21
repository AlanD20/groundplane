package etcd

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testenvironmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
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

// Rationale: every enabled Component membership is desired authority for a
// Zone, including secondary Caddy and non-address-reserving Tunnel networks.
func TestBeginZoneDeletionRejectsEnabledComponentMembership(t *testing.T) {
	for _, kind := range []core.ComponentKind{
		core.ComponentKindIngressCaddy,
		core.ComponentKindEdgeCloudflare,
	} {
		t.Run(string(kind), func(t *testing.T) {
			fixture := newZoneDeletionProjectionFixture(t, false)
			at := fixture.task.CreatedAt
			component := core.Component{
				ID: ids.NewAt(ids.KindComponent, at, 500), Owner: core.ComponentOwnerEnvironment,
				OwnerID: fixture.environment.Record.ID, Kind: kind, Enabled: true,
				GeneratedServices: []string{ids.NewAt(ids.KindService, at, 501)},
			}
			switch kind {
			case core.ComponentKindIngressCaddy:
				component.Config.Caddy = &core.CaddyComponentConfig{ZoneIDs: []string{
					ids.NewAt(ids.KindNetwork, at, 502), fixture.zone.Record.Desired.ID,
				}}
				component.PinnedIPv4 = "10.40.21.2"
			case core.ComponentKindEdgeCloudflare:
				component.Config.CloudflareTunnel = &core.CloudflareTunnelComponentConfig{
					ZoneIDs:  []string{fixture.zone.Record.Desired.ID},
					SecretID: ids.NewAt(ids.KindSecret, at, 503),
				}
			}
			record, err := testcomponents.NewRecord(component)
			if err != nil {
				t.Fatal(err)
			}
			fixture.authorities.Desired.Record.Components = append(
				fixture.authorities.Desired.Record.Components,
				record,
			)
			_, err = fixture.zones.BeginZoneDeletionWithTask(
				context.Background(), fixture.environment, fixture.project, fixture.zone, fixture.authorities,
				fixture.tombstone, fixture.intent, fixture.task, fixture.marker,
			)
			if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
				t.Fatalf("BeginZoneDeletionWithTask() error = %v, want resource_in_use", err)
			}
		})
	}
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
		context.Background(),
		testdeletions.TombstoneKey(string(testdeletions.DeletionTargetZone), fixture.zone.Record.Desired.ID),
	)
	if err != nil || tombstoneRead.Entry == nil {
		t.Fatalf("read backing Zone tombstone = %#v/%v", tombstoneRead, err)
	}
	tombstone, err := testdeletions.DecodeDeletionTombstone(tombstoneRead.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	storedIntent, found, err := fixture.hierarchy.GetZoneRemovalIntent(context.Background(), fixture.intent.OperationID)
	if err != nil || !found {
		t.Fatalf("GetZoneRemovalIntent() = %#v/%v/%v", storedIntent, found, err)
	}
	childAt := fixture.task.CreatedAt.Add(2 * time.Second)
	childID := ids.NewAt(ids.KindTask, childAt, 90)
	intent, err := testenvironmentchanges.TransferZoneRemovalIntent(storedIntent.Record, childID, childAt)
	if err != nil {
		t.Fatal(err)
	}
	child := zoneDeletionAgentTask(t, fixture.project.Record, fixture.environment.Record, intent, childAt)
	child.ID = childID
	child.OperationID = ids.NewAt(ids.KindOperation, childAt, 91)
	child.IdempotencyKey = "zone-handoff-key-0001"
	child.Actor = testtaskjournal.TaskActorSystem
	marker := pendingTaskMarker(child)
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: fixture.environment.Record.ID,
		Method: http.MethodDelete, Route: "/zones/{id}", Key: child.IdempotencyKey,
	}
	result, err = fixture.zones.HandoffBackingZoneDeletion(
		context.Background(),
		fixture.zone,
		fixture.task.ID,
		testkeyvalue.Versioned[testdeletions.DeletionTombstoneRecord]{
			Record: tombstone, Revision: tombstoneRead.Entry.ModRevision, ReadRevision: tombstoneRead.ReadRevision,
		},
		intent,
		child,
		marker,
	)
	assertZoneDeletionApplied(t, result, err)
	fixture.assertCurrentHead(t)
}

type zoneDeletionProjectionFixture struct {
	store       *memoryHierarchyStore
	hierarchy   *HierarchyRepository
	zones       *ZoneRepository
	project     testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	zone        testkeyvalue.Versioned[testzones.Record]
	authorities testenvironmentchanges.EnvironmentZoneRemovalAuthorities
	projection  testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]
	tombstone   testdeletions.DeletionTombstoneRecord
	intent      testenvironmentchanges.ZoneRemovalIntent
	task        TaskRecord
	marker      testidempotency.IdempotencyMarker
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
	record, err := testzones.NewRecord(environment.Record.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, baseTask.CreatedAt, 80), Name: "frontend", Subnet: "10.40.20.0/24",
		Internal: true, OwnerKind: ownerKind, OwnerID: ownerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	current := testenvironmentprojection.CloneEnvironmentComposeProjection(base)
	current.DesiredServices[0].Desired.Image = "example/api:2"
	current.DesiredZones = append(current.DesiredZones, testenvironmentprojection.EnvironmentZoneProjection{
		EnvironmentID: environment.Record.ID, Desired: record.Desired,
	})
	current = zonePublicationTestArtifact(t, current, record)
	seedServiceRepositoryTestDesiredProjection(t, store, current)
	projectionValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(base)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(projectionValue)
	poolValue, err := testrecordcodec.Encode(
		"zone_pool_registry",
		testnetworkreservations.ZonePoolRegistry{Reservations: map[string]string{
			record.Desired.ID: record.Desired.Subnet,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(poolValue)
	seed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environment.Record.ID),
			Value: projectionValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testnetworkreservations.ZonePoolRegistryKey(environment.Record.ID),
			Value: poolValue,
		},
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
	var target *testenvironmentprojection.EnvironmentZoneProjection
	for index := range selected.Record.DesiredZones {
		if selected.Record.DesiredZones[index].Desired.ID == record.Desired.ID {
			target = &selected.Record.DesiredZones[index]
			break
		}
	}
	if target == nil {
		t.Fatalf("selected projection is missing Zone %q", record.Desired.ID)
	}
	zone, err := testenvironmentqueries.JoinZone(selected, *target)
	if err != nil {
		t.Fatal(err)
	}
	at := baseTask.CreatedAt.Add(time.Minute)
	candidateID := ids.NewAt(ids.KindTask, at, 81)
	candidate := testenvironmentprojection.CloneEnvironmentComposeProjection(base)
	candidate.DesiredServices[0].Desired.Image = current.DesiredServices[0].Desired.Image
	candidate.RevisionID = candidateID
	candidate.RenderGeneration = selected.Record.RenderGeneration + 1
	locator := testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodDelete, Route: "/zones/{id}", Key: "zone-deletion-key-0001",
	}
	claim := stageZoneDesiredPublicationTest(t, store, testblueprints.EnvironmentBlueprintStageClaim{
		DescriptorID: strings.TrimPrefix(candidateID, "task_"), EnvironmentID: environment.Record.ID,
		RevisionID: candidateID, TaskID: candidateID, Locator: locator,
		Intent:               validEnvironmentBlueprintProtectedIntentForTest("zone-deletion"),
		BaselineHeadRevision: selected.Revision, SourceKind: testblueprints.EnvironmentBlueprintSourceMutation,
		RenderGeneration: candidate.RenderGeneration, ProjectionSchema: testblueprints.EnvironmentDesiredProjectionSchema,
		CreatedAt: at,
	}, selected.Record.RevisionID, candidate, record)
	task := zoneDeletionAgentTask(t, project.Record, environment.Record, testenvironmentchanges.ZoneRemovalIntent{
		OperationID: ids.NewAt(ids.KindOperation, at, 82), EnvironmentID: environment.Record.ID,
		ZoneID: record.Desired.ID, Claim: claim, CandidateProjection: candidate,
	}, at)
	task.ID = candidateID
	task.OperationID = ids.NewAt(ids.KindOperation, at, 82)
	task.IdempotencyKey = locator.Key
	if backing {
		task.Executor = testtaskjournal.TaskExecutorController
		task.Params = map[string]string{
			testtaskjournal.TaskResourceKindParam:          testtaskjournal.TaskResourceBackingZone,
			testtaskjournal.TaskZoneEnvironmentParam:       environment.Record.ID,
			testtaskjournal.TaskZoneImpactTokenParam:       strings.Repeat("b", 64),
			testtaskjournal.TaskZoneRemovalOperationParam:  task.OperationID,
			testblueprints.EnvironmentDesiredRevisionParam: claim.RevisionID,
		}
	}
	intent, err := testenvironmentchanges.NewZoneRemovalIntent(
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
	marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetZone,
		ID:   record.Desired.ID,
	}
	tombstone := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetZone, TargetID: record.Desired.ID, TargetRevision: zone.Revision,
		TaskID: task.ID, Phase: testdeletions.DeletionPhaseHostEffects, CreatedAt: at, UpdatedAt: at,
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
) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]) {
	t.Helper()
	if !backing {
		return createEnvironmentBlueprintOwners(t, hierarchy)
	}
	at := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	projectID := ids.NewAt(ids.KindProject, at, 200)
	project, err := hierarchy.CreateProject(context.Background(), testhierarchy.ProjectRecord{
		ID: projectID, Slug: "postgres", Name: "Postgres", Kind: testhierarchy.ProjectKindBacking,
	})
	if err != nil {
		t.Fatal(err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, at, 201)
	environmentRecord := testhierarchy.EnvironmentRecord{
		ID: environmentID, ProjectID: projectID, Name: "backing", NetworkPool: "10.40.0.0/16",
		VolumeDir:         "/var/lib/groundplane/vol/platform/" + projectID + "/" + environmentID,
		ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, at, 202), CreatedAt: at,
	}
	environmentValue, err := testhierarchy.EncodeEnvironment(environmentRecord)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(environmentValue)
	seed, err := hierarchy.store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentKey(environmentID), Value: environmentValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testhierarchy.EnvironmentNameKey(projectID, environmentRecord.Name),
			Value: []byte(environmentID),
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testhierarchy.EnvironmentOwnerKey(projectID, environmentID),
			Value: []byte(environmentID),
		},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed backing Environment = %#v/%v", seed, err)
	}
	environment := testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
		Record: environmentRecord, Revision: seed.Revision, ReadRevision: seed.Revision,
	}
	return project, environment
}

func zoneDeletionAgentTask(
	t *testing.T,
	project testhierarchy.ProjectRecord,
	environment testhierarchy.EnvironmentRecord,
	intent testenvironmentchanges.ZoneRemovalIntent,
	at time.Time,
) TaskRecord {
	t.Helper()
	owner, err := testtaskjournal.EnvironmentTaskOwner(project, environment)
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
		Owner: owner, Actor: testtaskjournal.TaskActorOperator, Executor: testtaskjournal.TaskExecutorAgent,
		PlanID: ids.NewAt(ids.KindPlan, at, 84), PlanHash: strings.Repeat("a", 64),
		RenderGeneration: int32(
			intent.CandidateProjection.RenderGeneration,
		), Type: testtaskjournal.TaskRemove, Target: intent.ZoneID,
		Params: map[string]string{
			testtaskjournal.TaskZoneEnvironmentParam:       environment.ID,
			testtaskjournal.TaskZoneRemovalOperationParam:  intent.OperationID,
			testblueprints.EnvironmentDesiredRevisionParam: intent.Claim.RevisionID,
			testtaskjournal.TaskComposeArtifactParam:       artifact.GetArtifactId(),
		},
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: ids.NewAt(ids.KindStep, at, 85)},
		}, TimeoutSeconds: 300,
		Status: testtaskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: at, UpdatedAt: at,
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
		context.Background(),
		testdeletions.TombstoneKey(string(testdeletions.DeletionTargetZone), fixture.zone.Record.Desired.ID),
	)
	if err != nil || read.Entry == nil {
		t.Fatalf("Zone deletion tombstone = %#v/%v", read, err)
	}
	tombstone, err := testdeletions.DecodeDeletionTombstone(read.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	if tombstone.TargetKind != testdeletions.DeletionTargetZone ||
		tombstone.TargetID != fixture.zone.Record.Desired.ID {
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
