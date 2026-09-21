package etcd

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: specialized direct publishers advance the same desired head as
// Blueprint Apply and must respect removal ownership even at final commit.
func TestSpecializedDesiredPublishersExcludeVolumeRemovalLock(t *testing.T) {
	for _, family := range []string{"service", "zone"} {
		for _, mode := range []string{"unlocked", "held", "late"} {
			t.Run(family+"/"+mode, func(t *testing.T) {
				var fixture specializedRemovalLockFixture
				if family == "service" {
					fixture = specializedServiceRemovalLockFixture(t)
				} else {
					fixture = specializedZoneRemovalLockFixture(t)
				}
				owner := removalrecord.Owner{
					EnvironmentID: fixture.environmentID, VolumeID: ids.New(ids.KindVolume), OperationID: ids.New(ids.KindOperation),
				}
				backend := &specializedRemovalLockRaceStore{memoryHierarchyStore: fixture.store}
				if mode == "held" {
					putSpecializedRemovalLock(t, fixture.store, owner)
				} else if mode == "late" {
					backend.owner = &owner
				}
				repository, err := newHierarchyRepository(backend)
				if err != nil {
					t.Fatal(err)
				}
				before := fixture.store.revision
				result, err := fixture.publish(context.Background(), repository)
				if err != nil {
					t.Fatal(err)
				}
				outcome, _, conflict, err := result.Classify()
				if mode == "unlocked" {
					if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
						t.Fatalf("unlocked publication rejected: %v/%v/%v", outcome, conflict, err)
					}
					return
				}
				if err != nil || outcome != IdempotencyKnownConflict || !isKind(conflict, errs.KindStateConflict) {
					t.Fatalf("specialized publisher ignored removal lock: %v/%v/%v", outcome, conflict, err)
				}
				if mode == "late" {
					before++
				}
				if fixture.store.revision != before {
					t.Fatal("rejected publication wrote state")
				}
			})
		}
	}
}

type specializedRemovalLockFixture struct {
	store         *memoryHierarchyStore
	environmentID string
	publish       func(context.Context, *HierarchyRepository) (IdempotencyTransactionResult, error)
}

func specializedServiceRemovalLockFixture(t *testing.T) specializedRemovalLockFixture {
	t.Helper()
	ctx := context.Background()
	_, store, environment, project := serviceRepositoryTestHierarchy(t)
	environment = readyServiceMutationEnvironment(t, ctx, store, environment)
	fixture := seedDesiredServiceFixture(t, ctx, store, environment.Record.ID, core.Service{
		ID: ids.New(ids.KindService), Name: "removal-lock", Image: "app:latest", Strategy: core.StrategyRecreate,
	}, "", 941, false, false)
	record := fixture.Service.Record
	marker := directServiceMutationMarker(environment.Record.ID, record.Desired.ID, "removal-lock")
	claim := stageDirectServicePublicationForTest(t, ctx, store, fixture, marker, 0)
	input := EnvironmentServiceDesiredPublication{
		Project: project, Environment: environment, Claim: claim,
		Revision: testblueprints.EnvironmentDesiredRevisionIdentity{
			EnvironmentID: environment.Record.ID,
			RevisionID:    fixture.Projection.Record.RevisionID,
		},
		Projection: fixture.Projection.Record, Change: testblueprints.EnvironmentBlueprintServiceChange{Record: record}, Marker: marker,
	}
	return specializedRemovalLockFixture{store: store, environmentID: environment.Record.ID,
		publish: func(ctx context.Context, repository *HierarchyRepository) (IdempotencyTransactionResult, error) {
			return repository.PublishEnvironmentServiceDesiredRevisionDirect(ctx, input)
		}}
}

func specializedZoneRemovalLockFixture(t *testing.T) specializedRemovalLockFixture {
	t.Helper()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	baseTask := environmentBlueprintTestTask(t, project.Record, environment.Record, 14200)
	base := environmentBlueprintTestProjection(environment.Record.ID, baseTask, 1)
	seedServiceRepositoryTestDesiredProjection(t, store, base)
	current, found, err := repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("baseline: %v", err)
	}
	zone, err := testzones.NewRecord(environment.Record.ID, core.Zone{
		ID: ids.New(ids.KindNetwork), Name: "frontend", Subnet: "10.40.20.0/24", Internal: true,
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate := testenvironmentprojection.CloneEnvironmentComposeProjection(current.Record)
	candidate.RevisionID, candidate.RenderGeneration = ids.New(ids.KindTask), 2
	candidate.DesiredZones = append(
		candidate.DesiredZones,
		testenvironmentprojection.EnvironmentZoneProjection{
			EnvironmentID: environment.Record.ID,
			Desired:       zone.Desired,
		},
	)
	candidate = zonePublicationTestArtifact(t, candidate, zone)
	locator := testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment,
		ScopeID:   environment.Record.ID,
		Method:    http.MethodPost,
		Route:     "/zones",
		Key:       "zone-removal-lock-publication",
	}
	intent := validEnvironmentBlueprintProtectedIntentForTest("zone-create")
	at := testAttachTime
	claim := stageZoneDesiredPublicationTest(t, store, testblueprints.EnvironmentBlueprintStageClaim{
		DescriptorID: strings.TrimPrefix(candidate.RevisionID, "task_"), EnvironmentID: environment.Record.ID,
		RevisionID: candidate.RevisionID, TaskID: candidate.RevisionID, Locator: locator, Intent: intent,
		BaselineHeadRevision: current.Revision, SourceKind: testblueprints.EnvironmentBlueprintSourceMutation,
		RenderGeneration: 2, ProjectionSchema: testblueprints.EnvironmentDesiredProjectionSchema, CreatedAt: at,
	}, current.Record.RevisionID, candidate, zone)
	marker, err := testidempotency.NewCompletedDirectIdempotencyMarker(
		locator,
		intent,
		testidempotency.IdempotencyResponse{
			Status: http.StatusCreated, ContentKind: "application/json", Body: []byte(`{"id":"` + zone.Desired.ID + `"}`),
		},
		at,
	)
	if err != nil {
		t.Fatal(err)
	}
	input := EnvironmentZoneDesiredPublication{Project: project, Environment: environment,
		ExpectedHeadRevision: current.Revision, Claim: claim,
		Revision: testblueprints.EnvironmentDesiredRevisionIdentity{
			EnvironmentID: environment.Record.ID,
			RevisionID:    candidate.RevisionID,
		},
		Projection: candidate, Zone: zone, Marker: marker}
	return specializedRemovalLockFixture{store: store, environmentID: environment.Record.ID,
		publish: func(ctx context.Context, repository *HierarchyRepository) (IdempotencyTransactionResult, error) {
			return repository.PublishEnvironmentZoneDesiredRevisionDirect(ctx, input)
		}}
}

func putSpecializedRemovalLock(t *testing.T, store *memoryHierarchyStore, owner removalrecord.Owner) {
	t.Helper()
	value, err := removalrecord.EncodeOwner(owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut,
		Key: removalrecord.EnvironmentLockKey(owner.EnvironmentID), Value: value}}); err != nil {
		t.Fatal(err)
	}
}

type specializedRemovalLockRaceStore struct {
	*memoryHierarchyStore
	owner *removalrecord.Owner
}

func (store *specializedRemovalLockRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if store.owner != nil {
		owner := *store.owner
		value, err := removalrecord.EncodeOwner(owner)
		if err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut,
			Key: removalrecord.EnvironmentLockKey(owner.EnvironmentID), Value: value}}); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		store.owner = nil
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
