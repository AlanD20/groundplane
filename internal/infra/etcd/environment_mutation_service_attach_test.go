package etcd

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: direct desired publication must read one fixed Environment
// snapshot, publish the sealed head and runtime sidecar, and advance its epoch.
func TestServiceMutationUsesFixedRevisionAndAdvancesEpoch(t *testing.T) {
	t.Parallel()
	_, store, environment, project := serviceRepositoryTestHierarchy(t)
	environment = readyServiceMutationEnvironment(t, context.Background(), store, environment)
	fixture := seedDesiredServiceFixture(t, context.Background(), store, environment.Record.ID, core.Service{
		ID: ids.NewAt(ids.KindService, testAttachTime, 910), Name: "fixed-revision", Image: "app:latest",
		Strategy: core.StrategyRecreate,
	}, "", 911, false, false)
	record := fixture.Service.Record
	marker := directServiceMutationMarker(environment.Record.ID, record.Desired.ID, "fixed-revision")
	claim := stageDirectServicePublicationForTest(t, context.Background(), store, fixture, marker, 0)
	audited := &ordinaryServiceMutationAuditStore{memoryHierarchyStore: store}
	repository, err := newHierarchyRepository(audited)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	result, err := repository.PublishEnvironmentServiceDesiredRevisionDirect(
		context.Background(),
		EnvironmentServiceDesiredPublication{
			Project: project, Environment: environment, ExpectedHeadRevision: 0,
			Claim: claim, Revision: EnvironmentDesiredRevisionIdentity{
				EnvironmentID: environment.Record.ID, RevisionID: fixture.Projection.Record.RevisionID,
			}, Projection: fixture.Projection.Record,
			Change: EnvironmentBlueprintServiceChange{Record: record}, Marker: marker,
		},
	)
	if err != nil {
		t.Fatalf("PublishEnvironmentServiceDesiredRevisionDirect() error = %v", err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil ||
		conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("direct Service publication = %v/%v/%v", outcome, conflict, classifyErr)
	}
	if len(audited.revisions) == 0 {
		t.Fatalf("fixed-revision GetMany calls = %v", audited.revisions)
	}
	for _, revision := range audited.revisions {
		if revision != audited.revisions[0] || revision <= 0 {
			t.Fatalf("fixed-revision GetMany calls = %v", audited.revisions)
		}
	}
	head, found, err := repository.GetEnvironmentBlueprintHead(context.Background(), environment.Record.ID)
	if err != nil || !found || head.Record.RevisionID != fixture.Projection.Record.RevisionID {
		t.Fatalf("GetEnvironmentBlueprintHead() = %#v/%v/%v", head, found, err)
	}
	epoch := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID)
	if epoch != result.revision {
		t.Fatalf("mutation epoch revision = %d, want publication revision %d", epoch, result.revision)
	}
	services, err := newServiceRepository(store)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	resolved, err := services.GetService(context.Background(), record.Desired.ID)
	if err != nil || !equalDirectServiceDesired(resolved.Record.Desired, record.Desired) ||
		resolved.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		t.Fatalf("GetService() = %#v, %v", resolved, err)
	}
}

// Rationale: a Service mutation references the selected Environment Zone
// projection without requiring a separate Zone record, while the Zone deletion
// tombstone still fences a concurrent removal.
func TestServiceMutationSelectedHeadZoneUsesTombstoneFence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, store, environment, project := serviceRepositoryTestHierarchy(t)
	environment = readyServiceMutationEnvironment(t, ctx, store, environment)
	zoneID := ids.NewAt(ids.KindNetwork, testAttachTime, 950)
	zone, err := NewZoneRecord(environment.Record.ID, core.Zone{
		ID: zoneID, Name: "frontend", Subnet: "10.40.10.0/29", Internal: true,
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord() error = %v", err)
	}
	desired := core.Service{
		ID: ids.NewAt(ids.KindService, testAttachTime, 951), Name: "zone-fenced", Image: "app:latest",
		Strategy: core.StrategyRecreate, Zones: []string{zone.Desired.Name},
	}
	fixture := seedDesiredServiceFixture(t, ctx, store, environment.Record.ID, desired, "", 952, false, false)
	fixture.Projection.Record.DesiredZones = []EnvironmentZoneProjection{{
		EnvironmentID: environment.Record.ID, Desired: zone.Desired,
	}}
	selectedZone, err := joinEnvironmentZone(fixture.Projection, fixture.Projection.Record.DesiredZones[0])
	if err != nil {
		t.Fatalf("joinEnvironmentZone() error = %v", err)
	}
	record := fixture.Service.Record
	marker := directServiceMutationMarker(environment.Record.ID, record.Desired.ID, "selected-head-zone")
	claim := stageDirectServicePublicationForTest(t, ctx, store, fixture, marker, 0)
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	result, err := repository.PublishEnvironmentServiceDesiredRevisionDirect(ctx, EnvironmentServiceDesiredPublication{
		Project: project, Environment: environment, ExpectedHeadRevision: 0,
		Claim: claim, Revision: EnvironmentDesiredRevisionIdentity{
			EnvironmentID: environment.Record.ID, RevisionID: fixture.Projection.Record.RevisionID,
		}, Projection: fixture.Projection.Record,
		Change:     EnvironmentBlueprintServiceChange{Record: record},
		References: ServiceMutationReferences{Zones: []Versioned[ZoneRecord]{selectedZone}}, Marker: marker,
	})
	if err != nil {
		t.Fatalf("PublishEnvironmentServiceDesiredRevisionDirect(selected-head Zone) error = %v", err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil ||
		conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("selected-head Zone Service publication = %v/%v/%v", outcome, conflict, classifyErr)
	}
	published, found, err := repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found || len(published.Record.DesiredZones) != 1 ||
		published.Record.DesiredZones[0].Desired.ID != zoneID {
		t.Fatalf("published selected-head Zone projection = %#v/%v/%v", published, found, err)
	}

	services, err := newServiceRepository(store)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	current, err := services.GetService(ctx, record.Desired.ID)
	if err != nil {
		t.Fatalf("GetService() error = %v", err)
	}
	replacementDesired := current.Record.Desired
	replacementDesired.Image = "app:v2"
	replacement, err := ReplaceServiceDesired(current.Record, replacementDesired)
	if err != nil {
		t.Fatalf("ReplaceServiceDesired() error = %v", err)
	}
	secondFixture := seedDesiredServiceFixture(
		t,
		ctx,
		store,
		environment.Record.ID,
		replacementDesired,
		"",
		953,
		false,
		false,
	)
	secondFixture.Projection.Record.RenderGeneration = published.Record.RenderGeneration + 1
	secondFixture.Claim.RenderGeneration = secondFixture.Projection.Record.RenderGeneration
	secondFixture.Projection.Record.DesiredZones = append(
		[]EnvironmentZoneProjection(nil),
		fixture.Projection.Record.DesiredZones...)
	secondMarker := directServiceMutationMarker(
		environment.Record.ID,
		record.Desired.ID,
		"selected-head-zone-tombstone",
	)
	secondClaim := stageDirectServicePublicationForTest(t, ctx, store, secondFixture, secondMarker, published.Revision)
	tombstoneValue := []byte("zone-removal-in-progress")
	defer clear(tombstoneValue)
	tombstoneResult, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: deletionTombstoneKey("zone", zoneID), Value: tombstoneValue,
	}})
	if err != nil || !tombstoneResult.Succeeded {
		t.Fatalf("seed Zone deletion tombstone = %#v/%v", tombstoneResult, err)
	}
	secondResult, err := repository.PublishEnvironmentServiceDesiredRevisionDirect(
		ctx,
		EnvironmentServiceDesiredPublication{
			Project: project, Environment: environment, ExpectedHeadRevision: published.Revision,
			Claim: secondClaim, Revision: EnvironmentDesiredRevisionIdentity{
				EnvironmentID: environment.Record.ID, RevisionID: secondFixture.Projection.Record.RevisionID,
			}, Projection: secondFixture.Projection.Record,
			Change:     EnvironmentBlueprintServiceChange{Current: &current, Record: replacement},
			References: ServiceMutationReferences{Zones: []Versioned[ZoneRecord]{selectedZone}}, Marker: secondMarker,
		},
	)
	if err != nil {
		t.Fatalf("PublishEnvironmentServiceDesiredRevisionDirect(tombstoned Zone) error = %v", err)
	}
	if outcome, _, conflict, classifyErr := secondResult.Classify(); classifyErr != nil ||
		outcome != IdempotencyKnownConflict || !isKind(conflict, errs.KindResourceInUse) {
		t.Fatalf("tombstoned Zone Service publication = %v/%v/%v", outcome, conflict, classifyErr)
	}
	head, found, err := repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil || !found || head.Record.RevisionID != published.Record.RevisionID {
		t.Fatalf("head after tombstoned Zone conflict = %#v/%v/%v", head, found, err)
	}
}

// Rationale: an epoch race must fail direct desired publication without
// publishing its head, runtime sidecar, or idempotency evidence.
func TestServiceMutationEpochRaceAndHeldLockPerformNoDomainWrite(t *testing.T) {
	t.Parallel()
	_, store, environment, project := serviceRepositoryTestHierarchy(t)
	environment = readyServiceMutationEnvironment(t, context.Background(), store, environment)
	fixture := seedDesiredServiceFixture(t, context.Background(), store, environment.Record.ID, core.Service{
		ID: ids.NewAt(ids.KindService, testAttachTime, 920), Name: "epoch-race", Image: "app:latest",
		Strategy: core.StrategyRecreate,
	}, "", 921, false, false)
	record := fixture.Service.Record
	marker := directServiceMutationMarker(environment.Record.ID, record.Desired.ID, "epoch-race")
	claim := stageDirectServicePublicationForTest(t, context.Background(), store, fixture, marker, 0)
	epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
		EnvironmentID: environment.Record.ID,
	})
	if err != nil {
		t.Fatalf("encodeEnvironmentMutationEpochRecord() error = %v", err)
	}
	defer clear(epochValue)
	tracing := &ordinaryServiceMutationAuditStore{
		memoryHierarchyStore: store,
		raceEnvironmentID:    environment.Record.ID,
		raceEpochValue:       epochValue,
	}
	repository, err := newHierarchyRepository(tracing)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	result, err := repository.PublishEnvironmentServiceDesiredRevisionDirect(
		context.Background(),
		EnvironmentServiceDesiredPublication{
			Project: project, Environment: environment, ExpectedHeadRevision: 0,
			Claim: claim, Revision: EnvironmentDesiredRevisionIdentity{
				EnvironmentID: environment.Record.ID, RevisionID: fixture.Projection.Record.RevisionID,
			}, Projection: fixture.Projection.Record,
			Change: EnvironmentBlueprintServiceChange{Record: record}, Marker: marker,
		},
	)
	if err != nil {
		t.Fatalf("PublishEnvironmentServiceDesiredRevisionDirect(epoch race) error = %v", err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil ||
		outcome != IdempotencyKnownConflict || !isKind(conflict, errs.KindStateConflict) {
		t.Fatalf("direct Service epoch race = %v/%v/%v", outcome, conflict, classifyErr)
	}
	for _, key := range []string{
		environmentBlueprintHeadKey(environment.Record.ID), serviceRuntimeKey(record.Desired.ID),
	} {
		stored, getErr := store.Get(context.Background(), key)
		if getErr != nil || stored.Entry != nil {
			t.Fatalf("failed direct Service publication key %q = %#v, %v", key, stored, getErr)
		}
	}

	// An already-held lock must reject a valid replacement before it needs any
	// new staging evidence or can advance the existing head.
	baseRepository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository(base) error = %v", err)
	}
	seeded := seedDesiredServiceFixture(t, context.Background(), store, environment.Record.ID,
		record.Desired, "", 922, false, true)
	current, err := newServiceRepository(store)
	if err != nil {
		t.Fatalf("newServiceRepository(current) error = %v", err)
	}
	joined, err := current.GetService(context.Background(), record.Desired.ID)
	if err != nil {
		t.Fatalf("GetService(current) error = %v", err)
	}
	projection, found, err := baseRepository.GetEnvironmentComposeProjection(
		context.Background(),
		environment.Record.ID,
	)
	if err != nil || !found || projection.Record.RevisionID != seeded.Projection.Record.RevisionID {
		t.Fatalf("GetEnvironmentComposeProjection() = %#v/%v/%v", projection, found, err)
	}
	lockedMarker := directServiceMutationMarker(environment.Record.ID, record.Desired.ID, "held-lock")
	lockedClaim := joinedServiceMutationClaim(joined.Record, projection.Record, lockedMarker)
	putEnvironmentMutationFenceTestLock(
		t, store, environment.Record.ID,
		environmentMutationFenceTestOwner(serviceRecordTestTime(), 923),
	)
	storeRevisionBefore := store.revision
	_, err = baseRepository.PublishEnvironmentServiceDesiredRevisionDirect(
		context.Background(),
		EnvironmentServiceDesiredPublication{
			Project: project, Environment: environment, ExpectedHeadRevision: projection.Revision,
			Claim: lockedClaim, Revision: EnvironmentDesiredRevisionIdentity{
				EnvironmentID: environment.Record.ID, RevisionID: lockedClaim.RevisionID,
			}, Projection: projection.Record,
			Change: EnvironmentBlueprintServiceChange{Current: &joined, Record: joined.Record}, Marker: lockedMarker,
		},
	)
	if !isKind(err, errs.KindResourceInUse) || store.revision != storeRevisionBefore {
		t.Fatalf("PublishEnvironmentServiceDesiredRevisionDirect(held lock) error = %v", err)
	}
}

// Rationale: desired Service projections are scoped by stable Environment and
// Service ids, so a record from another hierarchy cannot be accepted as a replacement.
func TestServiceMutationRejectsCrossTenantHierarchySpoof(t *testing.T) {
	t.Parallel()
	_, store, environment, _ := serviceRepositoryTestHierarchy(t)
	environment = readyServiceMutationEnvironment(t, context.Background(), store, environment)
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant, err := hierarchy.CreateTenant(context.Background(), TenantRecord{
		ID: ids.NewAt(ids.KindTenant, serviceRecordTestTime(), 930), Slug: "other", Name: "Other",
	})
	if err != nil {
		t.Fatalf("CreateTenant(other) error = %v", err)
	}
	otherProject, err := hierarchy.CreateProject(context.Background(), ProjectRecord{
		ID: ids.NewAt(ids.KindProject, serviceRecordTestTime(), 931), TenantID: tenant.Record.ID,
		Slug: "other", Name: "Other", Kind: ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject(other) error = %v", err)
	}
	forgedEnvironment := environment
	forgedEnvironment.Record.ProjectID = otherProject.Record.ID
	forgedEnvironment.Record.VolumeDir = "/var/lib/groundplane/vol/" + tenant.Record.ID + "/" +
		otherProject.Record.ID + "/" + environment.Record.ID
	forgedEnvironment.ReadRevision = store.revision
	fixture := seedDesiredServiceFixture(t, context.Background(), store, environment.Record.ID, core.Service{
		ID: ids.NewAt(ids.KindService, testAttachTime, 932), Name: "cross-tenant", Image: "app:latest",
		Strategy: core.StrategyRecreate,
	}, "", 933, false, false)
	record := fixture.Service.Record
	marker := directServiceMutationMarker(environment.Record.ID, record.Desired.ID, "cross-tenant")
	claim := stageDirectServicePublicationForTest(t, context.Background(), store, fixture, marker, 0)
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	result, err := repository.PublishEnvironmentServiceDesiredRevisionDirect(
		context.Background(),
		EnvironmentServiceDesiredPublication{
			Project: otherProject, Environment: forgedEnvironment, ExpectedHeadRevision: 0,
			Claim: claim, Revision: EnvironmentDesiredRevisionIdentity{
				EnvironmentID: environment.Record.ID, RevisionID: fixture.Projection.Record.RevisionID,
			}, Projection: fixture.Projection.Record,
			Change: EnvironmentBlueprintServiceChange{Record: record}, Marker: marker,
		},
	)
	if err == nil {
		outcome, _, conflict, classifyErr := result.Classify()
		if classifyErr != nil || outcome != IdempotencyKnownConflict || !isKind(conflict, errs.KindStateConflict) {
			t.Fatalf("cross-tenant direct Service publication = %v/%v/%v", outcome, conflict, classifyErr)
		}
	} else if !isKind(err, errs.KindScopeUnauthorized) && !isKind(err, errs.KindStateConflict) {
		t.Fatalf("cross-tenant direct Service publication error = %v", err)
	}
}

func directServiceMutationMarker(environmentID string, serviceID string, key string) IdempotencyMarker {
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPatch, Route: "/services/{id}", Key: "service-fixture-" + key,
	}
	marker.CreatedAt = testAttachTime
	marker.UpdatedAt = testAttachTime
	marker.TerminalAt = testAttachTime
	marker.RetainUntil = testAttachTime.Add(markerRetention)
	marker.Response.Body = []byte(`{"id":"` + serviceID + `"}`)
	return marker
}

func stageDirectServicePublicationForTest(
	t *testing.T,
	ctx context.Context,
	store hierarchyStore,
	fixture desiredServiceFixture,
	marker IdempotencyMarker,
	expectedHeadRevision int64,
) EnvironmentBlueprintStageClaim {
	t.Helper()
	claim := fixture.Claim
	claim.Locator = marker.Locator
	claim.Intent = marker.Intent
	claim.TaskID = fixture.Projection.Record.RevisionID
	claim.SourceKind = EnvironmentBlueprintSourceMutation
	claim.BaselineHeadRevision = expectedHeadRevision
	claim.CreatedAt = marker.CreatedAt
	desired := fixture.Service.Record.Desired
	streams, err := buildEnvironmentBlueprintStreams(EnvironmentBlueprintStageRequest{
		Claim: claim,
		Mutation: &EnvironmentDesiredMutationAudit{Service: &EnvironmentServiceMutationAudit{
			Action: EnvironmentServiceMutationCreate, ServiceID: desired.ID,
			Request: &EnvironmentServiceMutationRequest{
				EnvironmentID: fixture.Service.Record.EnvironmentID, Name: desired.Name, Image: desired.Image,
				Zones: desired.Zones, Strategy: desired.Strategy, OnFailure: desired.OnFailure,
				Healthcheck: desired.Healthcheck, Resources: desired.Resources, Expose: desired.Expose,
				Restart: desired.Restart, Replicas: desired.Replicas,
			},
		}},
		Projection:       fixture.Projection.Record,
		DependencyDigest: mustEnvironmentBlueprintDependencyDigest(t, fixture.Projection.Record),
	})
	if err != nil {
		t.Fatalf("build direct Service staging streams: %v", err)
	}
	defer clear(streams.Audit)
	defer clear(streams.Projection)
	descriptor := streams.Descriptor
	descriptor.State = EnvironmentBlueprintStageSealed
	descriptor.NextAuditChunk = descriptor.AuditChunks
	descriptor.NextProjectionChunk = descriptor.ProjectionChunks
	descriptorValue, err := encodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		t.Fatalf("encode direct Service descriptor: %v", err)
	}
	defer clear(descriptorValue)
	intentDigest, err := protectedBlueprintIntentDigest(claim.Intent)
	if err != nil {
		t.Fatalf("direct Service intent digest: %v", err)
	}
	locatorValue, err := encodeEnvironmentBlueprintStageLocator(claim.DescriptorID, intentDigest)
	if err != nil {
		t.Fatalf("encode direct Service locator: %v", err)
	}
	defer clear(locatorValue)
	locatorKey, _, err := environmentBlueprintLocatorKey(claim.Locator)
	if err != nil {
		t.Fatalf("direct Service locator key: %v", err)
	}
	rootValue, err := encodeEnvironmentBlueprintSeal(environmentBlueprintSealFromDescriptor(descriptor))
	if err != nil {
		t.Fatalf("encode direct Service seal: %v", err)
	}
	defer clear(rootValue)
	mutations := []Mutation{
		{Type: MutationPut, Key: environmentBlueprintDescriptorKeyByID(claim.DescriptorID), Value: descriptorValue},
		{Type: MutationPut, Key: environmentBlueprintRootKey(claim.EnvironmentID, claim.RevisionID), Value: rootValue},
		{Type: MutationPut, Key: locatorKey, Value: locatorValue},
	}
	for _, family := range []struct {
		id    uint8
		value []byte
	}{
		{id: EnvironmentBlueprintChunkAudit, value: streams.Audit},
		{id: EnvironmentBlueprintChunkProjection, value: streams.Projection},
	} {
		for index := uint32(0); index < chunkCount32(len(family.value)); index++ {
			from := int(index) * EnvironmentBlueprintChunkBytes
			to := min(from+EnvironmentBlueprintChunkBytes, len(family.value))
			data := family.value[from:to]
			chunkValue, encodeErr := encodeEnvironmentBlueprintChunk(EnvironmentBlueprintChunk{
				Family: family.id, Sequence: index, LogicalOffset: uint64(from),
				LogicalLength: uint32(len(data)), Digest: sha256.Sum256(data), Data: data,
			})
			if encodeErr != nil {
				t.Fatalf("encode direct Service chunk: %v", encodeErr)
			}
			mutations = append(mutations, Mutation{
				Type:  MutationPut,
				Key:   environmentBlueprintChunkKeyFor(claim.EnvironmentID, claim.RevisionID, family.id, index),
				Value: chunkValue,
			})
		}
	}
	defer clearMutationValues(mutations)
	result, err := store.Transact(ctx, nil, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("stage direct Service publication = %#v, %v", result, err)
	}
	return claim
}

func mustEnvironmentBlueprintDependencyDigest(
	t *testing.T,
	projection EnvironmentComposeProjection,
) [sha256.Size]byte {
	t.Helper()
	digest, err := EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatalf("EnvironmentBlueprintDependencyDigest() error = %v", err)
	}
	return digest
}

func joinedServiceMutationClaim(
	service ServiceRecord,
	projection EnvironmentComposeProjection,
	marker IdempotencyMarker,
) EnvironmentBlueprintStageClaim {
	return EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(projection.RevisionID, "task_"),
		EnvironmentID: service.EnvironmentID, RevisionID: projection.RevisionID, TaskID: projection.RevisionID,
		Locator: marker.Locator, Intent: marker.Intent, BaselineHeadRevision: 0,
		SourceKind: EnvironmentBlueprintSourceMutation, RenderGeneration: projection.RenderGeneration,
		ProjectionSchema: 1, CreatedAt: marker.CreatedAt,
	}
}

// Rationale: a no-op Attach rename persists replay evidence without advancing
// the epoch, and its exact replay remains available while a later lock is held.
func TestAttachRenameNoOpPersistsMarkerWithoutEpochAdvance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 940, "no-op-rename", nil)
	current := createTestAttach(t, ctx, repository, scope, record, &facts)
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment,
		ScopeID:   record.EnvironmentID,
		Method:    http.MethodPatch,
		Route:     "/attaches/{id}",
		Key:       "attach-no-op-rename-key-0001",
	}
	marker.Response = IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"id":"` + record.ID + `"}`),
	}
	marker.ReplayTarget = &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetAttach, ID: record.ID}
	epochBefore := mustEnvironmentMutationEpochRevision(t, store, record.EnvironmentID)
	result, err := repository.RenameAttachIdempotent(
		ctx, scope.Environment, scope.Project, current, current.Record.Name, marker,
	)
	if err != nil {
		t.Fatalf("RenameAttachIdempotent(no-op) error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RenameAttachIdempotent(no-op) = %v, %v, %v", outcome, conflict, classifyErr)
	}
	if epoch := mustEnvironmentMutationEpochRevision(t, store, record.EnvironmentID); epoch != epochBefore {
		t.Fatalf("no-op rename advanced epoch from %d to %d", epochBefore, epoch)
	}
	putEnvironmentMutationFenceTestLock(
		t,
		store.memoryHierarchyStore,
		record.EnvironmentID,
		environmentMutationFenceTestOwner(testAttachTime, 941),
	)
	replay, err := repository.RenameAttachIdempotent(
		ctx, scope.Environment, scope.Project, current, current.Record.Name, marker,
	)
	if err != nil {
		t.Fatalf("RenameAttachIdempotent(replay under lock) error = %v", err)
	}
	replayOutcome, existing, replayConflict, replayClassifyErr := replay.Classify()
	defer clear(existing.Intent.Ciphertext)
	defer clear(existing.Response.Body)
	if replayClassifyErr != nil || replayConflict != nil || replayOutcome != IdempotencyKnownExisting {
		t.Fatalf("RenameAttachIdempotent(replay) = %v, %v, %v", replayOutcome, replayConflict, replayClassifyErr)
	}
}

type ordinaryServiceMutationAuditStore struct {
	*memoryHierarchyStore
	revisions         []int64
	raceEnvironmentID string
	raceEpochValue    []byte
	raced             bool
}

func (store *ordinaryServiceMutationAuditStore) GetMany(
	ctx context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	result, err := store.memoryHierarchyStore.GetMany(ctx, request)
	revision := request.Revision
	if revision == 0 && result != nil {
		revision = result.ReadRevision
	}
	store.revisions = append(store.revisions, revision)
	return result, err
}

func (store *ordinaryServiceMutationAuditStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if store.raceEnvironmentID != "" && !store.raced {
		store.raced = true
		if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []Mutation{{
			Type: MutationPut, Key: environmentMutationEpochKey(store.raceEnvironmentID), Value: store.raceEpochValue,
		}}); err != nil {
			return TransactionResult{}, err
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

func mustEnvironmentMutationEpochRevision(
	t *testing.T,
	store interface {
		Get(context.Context, string) (*GetResult, error)
	},
	environmentID string,
) int64 {
	t.Helper()
	result, err := store.Get(context.Background(), environmentMutationEpochKey(environmentID))
	if err != nil || result == nil || result.Entry == nil {
		t.Fatalf("get mutation epoch = %#v, %v", result, err)
	}
	if _, err := decodeEnvironmentMutationEpochRecord(result.Entry.Value); err != nil {
		t.Fatalf("decodeEnvironmentMutationEpochRecord() error = %v", err)
	}
	return result.Entry.ModRevision
}

func readyServiceMutationEnvironment(
	t *testing.T,
	ctx context.Context,
	store hierarchyStore,
	environment Versioned[EnvironmentRecord],
) Versioned[EnvironmentRecord] {
	t.Helper()
	ready, err := CompleteEnvironmentProvisioning(
		environment.Record, environment.Record.CreateTaskID, true,
	)
	if err != nil {
		t.Fatalf("CompleteEnvironmentProvisioning() error = %v", err)
	}
	value, err := encodeEnvironment(ready)
	if err != nil {
		t.Fatalf("encode ready Environment = %v", err)
	}
	defer clear(value)
	result, err := store.Transact(ctx,
		[]Condition{{Key: environmentKey(ready.ID), ModRevision: environment.Revision}},
		[]Mutation{{Type: MutationPut, Key: environmentKey(ready.ID), Value: value}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("persist ready Environment = %#v, %v", result, err)
	}
	return Versioned[EnvironmentRecord]{
		Record: ready, Revision: result.Revision, ReadRevision: result.Revision,
	}
}
