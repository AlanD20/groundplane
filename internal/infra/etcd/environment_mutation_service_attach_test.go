package etcd

import (
	"context"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: every ordinary Service write must read one fixed Environment
// snapshot and advance its epoch in the same successful transaction.
func TestServiceMutationUsesFixedRevisionAndAdvancesEpoch(t *testing.T) {
	t.Parallel()
	_, store, environment, project := serviceRepositoryTestHierarchy(t)
	audited := &ordinaryServiceMutationAuditStore{memoryHierarchyStore: store}
	repository, err := newServiceRepository(audited)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	record := serviceRepositoryTestRecord(t, environment.Record.ID, 910, "fixed-revision")
	created, err := repository.CreateService(context.Background(), environment, project, record)
	if err != nil {
		t.Fatalf("CreateService() error = %v", err)
	}
	if len(audited.revisions) < 4 {
		t.Fatalf("fixed-revision GetMany calls = %v", audited.revisions)
	}
	for _, revision := range audited.revisions {
		if revision != audited.revisions[0] || revision <= 0 {
			t.Fatalf("fixed-revision GetMany calls = %v", audited.revisions)
		}
	}
	epoch := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID)
	if epoch != created.Revision {
		t.Fatalf("mutation epoch revision = %d, want Service revision %d", epoch, created.Revision)
	}
}

// Rationale: an epoch race must fail the Service CAS without publishing any
// primary or index, and an existing operation lock must reject even a no-op edit.
func TestServiceMutationEpochRaceAndHeldLockPerformNoDomainWrite(t *testing.T) {
	t.Parallel()
	_, store, environment, project := serviceRepositoryTestHierarchy(t)
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
	repository, err := newServiceRepository(tracing)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	record := serviceRepositoryTestRecord(t, environment.Record.ID, 920, "epoch-race")
	if _, err := repository.CreateService(context.Background(), environment, project, record); !isKind(
		err,
		errs.KindStateConflict,
	) {
		t.Fatalf("CreateService(epoch race) error = %v", err)
	}
	if _, err := repository.GetService(
		context.Background(),
		record.Desired.ID,
	); !isKind(
		err,
		errs.KindServiceNotFound,
	) {
		t.Fatalf("GetService(after epoch race) error = %v", err)
	}

	repository, err = newServiceRepository(store)
	if err != nil {
		t.Fatalf("newServiceRepository(base) error = %v", err)
	}
	createdRecord := serviceRepositoryTestRecord(t, environment.Record.ID, 921, "no-op")
	created, err := repository.CreateService(context.Background(), environment, project, createdRecord)
	if err != nil {
		t.Fatalf("CreateService(no-op fixture) error = %v", err)
	}
	epochBefore := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID)
	storeRevisionBefore := store.revision
	unchanged, err := repository.ReplaceDesired(
		context.Background(), environment, project, created, created.Record.Desired,
	)
	if err != nil || unchanged.Revision != created.Revision || store.revision != storeRevisionBefore ||
		mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID) != epochBefore {
		t.Fatalf("ReplaceDesired(no-op) = %#v, %v", unchanged, err)
	}
	putEnvironmentMutationFenceTestLock(
		t,
		store,
		environment.Record.ID,
		environmentMutationFenceTestOwner(serviceRecordTestTime(), 922),
	)
	if _, err := repository.ReplaceDesired(
		context.Background(), environment, project, created, created.Record.Desired,
	); !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("ReplaceDesired(held lock) error = %v", err)
	}
}

// Rationale: caller-supplied Versioned records cannot move a Service mutation
// into another Tenant because the canonical fence resolves actual ancestry.
func TestServiceMutationRejectsCrossTenantHierarchySpoof(t *testing.T) {
	t.Parallel()
	repository, store, environment, _ := serviceRepositoryTestHierarchy(t)
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
	record := serviceRepositoryTestRecord(t, environment.Record.ID, 932, "cross-tenant")
	if _, err := repository.CreateService(
		context.Background(), forgedEnvironment, otherProject, record,
	); !isKind(err, errs.KindScopeUnauthorized) {
		t.Fatalf("CreateService(cross-tenant) error = %v", err)
	}
	if _, err := repository.GetService(
		context.Background(),
		record.Desired.ID,
	); !isKind(
		err,
		errs.KindServiceNotFound,
	) {
		t.Fatalf("GetService(cross-tenant) error = %v", err)
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
	store.revisions = append(store.revisions, request.Revision)
	return store.memoryHierarchyStore.GetMany(ctx, request)
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
