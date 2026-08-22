package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Attach creation must publish metadata, every ownership index, and encrypted facts in one revision.
func TestAttachRepositoryCreatesAndReadsAtomicAttach(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 31, "api-db", nil)

	created := createTestAttach(t, ctx, repository, scope, record, &facts)
	if created.Revision == 0 || created.Record.ID != record.ID {
		t.Fatalf("CreateAttach() = %#v", created)
	}
	resolved, err := repository.ResolveAttach(ctx, record.EnvironmentID, record.Name)
	if err != nil {
		t.Fatalf("ResolveAttach() error = %v", err)
	}
	if resolved.Record.ID != record.ID || resolved.ReadRevision != created.Revision {
		t.Fatalf("ResolveAttach() = %#v", resolved)
	}
	page, err := repository.ListAttaches(ctx, record.EnvironmentID, PageRequest{Limit: 10})
	if err != nil {
		t.Fatalf("ListAttaches() error = %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].Record.ID != record.ID {
		t.Fatalf("ListAttaches() = %#v", page)
	}
	storedFacts, ok, err := repository.GetAttachFacts(ctx, created)
	if err != nil {
		t.Fatalf("GetAttachFacts() error = %v", err)
	}
	defer clear(storedFacts.Ciphertext)
	if !ok || string(storedFacts.Ciphertext) != "encrypted-facts" {
		t.Fatalf("GetAttachFacts() = %#v, %t", storedFacts, ok)
	}
	for _, key := range []string{
		attachOwnerKey(record.EnvironmentID, record.ID),
		attachNameKey(record.EnvironmentID, record.Name),
		attachServiceKey(record.ServiceIDs[0], record.ID),
		attachBackingServiceKey(record.BackingServiceID, record.ID),
		attachBackingProjectKey(record.BackingProjectID, record.ID),
	} {
		result, getErr := store.Get(ctx, key)
		if getErr != nil || result.Entry == nil || string(result.Entry.Value) != record.ID ||
			result.Entry.ModRevision != created.Revision {
			t.Fatalf("index %s = %#v, error = %v", key, result, getErr)
		}
	}
}

// Rationale: retry must retain the exact generated identity and fact metadata while changing only task lifecycle state.
func TestAttachLifecycleRetryPreservesIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 41, "worker-db", nil)
	current := createTestAttach(t, ctx, repository, scope, record, &facts)
	provisioning, err := MarkAttachProvisioning(current.Record, current.Record.TaskID)
	if err != nil {
		t.Fatalf("MarkAttachProvisioning() error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, provisioning)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(provisioning) error = %v", err)
	}
	failed, err := CompleteAttachProvisioning(current.Record, current.Record.TaskID, false)
	if err != nil {
		t.Fatalf("CompleteAttachProvisioning() error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, failed)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(failed) error = %v", err)
	}
	retryTaskID := ids.NewAt(ids.KindTask, testAttachTime.Add(time.Minute), 42)
	retried, err := RetryAttachOperation(current.Record, retryTaskID)
	if err != nil {
		t.Fatalf("RetryAttachOperation() error = %v", err)
	}
	if retried.ID != record.ID || retried.Name != record.Name || retried.TaskID != retryTaskID ||
		retried.Status != core.AttachPending || len(retried.FactSets) != len(record.FactSets) ||
		retried.FactSets[0].Facts[0] != record.FactSets[0].Facts[0] {
		t.Fatalf("RetryAttachOperation() changed durable identity: %#v", retried)
	}
	if _, err = repository.ReplaceLifecycle(ctx, current, retried); err != nil {
		t.Fatalf("ReplaceLifecycle(retry) error = %v", err)
	}
}

// Rationale: reverse grant membership must prevent target deletion and serialize grant creation against target lifecycle.
func TestAttachRepositoryProtectsGrantedAttach(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	targetRecord, targetFacts := testPendingAttach(t, scope, 51, "target-db", nil)
	target := createTestAttach(t, ctx, repository, scope, targetRecord, &targetFacts)
	target, err = advanceAttachReady(ctx, repository, target)
	if err != nil {
		t.Fatalf("advanceAttachReady() error = %v", err)
	}

	grantScope := scope
	grantScope.Grants = []Versioned[AttachRecord]{target}
	sourceRecord, sourceFacts := testPendingAttach(t, grantScope, 52, "source-db", []Versioned[AttachRecord]{target})
	createTestAttach(t, ctx, repository, grantScope, sourceRecord, &sourceFacts)
	target, err = repository.GetAttach(ctx, target.Record.ID)
	if err != nil {
		t.Fatalf("GetAttach(target) error = %v", err)
	}
	detachTaskID := ids.NewAt(ids.KindTask, testAttachTime.Add(2*time.Minute), 53)
	detaching, err := BeginAttachDetaching(target.Record, detachTaskID)
	if err != nil {
		t.Fatalf("BeginAttachDetaching() error = %v", err)
	}
	target, err = repository.ReplaceLifecycle(ctx, target, detaching)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(detaching) error = %v", err)
	}
	detached, err := CompleteAttachDetaching(target.Record, detachTaskID, true)
	if err != nil {
		t.Fatalf("CompleteAttachDetaching() error = %v", err)
	}
	target, err = repository.ReplaceLifecycle(ctx, target, detached)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(detached) error = %v", err)
	}
	_, err = repository.DeleteDetachedAttach(ctx, target)
	kind, _ := errs.KindOf(err)
	if kind != errs.KindResourceInUse {
		t.Fatalf("DeleteDetachedAttach() error = %v, want resource in use", err)
	}
}

// Rationale: the derived compare-and-mutation budget must reject combinations that cannot be committed atomically.
func TestAttachRepositoryEnforcesTransactionBudget(t *testing.T) {
	t.Parallel()
	record := AttachRecord{
		ServiceIDs:     make([]string, MaximumAttachConsumers),
		GrantAttachIDs: make([]string, 8),
	}
	if got := attachCreateWithTaskOperationCount(record, true); got <= maximumTransactionOperations {
		t.Fatalf(
			"attachCreateWithTaskOperationCount() = %d, want greater than %d",
			got,
			maximumTransactionOperations,
		)
	}
}

var testAttachTime = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

type attachTestStore struct {
	*memoryHierarchyStore
}

func newAttachTestStore() *attachTestStore {
	return &attachTestStore{memoryHierarchyStore: newMemoryHierarchyStore()}
}

func (store *attachTestStore) Health(context.Context) error {
	return nil
}

func (store *attachTestStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut, Key: key, Value: value}})
	return result.Revision, err
}

func (store *attachTestStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationDelete, Key: key}})
	return result.Revision, err
}

func (store *attachTestStore) Watch(context.Context, string, int64) (*WatchStream, error) {
	return nil, errs.New(errs.KindInternal, "Attach test store does not implement Watch")
}

func (store *attachTestStore) Snapshot(context.Context, io.Writer) error {
	return errs.New(errs.KindInternal, "Attach test store does not implement Snapshot")
}

func (store *attachTestStore) Close() error {
	return nil
}

func seedAttachScope(t *testing.T, ctx context.Context, store *attachTestStore) AttachCreateScope {
	t.Helper()
	hierarchy, err := NewHierarchyRepository(store)
	if err != nil {
		t.Fatalf("NewHierarchyRepository() error = %v", err)
	}
	services, err := NewServiceRepository(store)
	if err != nil {
		t.Fatalf("NewServiceRepository() error = %v", err)
	}
	tenant := TenantRecord{
		ID: ids.NewAt(ids.KindTenant, testAttachTime, 1), Slug: "acme", Name: "Acme",
	}
	if _, err = hierarchy.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := hierarchy.CreateProject(ctx, ProjectRecord{
		ID: ids.NewAt(ids.KindProject, testAttachTime, 2), TenantID: tenant.ID, Slug: "app", Name: "App",
		Kind: ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	backingProject, err := hierarchy.CreateProject(ctx, ProjectRecord{
		ID: ids.NewAt(ids.KindProject, testAttachTime, 3), Slug: "postgres", Name: "Postgres",
		Kind: ProjectKindBacking,
	})
	if err != nil {
		t.Fatalf("CreateProject(backing) error = %v", err)
	}
	environmentRecord, err := NewProvisioningEnvironment(
		"/srv/groundplane",
		project.Record,
		ids.NewAt(ids.KindEnvironment, testAttachTime, 4),
		"production",
		ids.NewAt(ids.KindTask, testAttachTime, 5),
		testAttachTime,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	environment, err := hierarchy.CreateEnvironment(ctx, environmentRecord)
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	backingEnvironmentRecord, err := NewProvisioningEnvironment(
		"/srv/groundplane",
		backingProject.Record,
		ids.NewAt(ids.KindEnvironment, testAttachTime, 6),
		"main",
		ids.NewAt(ids.KindTask, testAttachTime, 7),
		testAttachTime,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment(backing) error = %v", err)
	}
	backingEnvironmentValue, err := json.Marshal(backingEnvironmentRecord)
	if err != nil {
		t.Fatalf("json.Marshal(backing Environment) error = %v", err)
	}
	defer clear(backingEnvironmentValue)
	backingEnvironmentRevision, err := store.Put(
		ctx,
		environmentKey(backingEnvironmentRecord.ID),
		backingEnvironmentValue,
	)
	if err != nil {
		t.Fatalf("Put(backing Environment) error = %v", err)
	}
	backingEnvironment := Versioned[EnvironmentRecord]{
		Record:       backingEnvironmentRecord,
		Revision:     backingEnvironmentRevision,
		ReadRevision: backingEnvironmentRevision,
	}
	serviceRecord, err := NewServiceRecord(environment.Record.ID, core.Service{
		ID: ids.NewAt(ids.KindService, testAttachTime, 8), Name: "api", Image: "example/api:1",
	})
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	service, err := services.CreateService(ctx, environment, project, serviceRecord)
	if err != nil {
		t.Fatalf("CreateService() error = %v", err)
	}
	backingServiceRecord, err := NewServiceRecord(backingEnvironment.Record.ID, core.Service{
		ID: ids.NewAt(ids.KindService, testAttachTime, 9), Name: "postgres", Image: "postgres:16-alpine",
		Adapter: "postgres:16",
	})
	if err != nil {
		t.Fatalf("NewServiceRecord(backing) error = %v", err)
	}
	backingServiceValue, err := encodeServiceRecord(backingServiceRecord)
	if err != nil {
		t.Fatalf("encodeServiceRecord(backing) error = %v", err)
	}
	defer clear(backingServiceValue)
	backingServiceRevision, err := store.Put(
		ctx,
		serviceKey(backingServiceRecord.Desired.ID),
		backingServiceValue,
	)
	if err != nil {
		t.Fatalf("Put(backing Service) error = %v", err)
	}
	backingService := Versioned[ServiceRecord]{
		Record: backingServiceRecord, Revision: backingServiceRevision, ReadRevision: backingServiceRevision,
	}
	return AttachCreateScope{
		Project: project, Environment: environment, Services: []Versioned[ServiceRecord]{service},
		BackingProject: backingProject, BackingEnvironment: backingEnvironment, BackingService: backingService,
	}
}

func testPendingAttach(
	t *testing.T,
	scope AttachCreateScope,
	seed int64,
	name string,
	grants []Versioned[AttachRecord],
) (AttachRecord, AttachEncryptedFacts) {
	t.Helper()
	grantIDs := make([]string, 0, len(grants))
	factSets := []AttachFactSetMetadata{{Facts: []AttachFactDefinition{
		{Key: "pg16_DATABASE"},
		{Key: "pg16_PASSWORD", Secret: true},
		{Key: "pg16_URL", Secret: true},
	}}}
	for _, grant := range grants {
		grantIDs = append(grantIDs, grant.Record.ID)
		factSets = append(factSets, AttachFactSetMetadata{
			GrantAttachID: grant.Record.ID,
			Facts: []AttachFactDefinition{
				{Key: "pg16_DATABASE"},
				{Key: "pg16_PASSWORD", Secret: true},
				{Key: "pg16_URL", Secret: true},
			},
		})
	}
	id := ids.NewAt(ids.KindAttach, testAttachTime, seed)
	record, err := NewPendingAttachRecord(
		id,
		scope.Environment.Record.ID,
		name,
		scope.BackingProject.Record.ID,
		scope.BackingEnvironment.Record.ID,
		scope.BackingService.Record.Desired.ID,
		[]string{scope.Services[0].Record.Desired.ID},
		grantIDs,
		factSets,
		ids.NewAt(ids.KindTask, testAttachTime, seed+100),
		testAttachTime,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	facts, err := NewAttachEncryptedFacts(record.ID, 1, "age-x25519", "sha256", []byte("encrypted-facts"))
	if err != nil {
		t.Fatalf("NewAttachEncryptedFacts() error = %v", err)
	}
	return record, facts
}

func advanceAttachReady(
	ctx context.Context,
	repository *AttachRepository,
	current Versioned[AttachRecord],
) (Versioned[AttachRecord], error) {
	provisioning, err := MarkAttachProvisioning(current.Record, current.Record.TaskID)
	if err != nil {
		return Versioned[AttachRecord]{}, err
	}
	current, err = repository.ReplaceLifecycle(ctx, current, provisioning)
	if err != nil {
		return Versioned[AttachRecord]{}, err
	}
	ready, err := CompleteAttachProvisioning(current.Record, current.Record.TaskID, true)
	if err != nil {
		return Versioned[AttachRecord]{}, err
	}
	return repository.ReplaceLifecycle(ctx, current, ready)
}

func createTestAttach(
	t *testing.T,
	ctx context.Context,
	repository *AttachRepository,
	scope AttachCreateScope,
	record AttachRecord,
	facts *AttachEncryptedFacts,
) Versioned[AttachRecord] {
	t.Helper()
	recordDigest := sha256.Sum256([]byte(record.ID))
	seed := int64(binary.BigEndian.Uint64(recordDigest[:8]))
	planDigest := sha256.Sum256([]byte("attach-plan-" + record.ID))
	task := TaskRecord{
		ID: record.TaskID, OperationID: ids.NewAt(ids.KindOperation, record.CreatedAt, seed),
		IdempotencyKey: "attach-create-key-" + record.ID,
		Executor:       TaskExecutorAgent, PlanID: ids.NewAt(ids.KindPlan, record.CreatedAt, seed+1),
		PlanHash: hex.EncodeToString(planDigest[:]), RenderGeneration: 1,
		Type: TaskAttach, Target: record.ID,
		Steps:          []TaskStepRecord{{ID: ids.NewAt(ids.KindStep, record.CreatedAt, seed+2)}},
		TimeoutSeconds: 120, Status: TaskStatusPending, NextEventSequence: 1, CreatedAt: record.CreatedAt,
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		t.Fatalf("json.Marshal(TaskAccepted) error = %v", err)
	}
	intentCiphertext := []byte("protected-attach-intent-" + record.ID)
	intentDigest := sha256.Sum256(intentCiphertext)
	marker := IdempotencyMarker{
		Kind: IdempotencyMarkerTask, State: IdempotencyMarkerPending,
		Locator: IdempotencyLocator{
			ScopeKind: IdempotencyScopeEnvironment, ScopeID: record.EnvironmentID,
			Method: http.MethodPost, Route: "/attaches", Key: task.IdempotencyKey,
		},
		Intent: ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(intentDigest[:]), Ciphertext: intentCiphertext,
		},
		Response: IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: responseBody,
		},
		TaskID: task.ID, CreatedAt: record.CreatedAt, UpdatedAt: record.CreatedAt,
	}
	result, err := repository.CreateAttachWithTask(ctx, scope, record, facts, task, marker)
	if err != nil {
		t.Fatalf("CreateAttachWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf(
			"CreateAttachWithTask().Classify() = %v, %v, %v",
			outcome,
			conflict,
			classifyErr,
		)
	}
	created, err := repository.GetAttach(ctx, record.ID)
	if err != nil {
		t.Fatalf("GetAttach(created) error = %v", err)
	}
	queued, err := repository.store.Get(ctx, taskQueueKey(TaskExecutorAgent, task.ID))
	if err != nil || queued == nil || queued.Entry == nil || queued.Entry.ModRevision != created.Revision {
		t.Fatalf("Attach Task queue = %#v, error = %v", queued, err)
	}
	return created
}
