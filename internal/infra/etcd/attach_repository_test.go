package etcd

import (
	context "context"
	errors "errors"
	io "io"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	core "github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
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
	epoch, err := store.Get(ctx, testhierarchy.EnvironmentMutationEpochKey(record.EnvironmentID))
	if err != nil || epoch.Entry == nil || epoch.Entry.ModRevision != created.Revision {
		t.Fatalf("Attach creation mutation epoch = %#v, %v", epoch, err)
	}
	resolved, err := repository.ResolveAttach(ctx, record.EnvironmentID, record.Name)
	if err != nil {
		t.Fatalf("ResolveAttach() error = %v", err)
	}
	if resolved.Record.ID != record.ID || resolved.ReadRevision != created.Revision {
		t.Fatalf("ResolveAttach() = %#v", resolved)
	}
	if resolved.Record.BackingNetworkID != record.BackingNetworkID {
		t.Fatalf("ResolveAttach() lost backing network binding: %#v", resolved.Record)
	}
	page, err := repository.ListAttaches(ctx, record.EnvironmentID, testkeyvalue.PageRequest{Limit: 10})
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
	for _, key := range []string{testattachments.AttachOwnerKey(record.EnvironmentID, record.ID), testattachments.AttachNameKey(record.EnvironmentID, record.Name), testattachments.AttachServiceKey(record.ServiceID, record.ID), testattachments.AttachBackingServiceKey(record.BackingServiceID, record.ID), testattachments.AttachBackingProjectKey(record.BackingProjectID, record.ID)} {
		result, getErr := store.Get(ctx, key)
		if getErr != nil || result.Entry == nil || string(result.Entry.Value) != record.ID ||
			result.Entry.ModRevision != created.Revision {
			t.Fatalf("index %s = %#v, error = %v", key, result, getErr)
		}
	}
}

// Rationale: direct Attach publication must durably capture the selected
// backing authentication mode and publish no adapter-procedure Task steps for
// a no-auth backing Service.
func TestAttachRepositoryPublishesNoAuthenticationMode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	backing := seedDesiredServiceFixture(
		t,
		ctx,
		store,
		scope.BackingEnvironment.Record.ID,
		core.Service{
			ID:   ids.NewAt(ids.KindService, testAttachTime, 301),
			Name: "valkey", Image: "valkey/valkey:9-alpine",
			Adapter: "valkey:9", Authentication: core.BackingAuthenticationNone,
		},
		ids.NewAt(ids.KindNetwork, testAttachTime, 302),
		303,
		true,
		true,
	)
	scope.BackingService = backing.Service
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 304, "api-cache", nil)

	created := createTestAttach(t, ctx, repository, scope, record, &facts)
	storedTask, err := store.Get(ctx, testtaskjournal.TaskStorageKey(created.Record.TaskID))
	if err != nil || storedTask == nil || storedTask.Entry == nil {
		t.Fatalf("Get(published Task) = %#v, %v", storedTask, err)
	}
	task, err := DecodeTaskRecord(storedTask.Entry.Value)
	if err != nil {
		t.Fatalf("decodeTaskRecord() error = %v", err)
	}
	stored, err := repository.GetAttachTaskRenderInput(ctx, task.PlanID)
	if err != nil {
		t.Fatalf("GetAttachTaskRenderInput() error = %v", err)
	}
	if stored.Record.Authentication != core.BackingAuthenticationNone {
		t.Fatalf("published authentication = %q, want none", stored.Record.Authentication)
	}
	if len(task.Steps) != 1 {
		t.Fatalf("published no-auth Task steps = %d, want 1", len(task.Steps))
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
	provisioning, err := testattachments.MarkAttachProvisioning(current.Record, current.Record.TaskID)
	if err != nil {
		t.Fatalf("MarkAttachProvisioning() error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, provisioning)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(provisioning) error = %v", err)
	}
	failed, err := testattachments.CompleteAttachProvisioning(current.Record, current.Record.TaskID, false)
	if err != nil {
		t.Fatalf("CompleteAttachProvisioning() error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, failed)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(failed) error = %v", err)
	}
	retryTaskID := ids.NewAt(ids.KindTask, testAttachTime.Add(time.Minute), 42)
	retried, err := testattachments.RetryAttachOperation(current.Record, retryTaskID)
	if err != nil {
		t.Fatalf("RetryAttachOperation() error = %v", err)
	}
	if retried.ID != record.ID || retried.Name != record.Name || retried.TaskID != retryTaskID ||
		retried.Status != core.AttachPending || retried.BackingNetworkID != record.BackingNetworkID ||
		len(retried.FactSets) != len(record.FactSets) ||
		retried.FactSets[0].Facts[0] != record.FactSets[0].Facts[0] {
		t.Fatalf("RetryAttachOperation() changed durable identity: %#v", retried)
	}
	if _, err = repository.ReplaceLifecycle(ctx, current, retried); err != nil {
		t.Fatalf("ReplaceLifecycle(retry) error = %v", err)
	}
}

// Rationale: a missing Service runtime sidecar is the normal pre-observation state and must
// resolve to the explicit running intent without changing the sealed desired projection.
func TestServiceResolutionDefaultsMissingRuntimeToRunning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	serviceID := scope.Services[0].Record.Desired.ID
	if _, err := store.Delete(ctx, testservices.ServiceRuntimeKey(serviceID)); err != nil {
		t.Fatalf("Delete(Service runtime sidecar) error = %v", err)
	}
	services, err := NewServiceRepository(store)
	if err != nil {
		t.Fatalf("NewServiceRepository() error = %v", err)
	}
	resolved, err := services.GetService(ctx, serviceID)
	if err != nil {
		t.Fatalf("GetService() error = %v", err)
	}
	if resolved.Record.Desired.ID != serviceID || resolved.Record.Runtime.ServiceID != serviceID ||
		resolved.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		t.Fatalf("GetService(missing runtime) = %#v", resolved.Record)
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
	grantScope.Grants = []testkeyvalue.Versioned[testattachments.Record]{target}
	sourceRecord, sourceFacts := testPendingAttach(
		t,
		grantScope,
		52,
		"source-db",
		[]testkeyvalue.Versioned[testattachments.Record]{target},
	)
	createTestAttach(t, ctx, repository, grantScope, sourceRecord, &sourceFacts)
	target, err = repository.GetAttach(ctx, target.Record.ID)
	if err != nil {
		t.Fatalf("GetAttach(target) error = %v", err)
	}
	detachTaskID := ids.NewAt(ids.KindTask, testAttachTime.Add(2*time.Minute), 53)
	detaching, err := testattachments.BeginAttachDetaching(target.Record, detachTaskID)
	if err != nil {
		t.Fatalf("BeginAttachDetaching() error = %v", err)
	}
	target, err = repository.ReplaceLifecycle(ctx, target, detaching)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(detaching) error = %v", err)
	}
	detached, err := testattachments.CompleteAttachDetaching(target.Record, detachTaskID, true)
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

// Rationale: the locked one-consumer/eight-grant maximum must remain within the atomic transaction ceiling.
func TestAttachRepositoryMaximumCombinationFitsTransactionBudget(t *testing.T) {
	t.Parallel()
	record := testattachments.Record{
		ID:                 ids.NewAt(ids.KindAttach, testAttachTime, 902),
		ServiceID:          ids.NewAt(ids.KindService, testAttachTime, 901),
		CredentialAttachID: ids.NewAt(ids.KindAttach, testAttachTime, 902),
		GrantAttachIDs:     make([]string, 8),
	}
	if got := attachCreateWithTaskOperationCount(record, true); got > testkeyvalue.MaximumOperations {
		t.Fatalf(
			"attachCreateWithTaskOperationCount() = %d, want at most %d",
			got, testkeyvalue.MaximumOperations,
		)
	}
	if got := attachDetachWithTaskOperationCount(record); got != 63 {
		t.Fatalf("attachDetachWithTaskOperationCount() = %d, want 63", got)
	}
}

// Rationale: detach intent, immutable plan input, Agent Task, queue membership, replay evidence,
// and the Environment topology fence must become visible at one revision.
func TestAttachRepositoryPublishesDetachTaskAtomically(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 61, "detach-atomic", nil)
	ready := createTestAttach(t, ctx, repository, scope, record, &facts)
	ready, err = advanceAttachReady(ctx, repository, ready)
	if err != nil {
		t.Fatalf("advanceAttachReady() error = %v", err)
	}

	task := publishTestDetach(t, ctx, repository, scope, ready, record.CreatedAt.Add(time.Minute))
	detaching, err := repository.GetAttach(ctx, record.ID)
	if err != nil || detaching.Record.Status != core.AttachDetaching || detaching.Record.TaskID != task.ID {
		t.Fatalf("detaching Attach = %#v, %v", detaching, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	currentTask, err := tasks.GetTask(ctx, task.ID)
	if err != nil || currentTask.Revision != detaching.Revision {
		t.Fatalf("detach Task = %#v, %v", currentTask, err)
	}
	renderInput, err := repository.GetAttachTaskRenderInput(ctx, task.PlanID)
	if err != nil || renderInput.Revision != detaching.Revision {
		t.Fatalf("detach render input = %#v, %v", renderInput, err)
	}
	epoch, err := store.Get(ctx, testhierarchy.EnvironmentMutationEpochKey(record.EnvironmentID))
	if err != nil || epoch.Entry == nil || epoch.Entry.ModRevision != detaching.Revision {
		t.Fatalf("Attach mutation epoch = %#v, %v", epoch, err)
	}
}

// Rationale: the final transaction after idempotency, Environment fencing,
// and Backup exclusion composition must fit both its estimator and store cap.
func TestAttachDetachOperationBudgetMatchesComposedTransaction(t *testing.T) {
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 65, "detach-budget", nil)
	ready := createTestAttach(t, ctx, repository, scope, record, &facts)
	ready, err = advanceAttachReady(ctx, repository, ready)
	if err != nil {
		t.Fatalf("advanceAttachReady() error = %v", err)
	}
	capture := &attachDetachOperationCaptureStore{
		attachTestStore: store,
		attachID:        record.ID,
	}
	capturedRepository, err := NewAttachRepository(capture)
	if err != nil {
		t.Fatalf("NewAttachRepository(capture) error = %v", err)
	}
	publishTestDetach(t, ctx, capturedRepository, scope, ready, record.CreatedAt.Add(5*time.Minute))
	want := attachDetachWithTaskOperationCount(record)
	if capture.operations <= 0 || capture.operations > want || capture.operations > testkeyvalue.MaximumOperations {
		t.Fatalf(
			"composed Attach detach operations = %d, want at most %d and at most %d",
			capture.operations,
			want, testkeyvalue.MaximumOperations,
		)
	}
}

// Rationale: a new incoming grant committed after the fixed read must fail the
// removal CAS so the detached Attach and its new reverse membership survive.
func TestDeleteDetachedAttachRacesIncomingGrant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 66, "incoming-grant-race", nil)
	current := createTestAttach(t, ctx, repository, scope, record, &facts)
	current, err = advanceAttachReady(ctx, repository, current)
	if err != nil {
		t.Fatalf("advanceAttachReady() error = %v", err)
	}
	taskID := ids.NewAt(ids.KindTask, testAttachTime.Add(6*time.Minute), 660)
	detaching, err := testattachments.BeginAttachDetaching(current.Record, taskID)
	if err != nil {
		t.Fatalf("BeginAttachDetaching() error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, detaching)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(detaching) error = %v", err)
	}
	detached, err := testattachments.CompleteAttachDetaching(current.Record, taskID, true)
	if err != nil {
		t.Fatalf("CompleteAttachDetaching() error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, detached)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(detached) error = %v", err)
	}

	dependentID := ids.NewAt(ids.KindAttach, testAttachTime.Add(6*time.Minute), 661)
	reverseKey := testattachments.AttachGrantedByKey(record.ID, dependentID)
	racingStore := &attachIncomingGrantRaceStore{
		attachTestStore: store,
		prefix:          testattachments.AttachGrantedByPrefix(record.ID),
		key:             reverseKey,
		value:           []byte(dependentID),
	}
	racingRepository, err := NewAttachRepository(racingStore)
	if err != nil {
		t.Fatalf("NewAttachRepository(race) error = %v", err)
	}
	if _, err := racingRepository.DeleteDetachedAttach(ctx, current); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("DeleteDetachedAttach(incoming grant race) error = %v, want state conflict", err)
	}
	stored, err := repository.GetAttach(ctx, record.ID)
	if err != nil || stored.Record.Status != core.AttachDetached {
		t.Fatalf("GetAttach(after incoming grant race) = %#v/%v", stored, err)
	}
	reverse, err := store.Get(ctx, reverseKey)
	if err != nil || reverse.Entry == nil || string(reverse.Entry.Value) != dependentID {
		t.Fatalf("incoming grant reverse membership = %#v/%v", reverse, err)
	}
}

// Rationale: an exclusion committed after the fixed read is ResourceInUse only
// when it is valid and exactly bucketed; corrupt concurrent evidence is internal.
func TestAttachLifecycleReplacementRacesBackupSourceExclusion(t *testing.T) {
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 64, "backup-source-race", nil)
	ready := createTestAttach(t, ctx, repository, scope, record, &facts)
	ready, err = advanceAttachReady(ctx, repository, ready)
	if err != nil {
		t.Fatalf("advanceAttachReady() error = %v", err)
	}
	exclusionKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
		testbackupruntime.BackupSourceTargetAttach,
		record.ID,
	)
	if err != nil {
		t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
	}
	for index, test := range []struct {
		name     string
		value    []byte
		wantKind errs.Kind
	}{
		{
			name:     "valid",
			value:    testAttachBackupExclusionValue(t, scope.Environment.Record.ID, record.ID, 641),
			wantKind: errs.KindResourceInUse,
		},
		{
			name:     "malformed",
			value:    []byte("not-an-exclusion-record"),
			wantKind: errs.KindInternal,
		},
		{
			name: "misbucketed",
			value: testAttachBackupExclusionValue(
				t,
				scope.Environment.Record.ID,
				ids.NewAt(ids.KindAttach, testAttachTime.Add(4*time.Minute), 642),
				643,
			),
			wantKind: errs.KindInternal,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			raceStore := &attachBackupExclusionRaceStore{
				attachTestStore: store,
				exclusionKey:    exclusionKey,
				exclusionValue:  test.value,
			}
			raceRepository, err := NewAttachRepository(raceStore)
			if err != nil {
				t.Fatalf("NewAttachRepository(race) error = %v", err)
			}
			detaching, err := testattachments.BeginAttachDetaching(
				ready.Record,
				ids.NewAt(ids.KindTask, testAttachTime.Add(4*time.Minute), int64(640+index)),
			)
			if err != nil {
				t.Fatalf("BeginAttachDetaching() error = %v", err)
			}
			if _, err = raceRepository.ReplaceLifecycle(ctx, ready, detaching); !isKind(err, test.wantKind) {
				t.Fatalf("ReplaceLifecycle(detaching %s race) error = %v, want %v", test.name, err, test.wantKind)
			}
			if _, err := store.Delete(ctx, exclusionKey); err != nil {
				t.Fatalf("Delete(%s exclusion) error = %v", test.name, err)
			}
			stored, err := repository.GetAttach(ctx, record.ID)
			if err != nil || stored.Record.Status != core.AttachReady {
				t.Fatalf("GetAttach(after exclusion race) = %#v/%v", stored, err)
			}
		})
	}
}

var testAttachTime = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

// Rationale: multiple Services resolved from one Environment revision share
// one desired-head compare without dropping the separate backing Environment fence.
func TestAttachDesiredHeadConditionsCoalesceOneEnvironmentRevision(t *testing.T) {
	consumerEnvironmentID := ids.NewAt(ids.KindEnvironment, testAttachTime, 700)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, testAttachTime, 701)
	consumerRevision := int64(41)
	consumerService := selectedServiceFixture(t, consumerEnvironmentID,
		ids.NewAt(ids.KindService, testAttachTime, 703), "", consumerRevision)
	secondConsumerService := consumerService
	secondConsumerService.Record.Desired.ID = ids.NewAt(ids.KindService, testAttachTime, 704)
	backingService := selectedServiceFixture(t, backingEnvironmentID,
		ids.NewAt(ids.KindService, testAttachTime, 705), "", 52)

	conditions, err := attachDesiredHeadConditions(
		consumerEnvironmentID,
		consumerRevision,
		backingService,
		[]testkeyvalue.Versioned[testservices.ServiceRecord]{consumerService, secondConsumerService},
	)
	if err != nil {
		t.Fatalf("attachDesiredHeadConditions() error = %v", err)
	}
	if len(conditions) != 2 {
		t.Fatalf("attachDesiredHeadConditions() count = %d, want 2", len(conditions))
	}
	if conditions[0].Key != testblueprints.EnvironmentBlueprintHeadKey(consumerEnvironmentID) ||
		conditions[0].ModRevision != consumerRevision {
		t.Fatalf("consumer desired-head condition = %#v", conditions[0])
	}
	if conditions[1].Key != testblueprints.EnvironmentBlueprintHeadKey(backingEnvironmentID) ||
		conditions[1].ModRevision != backingService.Revision {
		t.Fatalf("backing desired-head condition = %#v", conditions[1])
	}
}

// Rationale: coalescing must never hide a Service desired-head change observed
// at a different revision of the same Environment.
func TestAttachDesiredHeadConditionsRejectConflictingEnvironmentRevision(t *testing.T) {
	environmentID := ids.NewAt(ids.KindEnvironment, testAttachTime, 702)
	service := selectedServiceFixture(t, environmentID,
		ids.NewAt(ids.KindService, testAttachTime, 706), "", 61)

	_, err := attachDesiredHeadConditions(
		environmentID,
		service.Revision+1,
		service,
		[]testkeyvalue.Versioned[testservices.ServiceRecord]{service},
	)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("attachDesiredHeadConditions() error = %v, want state conflict", err)
	}
}

type attachTestStore struct {
	*memoryHierarchyStore
}

type attachBackupExclusionRaceStore struct {
	*attachTestStore
	exclusionKey   string
	exclusionValue []byte
	injected       bool
}

type attachIncomingGrantRaceStore struct {
	*attachTestStore
	prefix   string
	key      string
	value    []byte
	injected bool
}

type attachDetachOperationCaptureStore struct {
	*attachTestStore
	attachID   string
	operations int
}

func (store *attachDetachOperationCaptureStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	for _, mutation := range mutations {
		if mutation.Type == testkeyvalue.MutationPut && mutation.Key == testattachments.AttachKey(store.attachID) {
			store.operations = len(conditions) + len(mutations)
			break
		}
	}
	return store.attachTestStore.Transact(ctx, conditions, mutations)
}

func (store *attachIncomingGrantRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if !store.injected {
		for _, condition := range conditions {
			if !condition.Prefix || condition.Key != store.prefix {
				continue
			}
			store.injected = true
			raced, err := store.memoryHierarchyStore.Transact(ctx, nil, []testkeyvalue.Mutation{{
				Type: testkeyvalue.MutationPut, Key: store.key, Value: store.value,
			}})
			if err != nil {
				return testkeyvalue.TransactionResult{}, err
			}
			return testkeyvalue.TransactionResult{Revision: raced.Revision}, nil
		}
	}
	return store.attachTestStore.Transact(ctx, conditions, mutations)
}

func (store *attachBackupExclusionRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if !store.injected {
		for _, condition := range conditions {
			if condition.Key != store.exclusionKey || condition.Prefix {
				continue
			}
			store.injected = true
			if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []testkeyvalue.Mutation{{
				Type: testkeyvalue.MutationPut, Key: store.exclusionKey, Value: store.exclusionValue,
			}}); err != nil {
				return testkeyvalue.TransactionResult{}, err
			}
			break
		}
	}
	return store.attachTestStore.Transact(ctx, conditions, mutations)
}

func testAttachBackupExclusionValue(
	t *testing.T,
	environmentID string,
	attachID string,
	seed int64,
) []byte {
	t.Helper()
	at := testAttachTime.Add(time.Duration(seed) * time.Second)
	value, err := testbackupruntime.EncodeBackupSourceTargetExclusionRecord(
		testbackupruntime.BackupSourceTargetExclusionRecord{
			EnvironmentID: environmentID,
			OperationID:   ids.NewAt(ids.KindOperation, at, seed),
			TaskID:        ids.NewAt(ids.KindTask, at, seed+1),
			OperationKind: testbackupruntime.BackupOperationBackup,
			TargetKind:    testbackupruntime.BackupSourceTargetAttach,
			TargetID:      attachID,
			CreatedAt:     at,
			UpdatedAt:     at,
		},
	)
	if err != nil {
		t.Fatalf("encodeBackupSourceTargetExclusionRecord() error = %v", err)
	}
	t.Cleanup(func() { clear(value) })
	return value
}

func newAttachTestStore() *attachTestStore {
	return &attachTestStore{memoryHierarchyStore: newMemoryHierarchyStore()}
}

func (store *attachTestStore) Health(context.Context) error {
	return nil
}

func (store *attachTestStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
	)
	return result.Revision, err
}

func (store *attachTestStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: key}})
	return result.Revision, err
}

func (store *attachTestStore) Watch(context.Context, string, int64) (*testkeyvalue.WatchStream, error) {
	return nil, errs.New(errs.KindInternal, "Attach test store does not implement Watch")
}

func (store *attachTestStore) Snapshot(context.Context, io.Writer) error {
	return errs.New(errs.KindInternal, "Attach test store does not implement Snapshot")
}

func (store *attachTestStore) Close() error {
	return nil
}

type desiredServiceFixture struct {
	Service    testkeyvalue.Versioned[testservices.ServiceRecord]
	Projection testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]
	Blueprint  testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision]
	Claim      testblueprints.EnvironmentBlueprintStageClaim
}
