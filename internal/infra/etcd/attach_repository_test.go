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
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: scoped reads must retain stable bindings and return encrypted facts without interpreting execution history.
func TestAttachRepositoryReadsScopedMetadataAndEncryptedFacts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 31, "api-db", nil)

	created := seedAttachMetadata(t, repository, record, &facts)
	if created.Revision == 0 || created.Record.ID != record.ID {
		t.Fatalf("seeded Attach metadata = %#v", created)
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
	target := seedAttachMetadata(t, repository, targetRecord, &targetFacts)
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
	seedAttachMetadata(t, repository, sourceRecord, &sourceFacts)
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
	current := seedAttachMetadata(t, repository, record, &facts)
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
	ready := seedAttachMetadata(t, repository, record, &facts)
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
