package workdispatch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type recoveryStore struct {
	mu          sync.Mutex
	values      map[string]keyvalue.KeyValue
	unavailable bool
	mutations   int
	revision    int64
	failPrefix  string
	failOnce    bool
	outage      chan struct{}
	recover     chan struct{}
	resumed     chan struct{}
	outageOnce  sync.Once
	resumeOnce  sync.Once
}

func (store *recoveryStore) GetMany(
	ctx context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.unavailable {
		return nil, errs.New(errs.KindStorageUnavailable, "injected storage outage")
	}
	values := make([]*keyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		value, found := store.values[key]
		if !found {
			continue
		}
		copy := value
		copy.Value = append([]byte(nil), value.Value...)
		values[index] = &copy
	}
	readRevision := request.Revision
	if readRevision == 0 {
		readRevision = store.revision
	}
	return &keyvalue.GetManyResult{
		Values: values, ReadRevision: readRevision, ResponseRevision: store.revision,
	}, nil
}

func (store *recoveryStore) Range(
	ctx context.Context,
	request keyvalue.RangeRequest,
) (*keyvalue.RangeResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if request.Prefix == store.failPrefix {
		if store.failOnce {
			store.failOnce = false
			store.outageOnce.Do(func() { close(store.outage) })
			return nil, errs.New(errs.KindStorageUnavailable, "injected loop outage")
		}
		store.resumeOnce.Do(func() { close(store.resumed) })
	}
	readRevision := request.Revision
	if readRevision == 0 {
		readRevision = store.revision
	}
	return &keyvalue.RangeResult{ReadRevision: readRevision, ResponseRevision: store.revision}, nil
}

func (*recoveryStore) MeasureTransaction(
	_ context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionBudget, error) {
	return keyvalue.TransactionBudget{Operations: len(conditions) + len(mutations)}, nil
}

func (store *recoveryStore) Transact(
	ctx context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	if err := ctx.Err(); err != nil {
		return keyvalue.TransactionResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, condition := range conditions {
		current := store.values[condition.Key]
		if current.ModRevision != condition.ModRevision {
			return keyvalue.TransactionResult{Revision: store.revision}, nil
		}
	}
	store.mutations++
	store.revision++
	for _, mutation := range mutations {
		switch mutation.Type {
		case keyvalue.MutationPut:
			current := store.values[mutation.Key]
			store.values[mutation.Key] = keyvalue.KeyValue{
				Key: mutation.Key, Value: append([]byte(nil), mutation.Value...),
				ModRevision: store.revision, Version: current.Version + 1,
			}
		case keyvalue.MutationDelete:
			delete(store.values, mutation.Key)
		}
	}
	return keyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *recoveryStore) Health(ctx context.Context) error {
	if store.recover == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-store.recover:
		return nil
	}
}

func TestRecoveryRetainsExactControllerIdentityWithoutWriting(t *testing.T) {
	identity := workexecution.Identity{Executor: workexecution.Controller, Generation: 6}
	store := &recoveryStore{values: make(map[string]keyvalue.KeyValue), unavailable: true, revision: 10}
	setRecoveryExecutor(t, store, workqueue.ExecutorRecord{
		Identity: identity, Ready: true, Capacity: 1,
		Operations: []workexecution.Operation{workexecution.Script},
	})
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		queue: queue,
		controller: &ControllerExecutor{
			queue: queue, forward: NewForwardGate(), owners: make(map[workexecution.Operation]ControllerOperation),
		},
	}
	if _, err := runtime.reconcileCurrent(t.Context(), identity); !storageUnavailable(err) {
		t.Fatalf("storage outage was not retained: %v", err)
	}
	store.unavailable = false
	if issues, err := runtime.reconcileCurrent(t.Context(), identity); err != nil || len(issues) != 0 {
		t.Fatalf("same executor did not recover: issues=%v err=%v", issues, err)
	}
	replacement := identity
	replacement.Generation++
	setRecoveryExecutor(t, store, workqueue.ExecutorRecord{
		Identity: replacement, Ready: true, Capacity: 1,
		Operations: []workexecution.Operation{workexecution.Script},
	})
	if _, err := runtime.reconcileCurrent(t.Context(), identity); err == nil {
		t.Fatal("replacement executor was accepted as the retained recovery owner")
	}
	if store.mutations != 0 {
		t.Fatalf("read-only recovery wrote %d transactions", store.mutations)
	}
}

func TestStorageRecoveryRejectsMixedPermanentLoopFailure(t *testing.T) {
	storage := errs.New(errs.KindStorageUnavailable, "injected outage")
	nested := errs.Wrap(errs.KindInternal, storage)
	if !recoverableStorageFailure(nested) {
		t.Fatal("nested typed storage outage was not recoverable")
	}
	mixed := errs.WrapJoined(
		errs.KindInternal,
		storage,
		errs.New(errs.KindStateConflict, "injected permanent loop failure"),
	)
	if recoverableStorageFailure(mixed) {
		t.Fatal("permanent loop failure was hidden by a joined storage outage")
	}
	if cancellationOnly(mixed) {
		t.Fatal("mixed permanent failure was mistaken for sibling cancellation")
	}
}

type recoveryOwnership struct {
	mu     sync.Mutex
	checks int
}

func (ownership *recoveryOwnership) Check() error {
	ownership.mu.Lock()
	defer ownership.mu.Unlock()
	ownership.checks++
	return nil
}

func (ownership *recoveryOwnership) count() int {
	ownership.mu.Lock()
	defer ownership.mu.Unlock()
	return ownership.checks
}

type recoveryRetention struct{}

func (recoveryRetention) Run(ctx context.Context, _ time.Duration, _ func(string, error)) error {
	<-ctx.Done()
	return ctx.Err()
}

type recoveryOperation struct {
	mu             sync.Mutex
	executeCalls   int
	reconcileCalls int
	settleCalls    int
}

func (*recoveryOperation) AuthorizeStart(context.Context, workqueue.Delivery) (workqueue.Change, error) {
	return workqueue.Change{}, errs.New(errs.KindInternal, "unexpected fresh Start")
}
func (*recoveryOperation) RejectUnstarted(context.Context, workqueue.Delivery) error {
	return errs.New(errs.KindInternal, "unexpected unstarted rejection")
}
func (owner *recoveryOperation) Execute(context.Context, workqueue.Delivery) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.executeCalls++
	return nil
}
func (owner *recoveryOperation) Reconcile(context.Context, workqueue.Delivery) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.reconcileCalls++
	return nil
}
func (owner *recoveryOperation) Settle(
	context.Context,
	*workqueue.Repository,
	workqueue.Delivery,
	time.Time,
) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.settleCalls++
	if owner.settleCalls == 1 {
		return errs.New(errs.KindStorageUnavailable, "injected ambiguous settlement")
	}
	return nil
}

func (owner *recoveryOperation) counts() (int, int, int) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return owner.executeCalls, owner.reconcileCalls, owner.settleCalls
}

func TestRuntimeSupervisesStorageOutageWithoutReplayingStartedExecution(t *testing.T) {
	identity := workexecution.Identity{Executor: workexecution.Controller, Generation: 6}
	store := &recoveryStore{
		values: make(map[string]keyvalue.KeyValue), revision: 10,
		failPrefix: "/v1/execution/waiting-input/", failOnce: true,
		outage: make(chan struct{}), recover: make(chan struct{}), resumed: make(chan struct{}),
	}
	seedStartedRecovery(t, store, identity)
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	forward := NewForwardGate()
	owner := &recoveryOperation{executeCalls: 1}
	scheduler := &Scheduler{queue: queue, forward: forward, owners: make(map[admissionKey]Admission)}
	controller := &ControllerExecutor{
		queue: queue, forward: forward,
		owners: map[workexecution.Operation]ControllerOperation{workexecution.Script: owner},
	}
	diagnostics, err := executiondiagnostics.New(store, queue)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(
		queue,
		store,
		forward,
		scheduler,
		controller,
		&Completions{queue: queue, owners: make(map[completionKey]CompletionOwner)},
		&Publications{queue: queue, owners: make(map[workexecution.Action]PublicationOwner)},
		&Inputs{queue: queue, owners: make(map[inputKind]InputOwner)},
		recoveryRetention{},
		diagnostics,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	ownership := &recoveryOwnership{}
	result := make(chan error, 1)
	go func() {
		result <- runtime.Run(ctx, ownership, identity, RuntimeConfig{
			CollectionInterval: time.Second, HistoryRetention: workexecution.TaskHistoryRetention,
			DiagnosticRetention: time.Hour, CollectionBudget: time.Second,
		})
	}()
	select {
	case <-store.outage:
	case <-time.After(5 * time.Second):
		t.Fatal("execution loop did not observe the storage outage")
	}
	select {
	case err := <-result:
		t.Fatalf("transient storage outage stopped the runtime: %v", err)
	default:
	}
	store.mu.Lock()
	before := store.mutations
	record, decodeErr := workqueue.DecodeExecutor(store.values[workqueue.ExecutorKey(identity)].Value)
	store.mu.Unlock()
	if decodeErr != nil || record.Identity != identity || before != 0 {
		t.Fatalf("outage replaced or mutated the executor: record=%+v writes=%d err=%v", record, before, decodeErr)
	}
	close(store.recover)
	select {
	case <-store.resumed:
	case <-time.After(5 * time.Second):
		t.Fatal("execution loops did not resume after storage recovery")
	}
	executed, reconciled, settled := owner.counts()
	if executed != 1 || reconciled < 2 || settled < 2 || ownership.count() < 3 {
		t.Fatalf("retained execution or lease was not reconciled: execute=%d reconcile=%d settle=%d lease_checks=%d",
			executed, reconciled, settled, ownership.count())
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runtime shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not stop after root cancellation")
	}
}

func seedStartedRecovery(t *testing.T, store *recoveryStore, identity workexecution.Identity) {
	t.Helper()
	created := time.Date(2026, 11, 22, 18, 0, 0, 0, time.UTC)
	assigned, started := created.Add(time.Second), created.Add(2*time.Second)
	deadline := created.Add(time.Hour)
	taskID, workID, attemptID := ids.New(ids.KindTask), ids.New(ids.KindWorkItem), ids.New(ids.KindAttempt)
	targetID, planID := ids.New(ids.KindService), ids.New(ids.KindPlan)
	input := workexecution.Input{ID: planID, SHA256: strings.Repeat("a", 64)}
	task := workexecution.Task{
		ID: taskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionRun,
		TargetID: targetID, TargetName: "retained execution", Intent: input,
		CreatedAt: created, UpdatedAt: started, Deadline: deadline, ExpectedItems: 1,
		PublicationComplete: true, Progress: workexecution.Progress{Running: 1},
	}
	item := workexecution.Item{
		ID: workID, TaskID: taskID, Operation: workexecution.Script, TargetID: targetID,
		Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims:    []workexecution.Claim{{ScopeID: targetID}}, Input: &input,
		Phase: workexecution.Running, AttemptNumber: 1, ActiveAttemptID: attemptID,
		Cleanup: workexecution.CleanupNone, CreatedAt: created, UpdatedAt: started, Deadline: deadline,
	}
	attempt := workexecution.Attempt{
		ID: attemptID, WorkItemID: workID, TaskID: taskID, Number: 1,
		Executor: identity, Direction: workexecution.Forward, Input: input,
		AssignedAt: assigned, Deadline: deadline, StartedAt: &started,
	}
	putRecoveryRecord(t, store, workqueue.TaskKey(taskID), func() ([]byte, error) {
		return workqueue.EncodeTask(task)
	})
	putRecoveryRecord(t, store, workqueue.ItemKey(workID), func() ([]byte, error) {
		return workqueue.EncodeItem(item)
	})
	putRecoveryRecord(t, store, workqueue.AttemptKey(attemptID), func() ([]byte, error) {
		return workqueue.EncodeAttempt(attempt)
	})
	putRecoveryRecord(t, store, workqueue.TaskItemKey(taskID, workID), func() ([]byte, error) {
		return recordcodec.Encode("execution_owner", workqueue.ClaimOwner{TaskID: taskID, WorkItemID: workID})
	})
	putRecoveryRecord(t, store, workqueue.SlotKey(identity, 0), func() ([]byte, error) {
		return recordcodec.Encode("execution_slot", workqueue.Slot{
			TaskID: taskID, WorkItemID: workID, AttemptID: attemptID,
		})
	})
	setRecoveryExecutor(t, store, workqueue.ExecutorRecord{
		Identity: identity, Ready: true, Capacity: 1,
		Operations: []workexecution.Operation{workexecution.Script},
	})
}

func putRecoveryRecord(t *testing.T, store *recoveryStore, key string, encode func() ([]byte, error)) {
	t.Helper()
	value, err := encode()
	if err != nil {
		t.Fatal(err)
	}
	store.values[key] = keyvalue.KeyValue{Key: key, Value: value, ModRevision: 4, Version: 1}
}

func setRecoveryExecutor(t *testing.T, store *recoveryStore, record workqueue.ExecutorRecord) {
	t.Helper()
	encoded, err := workqueue.EncodeExecutor(record)
	if err != nil {
		t.Fatal(err)
	}
	store.values[workqueue.ExecutorKey(record.Identity)] = keyvalue.KeyValue{
		Key: workqueue.ExecutorKey(record.Identity), Value: encoded, ModRevision: 4, Version: 1,
	}
}
