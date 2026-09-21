package agentchannel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testidempotencyowner "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

type blockingClaimTaskStore struct {
	repository *etcd.TaskRepository
	entered    chan<- struct{}
	release    <-chan struct{}
}

func (store *blockingClaimTaskStore) ListAgentAssignments(
	ctx context.Context,
	agentID string,
	generation uint64,
	maximum int32,
) ([]etcd.TaskAssignment, error) {
	return store.repository.ListAgentAssignments(ctx, agentID, generation, maximum)
}

func (store *blockingClaimTaskStore) ClaimNextTask(
	ctx context.Context,
	agentID string,
	generation uint64,
	at time.Time,
) (etcd.TaskAssignment, bool, error) {
	assignment, found, err := store.repository.ClaimNextTask(ctx, agentID, generation, at)
	if err != nil || !found {
		return assignment, found, err
	}
	select {
	case store.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return etcd.TaskAssignment{}, false, ctx.Err()
	case <-store.release:
		return assignment, true, nil
	}
}

func (store *blockingClaimTaskStore) GetTask(
	ctx context.Context,
	taskID string,
) (testkeyvalue.Versioned[etcd.TaskRecord], error) {
	return store.repository.GetTask(ctx, taskID)
}

func (store *blockingClaimTaskStore) ListTaskEvents(
	ctx context.Context,
	taskID string,
	revision int64,
) (etcd.TaskEventSnapshot, error) {
	return store.repository.ListTaskEvents(ctx, taskID, revision)
}

func (store *blockingClaimTaskStore) AppendTaskEvent(
	ctx context.Context,
	input testtaskjournal.TaskEventInput,
	at time.Time,
) (etcd.TaskEventAppend, error) {
	return store.repository.AppendTaskEvent(ctx, input, at)
}

func (store *blockingClaimTaskStore) AcknowledgeTask(
	ctx context.Context,
	agentID string,
	generation uint64,
	taskID string,
	assignmentID string,
	status testtaskjournal.TaskStatus,
	result testtaskjournal.TaskResultRecord,
	at time.Time,
) (testkeyvalue.Versioned[etcd.TaskRecord], error) {
	return store.repository.AcknowledgeTask(
		ctx,
		agentID,
		generation,
		taskID,
		assignmentID,
		status,
		result,
		at,
	)
}

func newChannelTaskRepository(t *testing.T, task etcd.TaskRecord) *etcd.TaskRepository {
	t.Helper()
	repository, err := etcd.NewTaskRepository(newChannelMemoryStore())
	if err != nil {
		t.Fatalf("NewTaskRepository() error = %v", err)
	}
	ciphertext := []byte("protected-channel-task")
	digest := sha256.Sum256(ciphertext)
	marker := testidempotencyowner.IdempotencyMarker{
		Kind: testidempotencyowner.IdempotencyMarkerTask, State: testidempotencyowner.IdempotencyMarkerPending,
		Locator: testidempotencyowner.IdempotencyLocator{
			ScopeKind: testidempotencyowner.IdempotencyScopePlatform,
			ScopeID:   "-",
			Method:    http.MethodPost,
			Route:     "/platform/tasks",
			Key:       task.IdempotencyKey,
		},
		Intent: testidempotencyowner.ProtectedIntentRecord{
			EnvelopeVersion:  1,
			Cipher:           "age-x25519",
			DigestAlgorithm:  "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]),
			Ciphertext:       ciphertext,
		},
		Response: testidempotencyowner.IdempotencyResponse{
			Status:      http.StatusAccepted,
			ContentKind: "application/json",
			Body:        []byte(`{"task_id":"` + task.ID + `"}`),
		},
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	result, err := repository.CreateTask(context.Background(), task, marker)
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
		t.Fatalf("CreateTask() outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	return repository
}

type channelMemoryStore struct {
	mu       sync.Mutex
	revision int64
	values   map[string]testkeyvalue.KeyValue
}

func newChannelMemoryStore() *channelMemoryStore {
	return &channelMemoryStore{values: make(map[string]testkeyvalue.KeyValue)}
}

func (store *channelMemoryStore) Health(context.Context) error { return nil }

func (store *channelMemoryStore) Get(ctx context.Context, key string) (*testkeyvalue.GetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return &testkeyvalue.GetResult{Entry: store.valueLocked(key), ReadRevision: store.revision}, nil
}

func (store *channelMemoryStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	values := make([]*testkeyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		values[index] = store.valueLocked(key)
	}
	readRevision := request.Revision
	if readRevision == 0 {
		readRevision = store.revision
	}
	return &testkeyvalue.GetManyResult{
		Values: values, ReadRevision: readRevision, ResponseRevision: store.revision,
	}, nil
}

func (store *channelMemoryStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
	)
	return result.Revision, err
}

func (store *channelMemoryStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: key}})
	return result.Revision, err
}

func (store *channelMemoryStore) Range(
	ctx context.Context,
	request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := make([]string, 0)
	for key := range store.values {
		if !strings.HasPrefix(key, request.Prefix) {
			continue
		}
		if request.StartExclusive != "" &&
			((!request.Descending && key <= request.StartExclusive) ||
				(request.Descending && key >= request.StartExclusive)) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if request.Descending {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	}
	more := int64(len(keys)) > request.Limit
	if more {
		keys = keys[:request.Limit]
	}
	values := make([]testkeyvalue.KeyValue, 0, len(keys))
	for _, key := range keys {
		values = append(values, *store.valueLocked(key))
	}
	readRevision := request.Revision
	if readRevision == 0 {
		readRevision = store.revision
	}
	return &testkeyvalue.RangeResult{
		Values: values, ReadRevision: readRevision, ResponseRevision: store.revision, More: more,
	}, nil
}

func (store *channelMemoryStore) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	return etcd.MeasureTransactionBudget(ctx, "/groundplane/", conditions, mutations)
}

func (store *channelMemoryStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if err := ctx.Err(); err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, condition := range conditions {
		value := store.conditionValueLocked(condition)
		actual := int64(0)
		if value != nil {
			actual = value.ModRevision
		}
		if actual != condition.ModRevision {
			failureReads := make([]*testkeyvalue.KeyValue, len(conditions))
			for index, candidate := range conditions {
				failureReads[index] = store.conditionValueLocked(candidate)
			}
			return testkeyvalue.TransactionResult{
				Revision: store.revision, FailureReads: failureReads,
			}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		switch mutation.Type {
		case testkeyvalue.MutationPut:
			current := store.values[mutation.Key]
			store.values[mutation.Key] = testkeyvalue.KeyValue{
				Key: mutation.Key, Value: append([]byte(nil), mutation.Value...),
				Version: current.Version + 1, ModRevision: store.revision,
			}
		case testkeyvalue.MutationDelete:
			if mutation.Prefix {
				for key := range store.values {
					if strings.HasPrefix(key, mutation.Key) {
						delete(store.values, key)
					}
				}
			} else {
				delete(store.values, mutation.Key)
			}
		default:
			return testkeyvalue.TransactionResult{}, errors.New("unexpected transaction mutation")
		}
	}
	return testkeyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *channelMemoryStore) Watch(
	context.Context, string,

	int64,
) (*testkeyvalue.WatchStream, error) {
	return nil, errors.New("unexpected Watch")
}

func (store *channelMemoryStore) Snapshot(context.Context, io.Writer) error {
	return errors.New("unexpected Snapshot")
}

func (store *channelMemoryStore) Close() error { return nil }

func (store *channelMemoryStore) valueLocked(key string) *testkeyvalue.KeyValue {
	value, ok := store.values[key]
	if !ok {
		return nil
	}
	value.Value = append([]byte(nil), value.Value...)
	return &value
}

func (store *channelMemoryStore) conditionValueLocked(condition testkeyvalue.Condition) *testkeyvalue.KeyValue {
	if !condition.Prefix {
		return store.valueLocked(condition.Key)
	}
	keys := make([]string, 0)
	for key := range store.values {
		if strings.HasPrefix(key, condition.Key) {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	return store.valueLocked(keys[0])
}
