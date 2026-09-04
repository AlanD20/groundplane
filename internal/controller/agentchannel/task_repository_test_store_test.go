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
) (etcd.Versioned[etcd.TaskRecord], error) {
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
	input etcd.TaskEventInput,
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
	status etcd.TaskStatus,
	result etcd.TaskResultRecord,
	at time.Time,
) (etcd.Versioned[etcd.TaskRecord], error) {
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
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: etcd.IdempotencyLocator{
			ScopeKind: etcd.IdempotencyScopePlatform,
			ScopeID:   "-",
			Method:    http.MethodPost,
			Route:     "/platform/tasks",
			Key:       task.IdempotencyKey,
		},
		Intent: etcd.ProtectedIntentRecord{
			EnvelopeVersion:  1,
			Cipher:           "age-x25519",
			DigestAlgorithm:  "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]),
			Ciphertext:       ciphertext,
		},
		Response: etcd.IdempotencyResponse{
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
	values   map[string]etcd.KeyValue
}

func newChannelMemoryStore() *channelMemoryStore {
	return &channelMemoryStore{values: make(map[string]etcd.KeyValue)}
}

func (store *channelMemoryStore) Health(context.Context) error { return nil }

func (store *channelMemoryStore) Get(ctx context.Context, key string) (*etcd.GetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return &etcd.GetResult{Entry: store.valueLocked(key), ReadRevision: store.revision}, nil
}

func (store *channelMemoryStore) GetMany(
	ctx context.Context,
	request etcd.GetManyRequest,
) (*etcd.GetManyResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	values := make([]*etcd.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		values[index] = store.valueLocked(key)
	}
	readRevision := request.Revision
	if readRevision == 0 {
		readRevision = store.revision
	}
	return &etcd.GetManyResult{
		Values: values, ReadRevision: readRevision, ResponseRevision: store.revision,
	}, nil
}

func (store *channelMemoryStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(ctx, nil, []etcd.Mutation{{Type: etcd.MutationPut, Key: key, Value: value}})
	return result.Revision, err
}

func (store *channelMemoryStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []etcd.Mutation{{Type: etcd.MutationDelete, Key: key}})
	return result.Revision, err
}

func (store *channelMemoryStore) Range(
	ctx context.Context,
	request etcd.RangeRequest,
) (*etcd.RangeResult, error) {
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
	values := make([]etcd.KeyValue, 0, len(keys))
	for _, key := range keys {
		values = append(values, *store.valueLocked(key))
	}
	readRevision := request.Revision
	if readRevision == 0 {
		readRevision = store.revision
	}
	return &etcd.RangeResult{
		Values: values, ReadRevision: readRevision, ResponseRevision: store.revision, More: more,
	}, nil
}

func (store *channelMemoryStore) Transact(
	ctx context.Context,
	conditions []etcd.Condition,
	mutations []etcd.Mutation,
) (etcd.TransactionResult, error) {
	if err := ctx.Err(); err != nil {
		return etcd.TransactionResult{}, err
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
			failureReads := make([]*etcd.KeyValue, len(conditions))
			for index, candidate := range conditions {
				failureReads[index] = store.conditionValueLocked(candidate)
			}
			return etcd.TransactionResult{
				Revision: store.revision, FailureReads: failureReads,
			}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		switch mutation.Type {
		case etcd.MutationPut:
			current := store.values[mutation.Key]
			store.values[mutation.Key] = etcd.KeyValue{
				Key: mutation.Key, Value: append([]byte(nil), mutation.Value...),
				Version: current.Version + 1, ModRevision: store.revision,
			}
		case etcd.MutationDelete:
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
			return etcd.TransactionResult{}, errors.New("unexpected transaction mutation")
		}
	}
	return etcd.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *channelMemoryStore) Watch(
	context.Context,
	string,
	int64,
) (*etcd.WatchStream, error) {
	return nil, errors.New("unexpected Watch")
}

func (store *channelMemoryStore) Snapshot(context.Context, io.Writer) error {
	return errors.New("unexpected Snapshot")
}

func (store *channelMemoryStore) Close() error { return nil }

func (store *channelMemoryStore) valueLocked(key string) *etcd.KeyValue {
	value, ok := store.values[key]
	if !ok {
		return nil
	}
	value.Value = append([]byte(nil), value.Value...)
	return &value
}

func (store *channelMemoryStore) conditionValueLocked(condition etcd.Condition) *etcd.KeyValue {
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
