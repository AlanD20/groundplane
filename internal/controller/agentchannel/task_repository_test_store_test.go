package agentchannel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testidempotencyowner "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

type blockingClaimTaskStore struct {
	channelStartupStore
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
	maximum int32,
) (etcd.TaskAssignment, bool, error) {
	assignment, found, err := store.repository.ClaimNextTask(ctx, agentID, generation, at, maximum)
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
