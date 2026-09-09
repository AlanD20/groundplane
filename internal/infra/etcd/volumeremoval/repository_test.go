package volumeremoval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestEnvironmentVolumeRemovalRuntimeCodecIsDeterministicBinary(t *testing.T) {
	t.Parallel()
	_, _, runtime, _, _ := environmentVolumeRemovalRuntimeFixture(t)
	encoded, err := removalrecord.EncodeRuntime(runtime)
	if err != nil {
		t.Fatal(err)
	}
	second, err := removalrecord.EncodeRuntime(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded[:4]) != "GVRR" || strings.Contains(string(encoded), `"operation_id"`) ||
		!bytesEqual(encoded, second) {
		t.Fatalf("runtime is not deterministic binary: %x", encoded)
	}
	decoded, err := removalrecord.DecodeRuntime(encoded)
	if err != nil || !sameEnvironmentVolumeRemovalRuntime(decoded, runtime) {
		t.Fatalf("runtime round trip = %#v, %v", decoded, err)
	}
	encoded = append(encoded, 0)
	if _, err := removalrecord.DecodeRuntime(encoded); err == nil {
		t.Fatal("removalrecord.DecodeRuntime() accepted corrupt state")
	}
}

func TestEnvironmentVolumeRemovalPathResumesPendingCallAndDeduplicatesCompletion(t *testing.T) {
	ctx := context.Background()
	store, repository, runtime, task, marker := environmentVolumeRemovalRuntimeFixture(t)
	persistEnvironmentVolumeRemovalTaskAndMarker(t, store, task, marker)
	created, err := repository.Create(ctx, runtime, task)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.Runtime.Record.Checkpoint != removalrecord.IntentSealed {
		t.Fatalf("created checkpoint = %v", created.Runtime.Record.Checkpoint)
	}
	if _, err := repository.AdvanceCheckpoint(
		ctx, runtime.OperationID, removalrecord.RevisionStaged,
		runtime.CreatedAt.Add(time.Second),
	); err != nil {
		t.Fatalf("AdvanceCheckpoint(revision staged) error = %v", err)
	}
	if _, err := repository.AdvanceCheckpoint(
		ctx, runtime.OperationID, removalrecord.DesiredPublished,
		runtime.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AdvanceCheckpoint(desired published) error = %v", err)
	}
	assignment := assignEnvironmentVolumeRemovalTask(
		t, store, task.ID, runtime.OperationID, runtime.CreatedAt.Add(3*time.Second),
	)
	if _, err := repository.MarkConsumersDetached(
		ctx, assignment, runtime.CreatedAt.Add(4*time.Second),
	); err != nil {
		t.Fatalf("MarkConsumersDetached() error = %v", err)
	}
	pending, existing, err := repository.BeginPathCall(
		ctx, assignment, runtime.CreatedAt.Add(5*time.Second),
	)
	if err != nil || existing || pending.Record.RequestOrdinal != 1 ||
		pending.Record.MutationBudget != removalrecord.MutationBudget {
		t.Fatalf("BeginPathCall() = %#v/%v/%v", pending, existing, err)
	}

	restarted, err := newEnvironmentVolumeRemovalRuntimeRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := restarted.Resume(ctx, runtime.OperationID)
	if err != nil || resumed.Pending == nil ||
		resumed.Pending.Record.RequestSHA256 != pending.Record.RequestSHA256 {
		t.Fatalf("Resume() = %#v, %v", resumed, err)
	}
	redelivered, existing, err := restarted.BeginPathCall(
		ctx, assignment, runtime.CreatedAt.Add(6*time.Second),
	)
	if err != nil || !existing || redelivered.Record.RequestSHA256 != pending.Record.RequestSHA256 ||
		!redelivered.Record.CreatedAt.Equal(pending.Record.CreatedAt) {
		t.Fatalf("BeginPathCall(restart) = %#v/%v/%v", redelivered, existing, err)
	}

	firstCompletion := removalrecord.Completion{
		OperationID: runtime.OperationID, RequestOrdinal: 1,
		RequestSHA256: pending.Record.RequestSHA256,
		MutationCount: 12, NextComponentStack: []string{"nested"},
		NextCursor: []byte("cursor-1"), CompletedAt: runtime.CreatedAt.Add(7 * time.Second),
	}
	firstCompletion.ResponseBytes = removalrecord.PathResponseBytes(firstCompletion)
	firstCompletion.ResponseSHA256 = removalrecord.PathResponseDigest(firstCompletion)
	firstResult := environmentVolumeRemovalPathResult(assignment, firstCompletion)
	progress, duplicate, err := restarted.CompletePathCall(ctx, firstResult)
	if err != nil || duplicate || progress.Record.NextRequestOrdinal != 2 ||
		progress.Record.DirectoryAbsent {
		t.Fatalf("CompletePathCall() = %#v/%v/%v", progress, duplicate, err)
	}
	replayed, duplicate, err := restarted.CompletePathCall(ctx, firstResult)
	if err != nil || !duplicate || replayed.Record.NextRequestOrdinal != 2 {
		t.Fatalf("CompletePathCall(replay) = %#v/%v/%v", replayed, duplicate, err)
	}

	secondPending, existing, err := restarted.BeginPathCall(
		ctx, assignment, runtime.CreatedAt.Add(8*time.Second),
	)
	if err != nil || existing || secondPending.Record.RequestOrdinal != 2 ||
		!bytesEqual(secondPending.Record.Cursor, []byte("cursor-1")) {
		t.Fatalf("BeginPathCall(second) = %#v/%v/%v", secondPending, existing, err)
	}
	secondCompletion := removalrecord.Completion{
		OperationID: runtime.OperationID, RequestOrdinal: 2,
		RequestSHA256: secondPending.Record.RequestSHA256,
		MutationCount: 1, DirectoryAbsent: true,
		CompletedAt: runtime.CreatedAt.Add(9 * time.Second),
	}
	secondCompletion.ResponseBytes = removalrecord.PathResponseBytes(secondCompletion)
	secondCompletion.ResponseSHA256 = removalrecord.PathResponseDigest(secondCompletion)
	progress, duplicate, err = restarted.CompletePathCall(
		ctx, environmentVolumeRemovalPathResult(assignment, secondCompletion),
	)
	if err != nil || duplicate || !progress.Record.DirectoryAbsent {
		t.Fatalf("CompletePathCall(absent) = %#v/%v/%v", progress, duplicate, err)
	}
	// Rationale: only the Task terminal transaction may finalize the operation;
	// advancing a runtime record alone cannot release ownership or retain replay.
	before := store.revision
	if _, err := restarted.AdvanceCheckpoint(ctx, runtime.OperationID, removalrecord.RuntimeFinalized,
		runtime.CreatedAt.Add(10*time.Second)); !isKind(err, errs.KindStateConflict) || store.revision != before {
		t.Fatalf("standalone checkpoint finalized removal: %v", err)
	}
}

func TestEnvironmentVolumeRemovalSuccessorKeepsRootReplayAndAttemptChain(t *testing.T) {
	ctx := context.Background()
	store, repository, runtime, task, marker := environmentVolumeRemovalRuntimeFixture(t)
	persistEnvironmentVolumeRemovalTaskAndMarker(t, store, task, marker)
	if _, err := repository.Create(ctx, runtime, task); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	terminal := terminalEnvironmentVolumeRemovalTask(
		t, store, task.ID, runtime.CreatedAt.Add(time.Second),
	)
	successor, err := etcd.CloneCapabilityRetryTask(
		terminal, ids.NewAt(ids.KindTask, runtime.CreatedAt, 91),
		etcd.TaskActorOperator, runtime.CreatedAt.Add(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	nextRuntime := runtime
	nextRuntime.CurrentTaskID = successor.ID
	nextRuntime.PredecessorTaskID = terminal.ID
	nextRuntime.AttemptOrdinal = 2
	nextRuntime.UpdatedAt = successor.CreatedAt
	nextAttempt := removalrecord.Attempt{
		OperationID: runtime.OperationID, OriginTaskID: runtime.OriginTaskID,
		TaskID: successor.ID, PredecessorTaskID: terminal.ID,
		Ordinal: 2, CreatedAt: successor.CreatedAt,
	}
	successor.Params = etcd.EnvironmentVolumeRemovalTaskParams(nextRuntime, nextAttempt.Ordinal)
	advanced, err := repository.PublishSuccessorAttempt(ctx, runtime.OperationID, successor)
	if err != nil {
		t.Fatalf("PublishSuccessorAttempt() error = %v", err)
	}
	if advanced.Runtime.Record.OriginTaskID != task.ID ||
		advanced.Runtime.Record.CurrentTaskID != successor.ID ||
		advanced.Runtime.Record.AttemptOrdinal != 2 ||
		advanced.Attempt.PredecessorTaskID != task.ID {
		t.Fatalf("successor state = %#v", advanced)
	}
	originTaskID, found, err := repository.ReplayRootResponse(
		ctx, runtime.OperationID, volumeRemovalRootLocator(runtime), runtime.IntentSHA256,
	)
	if err != nil || !found || originTaskID != task.ID {
		t.Fatalf("ReplayRootResponse() = %q/%v/%v", originTaskID, found, err)
	}
	different := sha256.Sum256([]byte("different protected delete intent"))
	if _, _, err := repository.ReplayRootResponse(
		ctx, runtime.OperationID, volumeRemovalRootLocator(runtime), different,
	); !isKind(err, errs.KindIdempotencyMismatch) {
		t.Fatalf("ReplayRootResponse(mismatch) error = %v", err)
	}
	stored, err := store.Get(ctx, etcd.CapabilityTaskKey(successor.ID))
	if err != nil || stored.Entry == nil {
		t.Fatalf("successor Task = %#v, %v", stored, err)
	}
}

func environmentVolumeRemovalRuntimeFixture(
	t *testing.T,
) (*memoryHierarchyStore, *EnvironmentVolumeRemovalRuntimeRepository, removalrecord.Runtime, etcd.TaskRecord, etcd.IdempotencyMarker) {
	t.Helper()
	store := newMemoryHierarchyStore()
	repository, err := newEnvironmentVolumeRemovalRuntimeRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 26, 18, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 10)
	projectID := ids.NewAt(ids.KindProject, now, 11)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 12)
	volumeID := ids.NewAt(ids.KindVolume, now, 13)
	operationID := ids.NewAt(ids.KindOperation, now, 14)
	taskID := ids.NewAt(ids.KindTask, now, 15)
	stepID := ids.NewAt(ids.KindStep, now, 16)
	task := validVolumeRemovalTask(now)
	task.ID = taskID
	task.OperationID = operationID
	task.Owner = etcd.TaskOwner{
		WorkspaceType: etcd.TaskWorkspaceTenant, TenantID: tenantID,
		ProjectID: projectID, EnvironmentID: environmentID,
	}
	task.Type = etcd.TaskRemove
	task.Target = volumeID
	task.TimeoutSeconds = removalrecord.TimeoutSeconds
	task.Steps = []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: stepID}}
	task.IdempotencyKey = "volume-remove-idempotency-key-0001"
	marker := pendingVolumeRemovalMarker(task)
	marker.Locator = etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodDelete, Route: "/volumes/{id}", Key: task.IdempotencyKey,
	}
	marker.TaskID = task.ID
	marker.Response.Status = http.StatusAccepted
	rootResponseSHA256 := sha256.Sum256(marker.Response.Body)
	runtime := removalrecord.Runtime{
		OperationID: operationID, EnvironmentID: environmentID, VolumeID: volumeID,
		Key: "application_data", DesiredRevisionID: ids.NewAt(ids.KindTask, now, 17),
		DesiredGeneration:      1,
		ImpactSHA256:           sha256.Sum256([]byte("impact")),
		EvidenceManifestSHA256: sha256.Sum256([]byte("manifest")),
		IntentSHA256:           sha256.Sum256([]byte("canonical protected delete intent")),
		RootLocator: removalrecord.ReplayLocator{
			ScopeKind: string(marker.Locator.ScopeKind), ScopeID: marker.Locator.ScopeID,
			Method: marker.Locator.Method, Route: marker.Locator.Route, Key: marker.Locator.Key,
		}, RootResponseSHA256: rootResponseSHA256,
		OriginTaskID: task.ID, CurrentTaskID: task.ID, AttemptOrdinal: 1,
		StepID: stepID, Checkpoint: removalrecord.IntentSealed,
		CreatedAt: now, UpdatedAt: now,
	}
	attempt := removalrecord.Attempt{
		OperationID: operationID, OriginTaskID: task.ID, TaskID: task.ID,
		Ordinal: 1, CreatedAt: now,
	}
	task.RenderGeneration = int32(runtime.DesiredGeneration)
	task.Params = etcd.EnvironmentVolumeRemovalTaskParams(runtime, attempt.Ordinal)
	if err := validateEnvironmentVolumeRemovalTask(task, runtime, attempt); err != nil {
		t.Fatalf("fixture Task error = %v", err)
	}
	return store, repository, runtime, task, marker
}

func persistEnvironmentVolumeRemovalTaskAndMarker(
	t *testing.T,
	store *memoryHierarchyStore,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) {
	t.Helper()
	taskValue, err := etcd.EncodeCapabilityTaskRecord(task)
	if err != nil {
		t.Fatal(err)
	}
	markerValue, err := etcd.EncodeCapabilityIdempotencyMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	markerKey, err := etcd.CapabilityIdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	ownerValue, err := removalrecord.EncodeOwner(removalrecord.Owner{
		VolumeID: task.Target, EnvironmentID: task.Owner.EnvironmentID, OperationID: task.OperationID,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), []etcd.Condition{
		{Key: etcd.CapabilityTaskKey(task.ID)}, {Key: markerKey},
		{Key: removalrecord.OwnerKey(task.Target)},
		{Key: removalrecord.EnvironmentLockKey(task.Owner.EnvironmentID)},
	}, []etcd.Mutation{
		{Type: etcd.MutationPut, Key: etcd.CapabilityTaskKey(task.ID), Value: taskValue},
		{Type: etcd.MutationPut, Key: markerKey, Value: markerValue},
		{Type: etcd.MutationPut, Key: removalrecord.OwnerKey(task.Target), Value: ownerValue},
		{Type: etcd.MutationPut, Key: removalrecord.EnvironmentLockKey(task.Owner.EnvironmentID), Value: ownerValue},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("persist root Task/marker = %#v, %v", result, err)
	}
}

func assignEnvironmentVolumeRemovalTask(
	t *testing.T,
	store *memoryHierarchyStore,
	taskID string,
	operationID string,
	assignedAt time.Time,
) EnvironmentVolumeRemovalAssignment {
	t.Helper()
	ctx := context.Background()
	stored, err := store.Get(ctx, etcd.CapabilityTaskKey(taskID))
	if err != nil || stored.Entry == nil {
		t.Fatalf("pending Task = %#v, %v", stored, err)
	}
	task, err := etcd.DecodeCapabilityTaskRecord(stored.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	running, err := etcd.TransitionCapabilityTaskStatus(
		task,
		etcd.TaskStatusPending,
		etcd.TaskStatusRunning,
		assignedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.NewAt(ids.KindAgent, task.CreatedAt, 70)
	deadline := assignedAt.Add(time.Duration(task.TimeoutSeconds) * time.Second)
	assignment := etcd.TaskAssignmentRecord{
		AssignmentID: ids.NewAt(ids.KindAssignment, task.CreatedAt, 71),
		TaskID:       task.ID, Executor: etcd.TaskExecutorAgent, AgentID: agentID,
		AgentGeneration: 3, ClaimedTaskRevision: stored.Entry.ModRevision,
		AssignedAt: assignedAt, Deadline: deadline,
		RecoveryDeadline: deadline.Add(time.Duration(task.TimeoutSeconds) * time.Second),
		ExecutionMode:    etcd.TaskExecutionModeForward, ExecutionEpoch: 1,
	}
	runningValue, err := etcd.EncodeCapabilityTaskRecord(running)
	if err != nil {
		t.Fatal(err)
	}
	assignmentValue, err := etcd.EncodeCapabilityTaskAssignment(assignment)
	if err != nil {
		t.Fatal(err)
	}
	claimKey := etcd.CapabilityTaskExecutionClaimKey(etcd.TaskExecutorAgent, agentID, task.ID)
	result, err := store.Transact(ctx, []etcd.Condition{
		{Key: etcd.CapabilityTaskKey(task.ID), ModRevision: stored.Entry.ModRevision},
		{Key: claimKey}, {Key: etcd.CapabilityTaskAssignmentIndexKey(task.ID)},
		{Key: etcd.CapabilityTaskTimeoutIndexKey(task.ID, assignment.Deadline)},
	}, []etcd.Mutation{
		{Type: etcd.MutationPut, Key: etcd.CapabilityTaskKey(task.ID), Value: runningValue},
		{Type: etcd.MutationPut, Key: claimKey, Value: assignmentValue},
		{Type: etcd.MutationPut, Key: etcd.CapabilityTaskAssignmentIndexKey(task.ID), Value: assignmentValue},
		{
			Type:  etcd.MutationPut,
			Key:   etcd.CapabilityTaskTimeoutIndexKey(task.ID, assignment.Deadline),
			Value: assignmentValue,
		},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("assign Task = %#v, %v", result, err)
	}
	return EnvironmentVolumeRemovalAssignment{
		OperationID: operationID, TaskID: task.ID, AssignmentID: assignment.AssignmentID,
		AgentID: agentID, AgentGeneration: assignment.AgentGeneration,
	}
}

func terminalEnvironmentVolumeRemovalTask(
	t *testing.T,
	store *memoryHierarchyStore,
	taskID string,
	startedAt time.Time,
) etcd.TaskRecord {
	t.Helper()
	ctx := context.Background()
	stored, err := store.Get(ctx, etcd.CapabilityTaskKey(taskID))
	if err != nil || stored.Entry == nil {
		t.Fatalf("source Task = %#v, %v", stored, err)
	}
	task, err := etcd.DecodeCapabilityTaskRecord(stored.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	running, err := etcd.TransitionCapabilityTaskStatus(task, etcd.TaskStatusPending, etcd.TaskStatusRunning, startedAt)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := etcd.TransitionCapabilityTaskStatus(
		running, etcd.TaskStatusRunning, etcd.TaskStatusFailed, startedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	value, err := etcd.EncodeCapabilityTaskRecord(failed)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(ctx, []etcd.Condition{{
		Key: etcd.CapabilityTaskKey(taskID), ModRevision: stored.Entry.ModRevision,
	}}, []etcd.Mutation{{Type: etcd.MutationPut, Key: etcd.CapabilityTaskKey(taskID), Value: value}})
	if err != nil || !result.Succeeded {
		t.Fatalf("terminalize source Task = %#v, %v", result, err)
	}
	return failed
}

func environmentVolumeRemovalPathResult(
	assignment EnvironmentVolumeRemovalAssignment,
	completion removalrecord.Completion,
) EnvironmentVolumeRemovalPathResult {
	return EnvironmentVolumeRemovalPathResult{
		Assignment: assignment, RequestOrdinal: completion.RequestOrdinal,
		RequestSHA256: completion.RequestSHA256, ResponseSHA256: completion.ResponseSHA256,
		ResponseBytes: completion.ResponseBytes, MutationCount: completion.MutationCount,
		NextComponentStack: append([]string(nil), completion.NextComponentStack...),
		NextCursor:         append([]byte(nil), completion.NextCursor...),
		DirectoryAbsent:    completion.DirectoryAbsent, CompletedAt: completion.CompletedAt,
	}
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

type memoryVersion struct {
	revision int64
	value    []byte
	present  bool
}

type memoryHierarchyStore struct {
	revision int64
	history  map[string][]memoryVersion
}

func newMemoryHierarchyStore() *memoryHierarchyStore {
	return &memoryHierarchyStore{history: make(map[string][]memoryVersion)}
}

func (store *memoryHierarchyStore) Get(_ context.Context, key string) (*etcd.GetResult, error) {
	return &etcd.GetResult{Entry: store.valueAt(key, store.revision), ReadRevision: store.revision}, nil
}

func (store *memoryHierarchyStore) GetMany(
	_ context.Context,
	request etcd.GetManyRequest,
) (*etcd.GetManyResult, error) {
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	values := make([]*etcd.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		values[index] = store.valueAt(key, revision)
	}
	return &etcd.GetManyResult{Values: values, ReadRevision: revision, ResponseRevision: store.revision}, nil
}

func (store *memoryHierarchyStore) Transact(
	_ context.Context,
	conditions []etcd.Condition,
	mutations []etcd.Mutation,
) (etcd.TransactionResult, error) {
	for _, condition := range conditions {
		value := store.valueAt(condition.Key, store.revision)
		actual := int64(0)
		if value != nil {
			actual = value.ModRevision
		}
		if actual != condition.ModRevision {
			failure := make([]*etcd.KeyValue, len(conditions))
			for index, item := range conditions {
				failure[index] = store.valueAt(item.Key, store.revision)
			}
			return etcd.TransactionResult{Revision: store.revision, FailureReads: failure}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		version := memoryVersion{revision: store.revision}
		switch mutation.Type {
		case etcd.MutationPut:
			version.present = true
			version.value = append([]byte(nil), mutation.Value...)
		case etcd.MutationDelete:
		default:
			return etcd.TransactionResult{}, errs.New(errs.KindInternal, "fake store received invalid mutation")
		}
		store.history[mutation.Key] = append(store.history[mutation.Key], version)
	}
	return etcd.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *memoryHierarchyStore) valueAt(key string, revision int64) *etcd.KeyValue {
	versions := store.history[key]
	for index := len(versions) - 1; index >= 0; index-- {
		version := versions[index]
		if version.revision > revision {
			continue
		}
		if !version.present {
			return nil
		}
		keyVersion := int64(0)
		for previous := index; previous >= 0 && versions[previous].present; previous-- {
			keyVersion++
		}
		return &etcd.KeyValue{
			Key:         key,
			Value:       append([]byte(nil), version.value...),
			Version:     keyVersion,
			ModRevision: version.revision,
		}
	}
	return nil
}

func validVolumeRemovalTask(now time.Time) etcd.TaskRecord {
	return etcd.TaskRecord{
		Actor: etcd.TaskActorOperator, Executor: etcd.TaskExecutorAgent,
		PlanID: ids.NewAt(ids.KindPlan, now, 3), PlanHash: strings.Repeat("a", 64),
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
}

func pendingVolumeRemovalMarker(task etcd.TaskRecord) etcd.IdempotencyMarker {
	ciphertext := []byte("protected-task-intent")
	digest := sha256.Sum256(ciphertext)
	body, _ := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: task.ID})
	return etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: etcd.IdempotencyLocator{ScopeKind: etcd.IdempotencyScopeEnvironment,
			ScopeID: ids.NewAt(ids.KindEnvironment, task.CreatedAt, 501), Method: http.MethodPost,
			Route: "/environments/{environment}/tasks", Key: task.IdempotencyKey},
		Intent: etcd.ProtectedIntentRecord{EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext},
		Response: etcd.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: body},
		TaskID:   task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
}

func isKind(err error, kind errs.Kind) bool { return errors.Is(err, errs.New(kind, "")) }
