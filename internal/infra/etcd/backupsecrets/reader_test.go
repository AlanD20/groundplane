package backupsecrets

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	base "github.com/AlanD20/groundplane/internal/infra/etcd"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: evidence ownership ends explicitly, so every returned encrypted
// credential buffer must be released even after partial consumption.
func TestBackupSecretResolutionEvidenceClearZeroesCiphertext(t *testing.T) {
	// Rationale: the reader transfers encrypted bytes to the Controller, which
	// must be able to release every partial buffer on success or failure.
	connectorCiphertext := bytes.Repeat([]byte{0xA1}, 16)
	secretCiphertext := bytes.Repeat([]byte{0xB2}, 16)
	evidence := Evidence{
		Credentials: testconnectors.EncryptedCredentials{Ciphertext: connectorCiphertext},
		SecretValues: []SecretValueEvidence{{
			Value: testsecrets.EncryptedValue{Ciphertext: secretCiphertext},
		}},
	}
	evidence.Clear()
	if evidence.Credentials.Ciphertext != nil || evidence.SecretValues != nil {
		t.Fatal("BackupSecretResolutionEvidence.Clear() retained ciphertext")
	}
	for index, value := range connectorCiphertext {
		if value != 0 {
			t.Fatalf("connector ciphertext byte %d = %d, want zero", index, value)
		}
	}
	for index, value := range secretCiphertext {
		if value != 0 {
			t.Fatalf("Secret ciphertext byte %d = %d, want zero", index, value)
		}
	}
}

// Rationale: a duplicated sealed step identity makes assignment authority
// ambiguous and must fail before any secret evidence is read.
func TestBackupSecretStepIndexRejectsDuplicateStepIDs(t *testing.T) {
	// Rationale: a duplicated sealed step identity would make slot delivery
	// ambiguous and must fail before any durable read or decryption.
	plan := &agentpb.ExecutionPlan{Steps: []*agentpb.ExecutionStep{
		{StepId: "step_backup_01"}, {StepId: "step_backup_01"},
	}}
	if _, err := backupSecretStepIndex(plan, "step_backup_01"); err == nil {
		t.Fatal("backupSecretStepIndex() error = nil")
	}
}

// Rationale: deletion fences are typed durable authority; a mismatched target
// kind is corruption rather than evidence that resolution may proceed.
func TestRequireNoDeletionFenceRejectsWrongTargetKind(t *testing.T) {
	// Rationale: a tombstone for another resource cannot be treated as a valid
	// fence for the selected Environment or Connector.
	value, err := testdeletions.EncodeDeletionTombstone(testdeletions.DeletionTombstoneRecord{
		TargetKind:     testdeletions.DeletionTargetProject,
		TargetID:       "prj_01H00000000000000000000000",
		TargetRevision: 1,
		TaskID:         "task_01H00000000000000000000000",
		Phase:          testdeletions.DeletionPhaseFinalizing,
		CreatedAt:      time.Unix(1, 0).UTC(),
		UpdatedAt:      time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("encodeDeletionTombstone() error = %v", err)
	}
	defer clear(value)
	if err := requireNoDeletionFence(
		&testkeyvalue.KeyValue{Value: value}, testdeletions.DeletionTargetEnvironment, "env_01H00000000000000000000000",
	); err == nil {
		t.Fatal("requireNoDeletionFence() error = nil")
	}
}

// Rationale: Volume snapshots have no arbitrary Service-count cap; their
// fixed-revision reads must be split only by the accepted etcd op ceiling.
func TestBackupSecretFixedReadBatchesArbitraryVolumeServiceEvidence(t *testing.T) {
	store := &boundedBackupSecretGetManyStore{values: map[string][]byte{}}
	keys := make([]string, testkeyvalue.MaximumOperations*2+7)

	for index := range keys {
		keys[index] = "/test/backup-volume-services/" + ids.NewAt(
			ids.KindService, time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC), int64(index+1),
		)
		store.values[keys[index]] = []byte{byte(index)}
	}
	reader, err := NewReader(store)
	if err != nil {
		t.Fatal(err)
	}
	read, err := reader.readFixed(context.Background(), keys, 7)
	if err != nil {
		t.Fatalf("readFixed() error = %v", err)
	}
	defer testkeyvalue.ClearValues(read.Values)
	if len(read.Values) != len(keys) || store.calls != 3 {
		t.Fatalf("read values/calls = %d/%d, want %d/3", len(read.Values), store.calls, len(keys))
	}
}

type boundedBackupSecretGetManyStore struct {
	values map[string][]byte
	calls  int
}

func (store *boundedBackupSecretGetManyStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if len(request.Keys) > testkeyvalue.MaximumOperations || request.Revision != 7 {
		return nil, errs.New(errs.KindInternal, "fixed read exceeded its operation bound or lost its pinned revision")
	}
	store.calls++
	result := &testkeyvalue.GetManyResult{ReadRevision: 7, ResponseRevision: 7}
	for _, key := range request.Keys {
		result.Values = append(
			result.Values,
			&testkeyvalue.KeyValue{Key: key, Value: append([]byte(nil), store.values[key]...), ModRevision: 7},
		)
	}
	return result, nil
}

type secretOwnedGetManyStoreFunc func(context.Context, testkeyvalue.GetManyRequest) (*testkeyvalue.GetManyResult, error)

func (fn secretOwnedGetManyStoreFunc) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	return fn(ctx, request)
}

// Rationale: a malformed later etcd batch must clear both that response and
// every sensitive buffer already accumulated from earlier batches.
func TestBackupSecretFixedReadClearsEarlierAndMalformedLaterResponses(t *testing.T) {
	t.Parallel()

	keys := make([]string, testkeyvalue.MaximumOperations+1)
	for index := range keys {
		keys[index] = fmt.Sprintf("/secret-owned/%03d", index)
	}
	var returned [][]byte
	calls := 0
	store := secretOwnedGetManyStoreFunc(
		func(_ context.Context, request testkeyvalue.GetManyRequest) (*testkeyvalue.GetManyResult, error) {
			calls++
			values := make([]*testkeyvalue.KeyValue, len(request.Keys))
			for index, key := range request.Keys {
				value := []byte{byte(calls), byte(index + 1), 0x7f}
				returned = append(returned, value)
				values[index] = &testkeyvalue.KeyValue{Key: key, Value: value, ModRevision: int64(index + 1)}
			}
			if calls == 2 {
				values[0].Key = "/secret-owned/malformed"
			}
			return &testkeyvalue.GetManyResult{
				Values: values, ReadRevision: request.Revision, ResponseRevision: request.Revision,
			}, nil
		},
	)

	reader := &Reader{store: store}
	if _, err := reader.readFixed(context.Background(), keys, 41); err == nil {
		t.Fatal("expected malformed later batch to fail")
	}
	if calls != 2 {
		t.Fatalf("get many calls = %d, want 2", calls)
	}
	for index, value := range returned {
		for offset, octet := range value {
			if octet != 0 {
				t.Fatalf("returned buffer %d byte %d was not cleared", index, offset)
			}
		}
	}
}

// Rationale: batching is an implementation detail; every batch must retain
// the one immutable assignment revision even when current state mutates.
func TestBackupSecretFixedReadPinsEveryBatchAcrossMutation(t *testing.T) {
	t.Parallel()

	keys := make([]string, testkeyvalue.MaximumOperations+1)
	for index := range keys {
		keys[index] = fmt.Sprintf("/fixed-secret/%03d", index)
	}
	const fixedRevision int64 = 77
	snapshot := []byte("sealed-at-77")
	current := []byte("sealed-at-77")
	calls := 0
	store := secretOwnedGetManyStoreFunc(
		func(_ context.Context, request testkeyvalue.GetManyRequest) (*testkeyvalue.GetManyResult, error) {
			calls++
			if request.Revision != fixedRevision {
				t.Fatalf("batch revision = %d, want %d", request.Revision, fixedRevision)
			}
			values := make([]*testkeyvalue.KeyValue, len(request.Keys))
			for index, key := range request.Keys {
				selected := current
				if request.Revision == fixedRevision {
					selected = snapshot
				}
				values[index] = &testkeyvalue.KeyValue{
					Key:         key,
					Value:       append([]byte(nil), selected...),
					ModRevision: fixedRevision,
				}
			}
			if calls == 1 {
				current = []byte("mutated-between-batches")
			}
			return &testkeyvalue.GetManyResult{
				Values: values, ReadRevision: request.Revision, ResponseRevision: fixedRevision + 1,
			}, nil
		},
	)

	reader := &Reader{store: store}
	result, err := reader.readFixed(context.Background(), keys, fixedRevision)
	if err != nil {
		t.Fatalf("readFixed: %v", err)
	}
	defer testkeyvalue.ClearValues(result.Values)
	if calls != 2 {
		t.Fatalf("get many calls = %d, want 2", calls)
	}
	for index, value := range result.Values {
		if value == nil || string(value.Value) != string(snapshot) {
			t.Fatalf("value %d did not come from fixed revision", index)
		}
	}
}

// Rationale: Config capture seals the run's snapshot revision while current
// target evidence is independently validated at the later assignment view.
func TestBackupSecretConfigCaptureBindsSealedSnapshotAtLaterAssignmentRevision(t *testing.T) {
	t.Parallel()

	const (
		taskID             = "task-config-capture"
		environmentID      = "environment-config-capture"
		snapshotRevision   = int64(101)
		assignmentRevision = int64(303)
		targetRevision     = int64(29)
	)
	dynamic := newBackupSecretDynamicRead()
	dynamic.add(testhierarchy.EnvironmentKey(environmentID))
	result := &testkeyvalue.GetManyResult{
		ReadRevision: assignmentRevision,
		Values: []*testkeyvalue.KeyValue{{
			Key: testhierarchy.EnvironmentKey(environmentID), ModRevision: targetRevision,
		}},
	}
	run := &testbackupruntime.BackupRunRecord{TaskID: taskID, EnvironmentID: environmentID}
	source := testbackupruntime.BackupRunSourceAttemptRecord{
		Kind: testbackupruntime.BackupRuntimeSourceConfig, TargetRevision: targetRevision,
		Snapshot: testbackupruntime.BackupRunSourceSnapshot{Config: &testbackupruntime.BackupConfigSourceSnapshot{
			ConfigSnapshotID: taskID, ReadRevision: snapshotRevision,
		}},
	}
	step := &agentpb.BackupSourceCapture{Source: &agentpb.BackupSourceCapture_Config{
		Config: &agentpb.BackupConfigSource{SnapshotRevision: uint64(snapshotRevision)},
	}}
	reader := &Reader{}
	if err := reader.validateCaptureTargetEvidence(result, dynamic, run, source, step); err != nil {
		t.Fatalf("later assignment fixed revision rejected immutable config snapshot: %v", err)
	}

	step.GetConfig().SnapshotRevision++
	if err := reader.validateCaptureTargetEvidence(result, dynamic, run, source, step); err == nil {
		t.Fatal("mismatched sealed config snapshot revision was accepted")
	}
}

// Rationale: ADR0024's durable assignment deadline is an exclusive execution
// fence; reconnects at or after it must fail before timeout maintenance runs.
func TestResolveBackupSecretEvidenceAssignmentDeadlineBoundaries(t *testing.T) {
	t.Parallel()

	store, request, deadline := newDeadlineBoundaryFixture(t)
	reader, err := NewReader(store)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		offset   time.Duration
		wantKind errs.Kind
	}{
		{name: "before", offset: -time.Nanosecond, wantKind: errs.KindInternal},
		{name: "at", wantKind: errs.KindStateConflict},
		{name: "after before maintenance", offset: time.Nanosecond, wantKind: errs.KindStateConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader.now = func() time.Time { return deadline.Add(test.offset) }
			evidence, resolveErr := reader.ResolveBackupSecretEvidence(context.Background(), request)
			evidence.Clear()
			if !errors.Is(resolveErr, errs.New(test.wantKind, "")) {
				t.Fatalf("ResolveBackupSecretEvidence() error = %v, want kind %v", resolveErr, test.wantKind)
			}
		})
	}
}

type deadlineBoundaryStore struct {
	revision        int64
	taskValue       []byte
	assignmentValue []byte
}

func (store *deadlineBoundaryStore) GetMany(
	_ context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if request.Revision == 0 {
		return &testkeyvalue.GetManyResult{
			ReadRevision: store.revision, ResponseRevision: store.revision,
			Values: []*testkeyvalue.KeyValue{
				{Key: request.Keys[0], Value: append([]byte(nil), store.taskValue...), ModRevision: store.revision},
				{
					Key:         request.Keys[1],
					Value:       append([]byte(nil), store.assignmentValue...),
					ModRevision: store.revision,
				},
			},
		}, nil
	}
	values := make([]*testkeyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		switch index {
		case 0:
			values[index] = &testkeyvalue.KeyValue{
				Key:         key,
				Value:       append([]byte(nil), store.taskValue...),
				ModRevision: store.revision,
			}
		case 1, 2, 3:
			values[index] = &testkeyvalue.KeyValue{
				Key:         key,
				Value:       append([]byte(nil), store.assignmentValue...),
				ModRevision: store.revision,
			}
		case 4:
			values[index] = &testkeyvalue.KeyValue{
				Key:         key,
				Value:       []byte("corrupt-run-after-deadline-check"),
				ModRevision: store.revision,
			}
		}
	}
	return &testkeyvalue.GetManyResult{
		ReadRevision: store.revision, ResponseRevision: store.revision, Values: values,
	}, nil
}

func newDeadlineBoundaryFixture(
	t *testing.T,
) (*deadlineBoundaryStore, backupsecret.Request, time.Time) {
	t.Helper()
	createdAt := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	taskID := ids.NewAt(ids.KindTask, createdAt, 1)
	operationID := ids.NewAt(ids.KindOperation, createdAt, 2)
	planID := ids.NewAt(ids.KindPlan, createdAt, 3)
	stepID := ids.NewAt(ids.KindStep, createdAt, 4)
	environmentID := ids.NewAt(ids.KindEnvironment, createdAt, 5)
	sourceID := ids.NewAt(ids.KindBackupSource, createdAt, 6)
	pointID := ids.NewAt(ids.KindRecoveryPoint, createdAt, 7)
	connectorID := ids.NewAt(ids.KindConnector, createdAt, 8)
	agentID := ids.NewAt(ids.KindAgent, createdAt, 9)
	assignmentID := ids.NewAt(ids.KindAssignment, createdAt, 10)
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	capture := &agentpb.BackupSourceCapture{
		SourceId: sourceID, SourceRevision: 11,
		TargetId: environmentID, TargetRevision: 12,
		PointId: pointID, ConnectorId: connectorID, ConnectorRevision: 13,
		SourceFormat: agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_ENVIRONMENT_CONFIG_V1,
		Encryption:   agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
		KeyEra:       2, AgeRecipient: identity.Recipient().String(),
		Upload: &agentpb.BackupUploadAuthority{
			ConnectorEndpoint: "https://objects.example.test", ConnectorBucket: "groundplane-backups",
			ConnectorPrefix: "production/", ConnectorRegion: "auto",
			ConnectorAddressing: agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_PATH_STYLE,
			ProtectedObjectKey:  "production/" + environmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
			ImmutableCreate:     true, PutAfterArtifactPreparedAck: true, HeadAfterUploadCompletedAck: true,
		},
		Source: &agentpb.BackupSourceCapture_Config{Config: &agentpb.BackupConfigSource{SnapshotRevision: 14}},
	}
	sealed, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP, TargetId: environmentID,
		Steps: []*agentpb.ExecutionStep{{
			StepId: stepID, TimeoutSeconds: 600,
			Payload: &agentpb.ExecutionStep_BackupSourceCapture{BackupSourceCapture: capture},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assignedAt := createdAt.Add(time.Second)
	deadline := assignedAt.Add(6 * time.Hour)
	task := base.TaskRecord{
		ID: taskID, OperationID: operationID, Owner: testtaskjournal.PlatformTaskOwner(),
		Actor: testtaskjournal.TaskActorOperator, Executor: testtaskjournal.TaskExecutorAgent,
		PlanID: planID, PlanHash: hex.EncodeToString(sealed.PlanHash), Type: testtaskjournal.TaskBackup,
		Target: environmentID, Steps: []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: stepID}},
		TimeoutSeconds: 6 * 60 * 60, Status: testtaskjournal.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: assignedAt, StartedAt: &assignedAt,
	}
	taskValue, err := base.EncodeTaskRecord(task)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clear(taskValue) })
	assignment := testtaskassignments.TaskAssignmentRecord{
		AssignmentID: assignmentID, TaskID: taskID, Executor: testtaskjournal.TaskExecutorAgent,
		AgentID: agentID, AgentGeneration: 1, ClaimedTaskRevision: 40,
		AssignedAt: assignedAt, Deadline: deadline, RecoveryDeadline: deadline.Add(6 * time.Hour),
		ExecutionMode: testtaskassignments.TaskExecutionModeForward, ExecutionEpoch: 1,
	}
	assignmentValue, err := testtaskassignments.EncodeTaskAssignment(assignment)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clear(assignmentValue) })
	request := backupsecret.Request{
		TaskID: taskID, AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: 1,
		Deadline: deadline, StepID: stepID, Plan: sealed, Step: sealed.Steps[0],
	}
	return &deadlineBoundaryStore{
		revision: 41, taskValue: taskValue, assignmentValue: assignmentValue,
	}, request, deadline
}
