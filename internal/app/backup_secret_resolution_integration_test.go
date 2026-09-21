package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	testbackup "github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	backupsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/backupsecrets"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	appBackupSourceRevision       int64 = 100
	appBackupEnvironmentRevision  int64 = 101
	appBackupConnectorRevision    int64 = 102
	appBackupSecretRevision       int64 = 103
	appBackupPointRevision        int64 = 200
	appBackupPendingPruneRevision int64 = 201
	appBackupPublicationRevision  int64 = 300
	appBackupAssignmentRevision   int64 = 400
	appBackupTaskTimeoutSeconds   int64 = 6 * 60 * 60
)

// Rationale: the app composition must run one published prune snapshot through
// the production fixed-revision reader, Controller decryption, authenticated
// recovered dispatch, exact S3 slots, and owned cleanup.
func TestBackupSecretReaderResolverAgentChannelComposition(t *testing.T) {
	fixture := newAppBackupSecretFixture(t)
	reader, err := backupsecrets.NewReader(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	crypt := &appBackupSecretCrypt{plaintexts: fixture.plaintexts}
	protector, err := secretvalue.NewProtector(crypt, crypt)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := testbackup.NewBackupSecretResolver(reader, protector)
	if err != nil {
		t.Fatal(err)
	}
	auth := &appBackupSecretAuthenticator{
		agentID:    fixture.request.AgentID,
		generation: fixture.request.AgentGeneration,
		config: &agentpb.AgentConfig{
			PullIntervalSeconds: 5,
			MaxConcurrentTasks:  1,
		},
	}
	server := agentchannel.NewWithPrivateTransfers(
		auth,
		agentchannel.NewRegistry(),
		&appBackupSecretTaskStore{claim: fixture.claim},
		&appBackupSecretPlanResolver{plan: fixture.request.Plan},
		nil,
		resolver,
	)
	stream := &appBackupSecretStream{messages: []*agentpb.AgentMessage{
		appBackupSecretAuthenticate(fixture.request.AgentID),
		{
			Payload: &agentpb.AgentMessage_Ready{
				Ready: &agentpb.Ready{Capacity: 1, Version: "integration"},
			},
		},
	}}
	if err := server.Connect(stream); err != nil {
		t.Fatalf("connect: %v", err)
	}

	var assignmentCount int
	var transfers []*agentpb.BackupSecretSlotTransfer
	for _, message := range stream.sent {
		if assignment := message.GetTaskAssignment(); assignment != nil {
			assignmentCount++
			if assignment.GetTaskId() != fixture.request.TaskID ||
				assignment.GetAssignmentId() != fixture.request.AssignmentID || assignment.GetExecutionEpoch() != 1 {
				t.Fatalf("task assignment = %#v", assignment)
			}
		}
		if transfer := message.GetBackupSecretSlotTransfer(); transfer != nil {
			transfers = append(transfers, transfer)
		}
	}
	if assignmentCount != 1 || len(transfers) != 6 {
		t.Fatalf("assignment count = %d, slot frames = %d", assignmentCount, len(transfers))
	}
	assertAppBackupSecretSlot(
		t,
		transfers[:3],
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		fixture.expectedAccess,
	)
	assertAppBackupSecretSlot(
		t,
		transfers[3:],
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
		fixture.expectedSecret,
	)
	if fixture.store.calls != 6 {
		t.Fatalf("fixed-revision store calls = %d, want 6", fixture.store.calls)
	}
	if len(crypt.ciphertexts) != 2 || len(crypt.opened) != 2 {
		t.Fatalf("crypt opens = %d/%d, want 2/2", len(crypt.ciphertexts), len(crypt.opened))
	}
	for index, ciphertext := range crypt.ciphertexts {
		if !allAppBackupSecretZero(ciphertext) {
			t.Fatalf("reader ciphertext %d was not cleared", index)
		}
	}
	for index, plaintext := range crypt.opened {
		if !allAppBackupSecretZero(plaintext) {
			t.Fatalf("provider plaintext %d was not cleared", index)
		}
	}
}

type appBackupSecretFixture struct {
	claim          etcd.TaskAssignment
	request        backupsecret.Request
	store          *appBackupSecretStore
	plaintexts     map[string][]byte
	expectedAccess string
	expectedSecret string
}

func newAppBackupSecretFixture(t *testing.T) appBackupSecretFixture {
	t.Helper()
	pointCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	verifiedAt := pointCreatedAt.Add(time.Second)
	pruneCreatedAt := pointCreatedAt.Add(2 * time.Second)
	dispatchAt := pointCreatedAt.Add(3 * time.Second)
	assignedAt := pointCreatedAt.Add(4 * time.Second)
	environmentCreatedAt := pointCreatedAt.Add(-time.Hour)

	tenantID := ids.NewAt(ids.KindTenant, environmentCreatedAt, 1)
	projectID := ids.NewAt(ids.KindProject, environmentCreatedAt, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, environmentCreatedAt, 3)
	createTaskID := ids.NewAt(ids.KindTask, environmentCreatedAt, 4)
	sourceID := ids.NewAt(ids.KindBackupSource, environmentCreatedAt, 5)
	volumeID := ids.NewAt(ids.KindVolume, environmentCreatedAt, 6)
	connectorID := ids.NewAt(ids.KindConnector, environmentCreatedAt, 7)
	pointID := ids.NewAt(ids.KindRecoveryPoint, pointCreatedAt, 8)
	operationID := ids.NewAt(ids.KindOperation, dispatchAt, 9)
	taskID := ids.NewAt(ids.KindTask, dispatchAt, 10)
	planID := ids.NewAt(ids.KindPlan, dispatchAt, 11)
	stepID := ids.NewAt(ids.KindStep, dispatchAt, 12)
	assignmentID := ids.NewAt(ids.KindAssignment, assignedAt, 13)
	agentID := ids.NewAt(ids.KindAgent, assignedAt, 14)
	secretID := ids.NewAt(ids.KindSecret, environmentCreatedAt, 15)

	project := testhierarchy.ProjectRecord{
		ID:       projectID,
		TenantID: tenantID,
		Slug:     "production",
		Name:     "Production",
		Kind:     testhierarchy.ProjectKindTenant,
	}
	environment, err := testhierarchy.NewProvisioningEnvironment(
		"/var/lib/groundplane/vol",
		project,
		environmentID,
		"production",
		"10.40.0.0/24",
		createTaskID,
		environmentCreatedAt,
	)
	if err != nil {
		t.Fatalf("create environment fixture: %v", err)
	}
	environment, err = testhierarchy.CompleteEnvironmentProvisioning(environment, createTaskID, true)
	if err != nil {
		t.Fatalf("complete environment fixture: %v", err)
	}
	owner, err := testtaskjournal.EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatalf("create task owner fixture: %v", err)
	}

	connector, err := testconnectors.NewRecord(core.Connector{
		ID: connectorID, EnvironmentID: environmentID, Name: "backups",
		Kind: core.ConnectorKindS3Compatible, Endpoint: "https://objects.example.test",
		Bucket: "groundplane-backups", Prefix: "production/", Region: "auto", PathStyle: true,
		Credentials: map[core.ConnectorCredentialName]core.ConnectorCredential{
			core.ConnectorCredentialAccessKey: {Kind: core.ConnectorCredentialDirect},
			core.ConnectorCredentialSecretKey: {
				Kind: core.ConnectorCredentialSecretRef, SecretRef: "C16_SECRET",
			},
		},
	})
	if err != nil {
		t.Fatalf("create connector fixture: %v", err)
	}
	directCiphertext := []byte("app-direct-envelope")
	directCredentials, err := testconnectors.NewEncryptedCredentials(connectorID, directCiphertext)
	if err != nil {
		t.Fatalf("create connector credentials fixture: %v", err)
	}
	defer clear(directCredentials.Ciphertext)

	secretRecord, err := testsecrets.NewProjectRecord(
		secretID,
		projectID,
		"C16_SECRET",
		core.SecretKindEnvVar,
		"",
		environmentCreatedAt,
	)
	if err != nil {
		t.Fatalf("create secret fixture: %v", err)
	}
	secretCiphertext := []byte("app-secret-envelope")
	secretDigest := sha256.Sum256(secretCiphertext)
	secretValue := testsecrets.EncryptedValue{
		SecretID: secretID, EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextSHA256: hex.EncodeToString(secretDigest[:]), Ciphertext: secretCiphertext,
	}

	source := testbackuppolicy.BackupSourceRecord{
		ID: sourceID, EnvironmentID: environmentID, Kind: core.BackupSourceVolume,
		TargetID: volumeID, CreatedAt: environmentCreatedAt,
	}
	storedDigest := sha256.Sum256([]byte("stored-artifact"))
	point := testbackupruntime.BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: testbackupruntime.BackupRecoveryPointSnapshot{
			ID:              pointID,
			EnvironmentID:   environmentID,
			SourceID:        sourceID,
			SourceKind:      testbackupruntime.BackupRuntimeSourceVolume,
			TargetID:        volumeID,
			ConnectorID:     connectorID,
			ConnectorPrefix: "production/",
			ObjectKey:       "production/" + environmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
			SourceFormat:    testbackupruntime.BackupRuntimeFormatVolume,
			Encryption:      testbackupruntime.BackupRuntimeEncryptionNone,
			SizeBytes:       4096,
			SHA256:          hex.EncodeToString(storedDigest[:]),
			CreatedAt:       pointCreatedAt,
		},
		VerifiedAt: verifiedAt,
	}
	pendingPrune := testbackupruntime.BackupRecoveryPointPruneRecord{
		Point: point.BackupRecoveryPointSnapshot, PointRevision: appBackupPointRevision,
		OperationID: operationID, State: testbackupruntime.BackupPrunePending,
		CreatedAt: pruneCreatedAt, UpdatedAt: pruneCreatedAt,
	}
	assignedPrune := pendingPrune
	assignedPrune.State = testbackupruntime.BackupPruneAssigned
	assignedPrune.TaskID = taskID
	assignedPrune.UpdatedAt = dispatchAt
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID: taskID, OperationID: operationID, EnvironmentID: environmentID,
		RecoveryPointIDs: []string{pointID}, CreatedAt: dispatchAt,
	}

	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE, TargetId: environmentID,
		Steps: []*agentpb.ExecutionStep{
			{
				StepId:         stepID,
				TimeoutSeconds: executionplan.MaximumBackupPruneStepTimeoutSeconds,
				Payload: &agentpb.ExecutionStep_BackupArtifactPrune{
					BackupArtifactPrune: &agentpb.BackupArtifactPrune{
						Ordinal:             1,
						PruneOperationId:    operationID,
						PruneRevision:       uint64(appBackupPendingPruneRevision),
						PointId:             pointID,
						PointRevision:       uint64(appBackupPointRevision),
						SourceId:            sourceID,
						SourceRevision:      uint64(appBackupSourceRevision),
						EnvironmentId:       environmentID,
						EnvironmentRevision: uint64(appBackupEnvironmentRevision),
						ConnectorId:         connectorID,
						ConnectorRevision:   uint64(appBackupConnectorRevision),
						ConnectorEndpoint:   connector.Connector.Endpoint,
						ConnectorBucket:     connector.Connector.Bucket,
						ConnectorPrefix:     connector.Connector.Prefix,
						ConnectorRegion:     connector.Connector.Region,
						ConnectorAddressing: agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_PATH_STYLE,
						ProtectedObjectKey:  point.ObjectKey,
						StoredSizeBytes:     uint64(point.SizeBytes),
						StoredSha256:        storedDigest[:],
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("seal backup prune plan: %v", err)
	}
	startedAt := assignedAt
	task := etcd.TaskRecord{
		ID:                taskID,
		OperationID:       operationID,
		Owner:             owner,
		Actor:             testtaskjournal.TaskActorSystem,
		Executor:          testtaskjournal.TaskExecutorAgent,
		PlanID:            planID,
		PlanHash:          hex.EncodeToString(plan.PlanHash),
		Type:              testtaskjournal.TaskBackupPrune,
		Target:            environmentID,
		Steps:             []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: stepID}},
		TimeoutSeconds:    appBackupTaskTimeoutSeconds,
		Status:            testtaskjournal.TaskStatusRunning,
		NextEventSequence: 1,
		CreatedAt:         dispatchAt,
		UpdatedAt:         assignedAt,
		StartedAt:         &startedAt,
	}
	deadline := assignedAt.Add(time.Duration(appBackupTaskTimeoutSeconds) * time.Second)
	recoveryDeadline := deadline.Add(time.Duration(appBackupTaskTimeoutSeconds) * time.Second)
	assignment := testtaskassignments.TaskAssignmentRecord{
		AssignmentID: assignmentID, TaskID: taskID, Executor: testtaskjournal.TaskExecutorAgent,
		AgentID: agentID, AgentGeneration: 1, ClaimedTaskRevision: appBackupPublicationRevision,
		AssignedAt: assignedAt, Deadline: deadline, RecoveryDeadline: recoveryDeadline,
		ExecutionMode: testtaskassignments.TaskExecutionModeForward, ExecutionEpoch: 1,
	}
	claim := etcd.TaskAssignment{
		Task: testkeyvalue.Versioned[etcd.TaskRecord]{
			Record:       task,
			Revision:     appBackupAssignmentRevision,
			ReadRevision: appBackupAssignmentRevision,
		},
		Assignment: testkeyvalue.Versioned[testtaskassignments.TaskAssignmentRecord]{
			Record:       assignment,
			Revision:     appBackupAssignmentRevision,
			ReadRevision: appBackupAssignmentRevision,
		},
	}
	request := backupsecret.Request{
		TaskID: taskID, AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: 1,
		Deadline: deadline, StepID: stepID, Plan: plan, Step: plan.Steps[0],
	}

	store := &appBackupSecretStore{
		task:              appBackupTaskValue(t, task),
		assignment:        appBackupAssignmentValue(t, assignment),
		dispatch:          appBackupEnvelope(t, "recovery-point-prune-dispatch", dispatch),
		point:             appBackupEnvelope(t, "recovery-point", point),
		pendingPrune:      appBackupEnvelope(t, "recovery-point-prune", pendingPrune),
		assignedPrune:     appBackupEnvelope(t, "recovery-point-prune", assignedPrune),
		source:            appBackupEnvelope(t, "backup-source", source),
		connector:         appBackupEnvelope(t, "connector", connector),
		environment:       appBackupEnvelope(t, "environment", environment),
		directCredentials: appBackupEnvelope(t, "connector_credentials", directCredentials),
		project:           appBackupEnvelope(t, "project", project),
		secretID:          []byte(secretID),
		secretRecord:      appBackupEnvelope(t, "secret", secretRecord),
		secretValue:       appBackupEnvelope(t, "secret_value", secretValue),
	}
	return appBackupSecretFixture{
		claim: claim, request: request, store: store,
		plaintexts: map[string][]byte{
			string(directCiphertext): []byte(`{"access_key":"app-access"}`),
			string(secretCiphertext): []byte("app-secret"),
		},
		expectedAccess: "app-access", expectedSecret: "app-secret",
	}
}

type appBackupSecretStore struct {
	task              []byte
	assignment        []byte
	dispatch          []byte
	point             []byte
	pendingPrune      []byte
	assignedPrune     []byte
	source            []byte
	connector         []byte
	environment       []byte
	directCredentials []byte
	project           []byte
	secretID          []byte
	secretRecord      []byte
	secretValue       []byte
	calls             int
}

type appBackupSecretStoredValue struct {
	value          []byte
	createRevision int64
	modRevision    int64
	version        int64
}

func (store *appBackupSecretStore) GetMany(
	_ context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	store.calls++
	var readRevision int64
	var values []appBackupSecretStoredValue
	switch store.calls {
	case 1:
		if request.Revision != 0 || len(request.Keys) != 2 {
			return nil, errs.New(errs.KindInternal, "backup secret anchor read changed")
		}
		readRevision = appBackupAssignmentRevision
		values = []appBackupSecretStoredValue{{}, store.assignmentValue()}
	case 2:
		if request.Revision != appBackupAssignmentRevision || len(request.Keys) != 6 {
			return nil, errs.New(errs.KindInternal, "backup secret base read changed")
		}
		readRevision = appBackupAssignmentRevision
		assignment := store.assignmentValue()
		values = []appBackupSecretStoredValue{
			store.taskValue(), assignment, assignment, assignment, {}, store.dispatchValue(),
		}
	case 3:
		if request.Revision != appBackupAssignmentRevision || len(request.Keys) != 8 {
			return nil, errs.New(errs.KindInternal, "backup secret first dynamic read changed")
		}
		readRevision = appBackupAssignmentRevision
		values = store.dynamicValues()
	case 4:
		if request.Revision != appBackupPendingPruneRevision || len(request.Keys) != 1 {
			return nil, errs.New(errs.KindInternal, "backup secret sealed prune read changed")
		}
		readRevision = appBackupPendingPruneRevision
		values = []appBackupSecretStoredValue{{
			value: store.pendingPrune, createRevision: appBackupPendingPruneRevision,
			modRevision: appBackupPendingPruneRevision, version: 1,
		}}
	case 5:
		if request.Revision != appBackupAssignmentRevision || len(request.Keys) != 12 {
			return nil, errs.New(errs.KindInternal, "backup secret second dynamic read changed")
		}
		readRevision = appBackupAssignmentRevision
		values = append(store.dynamicValues(),
			appBackupStored(store.project, appBackupSecretRevision-1),
			appBackupSecretStoredValue{},
			appBackupStored(store.secretID, appBackupSecretRevision),
			appBackupSecretStoredValue{},
		)
	case 6:
		if request.Revision != appBackupAssignmentRevision || len(request.Keys) != 15 {
			return nil, errs.New(errs.KindInternal, "backup secret final dynamic read changed")
		}
		readRevision = appBackupAssignmentRevision
		values = append(store.dynamicValues(),
			appBackupStored(store.project, appBackupSecretRevision-1),
			appBackupSecretStoredValue{},
			appBackupStored(store.secretID, appBackupSecretRevision),
			appBackupSecretStoredValue{},
			appBackupStored(store.secretRecord, appBackupSecretRevision),
			appBackupSecretStoredValue{},
			appBackupStored(store.secretValue, appBackupSecretRevision),
		)
	default:
		return nil, errs.New(errs.KindInternal, "unexpected backup secret read")
	}
	result := &testkeyvalue.GetManyResult{
		Values: make([]*testkeyvalue.KeyValue, len(values)), ReadRevision: readRevision,
		ResponseRevision: appBackupAssignmentRevision,
	}
	for index, value := range values {
		if value.value == nil {
			continue
		}
		result.Values[index] = &testkeyvalue.KeyValue{
			Key:         request.Keys[index],
			Value:       append([]byte(nil), value.value...),
			ModRevision: value.modRevision,
			Version:     value.version,
		}
	}
	return result, nil
}

func (store *appBackupSecretStore) assignmentValue() appBackupSecretStoredValue {
	return appBackupStored(store.assignment, appBackupAssignmentRevision)
}

func (store *appBackupSecretStore) taskValue() appBackupSecretStoredValue {
	return appBackupSecretStoredValue{
		value: store.task, createRevision: appBackupPublicationRevision,
		modRevision: appBackupAssignmentRevision, version: 2,
	}
}

func (store *appBackupSecretStore) dispatchValue() appBackupSecretStoredValue {
	return appBackupStored(store.dispatch, appBackupPublicationRevision)
}

func (store *appBackupSecretStore) dynamicValues() []appBackupSecretStoredValue {
	return []appBackupSecretStoredValue{
		appBackupStored(store.point, appBackupPointRevision),
		{
			value: store.assignedPrune, createRevision: appBackupPendingPruneRevision,
			modRevision: appBackupPublicationRevision, version: 2,
		},
		appBackupStored(store.source, appBackupSourceRevision),
		appBackupStored(store.connector, appBackupConnectorRevision),
		appBackupStored(store.environment, appBackupEnvironmentRevision),
		{},
		{},
		appBackupStored(store.directCredentials, appBackupConnectorRevision),
	}
}

func appBackupStored(value []byte, revision int64) appBackupSecretStoredValue {
	return appBackupSecretStoredValue{
		value: value, createRevision: revision, modRevision: revision, version: 1,
	}
}

func appBackupEnvelope(t *testing.T, kind string, data any) []byte {
	t.Helper()
	value, err := json.Marshal(struct {
		Schema int    `json:"schema"`
		Kind   string `json:"kind"`
		Data   any    `json:"data"`
	}{Schema: 1, Kind: kind, Data: data})
	if err != nil {
		t.Fatalf("encode %s fixture: %v", kind, err)
	}
	return value
}

func appBackupTaskValue(t *testing.T, task etcd.TaskRecord) []byte {
	t.Helper()
	data := struct {
		ID                string                           `json:"id"`
		OperationID       string                           `json:"operation_id"`
		Owner             testtaskjournal.TaskOwner        `json:"owner"`
		Actor             testtaskjournal.TaskActor        `json:"actor"`
		Executor          testtaskjournal.TaskExecutor     `json:"executor"`
		PlanID            string                           `json:"plan_id"`
		PlanHash          string                           `json:"plan_hash"`
		RenderGeneration  int32                            `json:"render_generation"`
		Type              testtaskjournal.TaskType         `json:"type"`
		Target            string                           `json:"target"`
		Steps             []testtaskjournal.TaskStepRecord `json:"steps"`
		TimeoutSeconds    int64                            `json:"timeout_seconds"`
		Status            testtaskjournal.TaskStatus       `json:"status"`
		NextEventSequence uint64                           `json:"next_event_sequence"`
		EventCount        uint32                           `json:"event_count"`
		CreatedAt         string                           `json:"created_at"`
		UpdatedAt         string                           `json:"updated_at"`
		StartedAt         string                           `json:"started_at"`
	}{
		ID:                task.ID,
		OperationID:       task.OperationID,
		Owner:             task.Owner,
		Actor:             task.Actor,
		Executor:          task.Executor,
		PlanID:            task.PlanID,
		PlanHash:          task.PlanHash,
		RenderGeneration:  task.RenderGeneration,
		Type:              task.Type,
		Target:            task.Target,
		Steps:             task.Steps,
		TimeoutSeconds:    task.TimeoutSeconds,
		Status:            task.Status,
		NextEventSequence: task.NextEventSequence,
		EventCount:        task.EventCount,
		CreatedAt: task.CreatedAt.Format(
			time.RFC3339Nano,
		),
		UpdatedAt: task.UpdatedAt.Format(time.RFC3339Nano),
		StartedAt: task.StartedAt.Format(time.RFC3339Nano),
	}
	return appBackupEnvelope(t, "task", data)
}

func appBackupAssignmentValue(t *testing.T, assignment testtaskassignments.TaskAssignmentRecord) []byte {
	t.Helper()
	value, err := json.Marshal(struct {
		Schema              int                                   `json:"schema"`
		AssignmentID        string                                `json:"assignment_id"`
		TaskID              string                                `json:"task_id"`
		Executor            testtaskjournal.TaskExecutor          `json:"executor"`
		AgentID             string                                `json:"agent_id"`
		AgentGeneration     uint64                                `json:"agent_generation"`
		ClaimedTaskRevision int64                                 `json:"claimed_task_revision"`
		AssignedAt          string                                `json:"assigned_at"`
		Deadline            string                                `json:"forward_deadline"`
		RecoveryDeadline    string                                `json:"recovery_deadline"`
		ExecutionMode       testtaskassignments.TaskExecutionMode `json:"execution_mode"`
		ExecutionEpoch      uint32                                `json:"execution_epoch"`
	}{
		Schema:              3,
		AssignmentID:        assignment.AssignmentID,
		TaskID:              assignment.TaskID,
		Executor:            assignment.Executor,
		AgentID:             assignment.AgentID,
		AgentGeneration:     assignment.AgentGeneration,
		ClaimedTaskRevision: assignment.ClaimedTaskRevision,
		AssignedAt:          assignment.AssignedAt.Format(time.RFC3339Nano),
		Deadline:            assignment.Deadline.Format(time.RFC3339Nano),
		RecoveryDeadline:    assignment.RecoveryDeadline.Format(time.RFC3339Nano),
		ExecutionMode:       assignment.ExecutionMode,
		ExecutionEpoch:      assignment.ExecutionEpoch,
	})
	if err != nil {
		t.Fatalf("encode assignment fixture: %v", err)
	}
	return value
}

type appBackupSecretCrypt struct {
	plaintexts  map[string][]byte
	ciphertexts [][]byte
	opened      [][]byte
}

func (*appBackupSecretCrypt) Seal(context.Context, []byte) ([]byte, error) {
	return nil, errs.New(errs.KindInternal, "backup secret composition must not seal")
}

func (crypt *appBackupSecretCrypt) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	plaintext, ok := crypt.plaintexts[string(ciphertext)]
	if !ok {
		return nil, errs.New(errs.KindInternal, "unexpected backup secret ciphertext")
	}
	crypt.ciphertexts = append(crypt.ciphertexts, ciphertext)
	opened := append([]byte(nil), plaintext...)
	crypt.opened = append(crypt.opened, opened)
	return opened, nil
}

type appBackupSecretAuthenticator struct {
	agentID    string
	generation uint64
	config     *agentpb.AgentConfig
}

func (auth *appBackupSecretAuthenticator) Authenticate(
	_ context.Context,
	agentID string,
	_ agentchannel.Token,
) (agentchannel.Authorization, error) {
	if agentID != auth.agentID {
		return agentchannel.Authorization{}, errs.New(errs.KindInternal, "unexpected agent")
	}
	return agentchannel.Authorization{
		Generation: auth.generation,
		Config:     proto.Clone(auth.config).(*agentpb.AgentConfig),
	}, nil
}

func (auth *appBackupSecretAuthenticator) Configuration(
	context.Context,
	string,
	uint64,
) (*agentpb.AgentConfig, error) {
	return proto.Clone(auth.config).(*agentpb.AgentConfig), nil
}

type appBackupSecretTaskStore struct {
	claim etcd.TaskAssignment
}

func (store *appBackupSecretTaskStore) ListAgentAssignments(
	context.Context,
	string,
	uint64,
	int32,
) ([]etcd.TaskAssignment, error) {
	return []etcd.TaskAssignment{store.claim}, nil
}

func (*appBackupSecretTaskStore) ReconnectAgentAssignment(
	context.Context,
	etcd.TaskAssignment,
) (etcd.TaskAssignment, error) {
	return etcd.TaskAssignment{}, errs.New(errs.KindInternal, "unexpected assignment reconnect")
}

func (*appBackupSecretTaskStore) ClaimNextTask(
	context.Context,
	string,
	uint64,
	time.Time,
) (etcd.TaskAssignment, bool, error) {
	return etcd.TaskAssignment{}, false, nil
}

func (store *appBackupSecretTaskStore) GetTask(
	_ context.Context,
	taskID string,
) (testkeyvalue.Versioned[etcd.TaskRecord], error) {
	if taskID != store.claim.Task.Record.ID {
		return testkeyvalue.Versioned[etcd.TaskRecord]{}, errs.New(errs.KindTaskNotFound, "unexpected task")
	}
	return store.claim.Task, nil
}

func (store *appBackupSecretTaskStore) ListTaskEvents(
	_ context.Context,
	taskID string,
	revision int64,
) (etcd.TaskEventSnapshot, error) {
	if taskID != store.claim.Task.Record.ID || revision != store.claim.Task.ReadRevision {
		return etcd.TaskEventSnapshot{}, errs.New(errs.KindInternal, "unexpected task event snapshot")
	}
	return etcd.TaskEventSnapshot{
		Task: store.claim.Task.Record, Revision: store.claim.Task.ReadRevision,
	}, nil
}

func (*appBackupSecretTaskStore) AppendTaskEvent(
	context.Context, testtaskjournal.TaskEventInput,

	time.Time,
) (etcd.TaskEventAppend, error) {
	return etcd.TaskEventAppend{}, errs.New(errs.KindInternal, "unexpected task event")
}

func (*appBackupSecretTaskStore) AcknowledgeTask(
	context.Context,
	string,
	uint64,
	string,
	string, testtaskjournal.TaskStatus, testtaskjournal.TaskResultRecord,

	time.Time,
) (testkeyvalue.Versioned[etcd.TaskRecord], error) {
	return testkeyvalue.Versioned[etcd.TaskRecord]{}, errs.New(
		errs.KindInternal,
		"unexpected task acknowledgement",
	)
}

type appBackupSecretPlanResolver struct {
	plan *agentpb.ExecutionPlan
}

func (resolver *appBackupSecretPlanResolver) ResolveExecutionPlan(
	context.Context,
	etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	return proto.Clone(resolver.plan).(*agentpb.ExecutionPlan), nil
}

type appBackupSecretStream struct {
	messages []*agentpb.AgentMessage
	sent     []*agentpb.ControllerMessage
}

func (stream *appBackupSecretStream) Send(message *agentpb.ControllerMessage) error {
	stream.sent = append(stream.sent, proto.Clone(message).(*agentpb.ControllerMessage))
	return nil
}

func (stream *appBackupSecretStream) Recv() (*agentpb.AgentMessage, error) {
	if len(stream.messages) == 0 {
		return nil, io.EOF
	}
	message := stream.messages[0]
	stream.messages = stream.messages[1:]
	return message, nil
}

func (*appBackupSecretStream) SetHeader(metadata.MD) error  { return nil }
func (*appBackupSecretStream) SendHeader(metadata.MD) error { return nil }
func (*appBackupSecretStream) SetTrailer(metadata.MD)       {}
func (*appBackupSecretStream) Context() context.Context     { return context.Background() }
func (*appBackupSecretStream) SendMsg(any) error {
	return errs.New(errs.KindInternal, "unexpected generic stream send")
}
func (*appBackupSecretStream) RecvMsg(any) error {
	return errs.New(errs.KindInternal, "unexpected generic stream receive")
}

func appBackupSecretAuthenticate(agentID string) *agentpb.AgentMessage {
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Authenticate{
		Authenticate: &agentpb.Authenticate{AgentId: agentID, Token: bytes.Repeat([]byte{7}, 32)},
	}}
}

func assertAppBackupSecretSlot(
	t *testing.T,
	frames []*agentpb.BackupSecretSlotTransfer,
	purpose agentpb.BackupSecretSlotPurpose,
	want string,
) {
	t.Helper()
	if len(frames) != 3 || frames[0].GetPurpose() != purpose || frames[0].GetHeader() == nil ||
		frames[1].GetPurpose() != purpose || string(frames[1].GetChunk().GetContent()) != want ||
		frames[2].GetPurpose() != purpose || frames[2].GetEnd() == nil {
		t.Fatalf("slot frames = %#v", frames)
	}
}

func allAppBackupSecretZero(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}
