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

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	testbackup "github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testbackupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
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
	appBackupPolicyRevision       int64 = 104
	appBackupPointRevision        int64 = 200
	appBackupPendingPruneRevision int64 = 201
	appBackupPublicationRevision  int64 = 300
	appBackupAssignmentRevision   int64 = 400
	appBackupTaskTimeoutSeconds   int64 = executionplan.MaximumBackupPruneStepTimeoutSeconds
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
	resolver, err := testbackup.NewBackupSecretResolver(t.Context(), reader, protector)
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
	checkpointStore := newMemoryHierarchyStore()
	if _, err := checkpointStore.Put(t.Context(), testtaskjournal.TaskAssignmentIndexKey(fixture.request.TaskID),
		appBackupAssignmentValue(t, fixture.claim.Assignment.Record)); err != nil {
		t.Fatal(err)
	}
	checkpointRepository, err := etcd.NewBackupRuntimeRepository(checkpointStore)
	if err != nil {
		t.Fatal(err)
	}
	checkpoints, err := testbackup.NewBackupCheckpointService(checkpointRepository, checkpointStore, protector)
	if err != nil {
		t.Fatal(err)
	}
	server := agentchannel.NewWithRuntimeServices(
		auth,
		agentchannel.NewRegistry(),
		&appBackupSecretTaskStore{claim: fixture.claim},
		&appBackupSecretPlanResolver{plan: fixture.request.Plan},
		nil,
		resolver,
		checkpoints,
	)
	stagingInventory, stagingAck := appBackupSecretStagingExchange(t)
	streamContext, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream := &appBackupSecretStream{
		ctx:         streamContext,
		payloadDone: make(chan struct{}),
		messages: []*agentpb.AgentMessage{
			appBackupSecretAuthenticate(fixture.request.AgentID),
			{
				Payload: &agentpb.AgentMessage_BackupStagingInventory{BackupStagingInventory: stagingInventory},
			},
			{
				Payload: &agentpb.AgentMessage_BackupStagingRecoveryAck{BackupStagingRecoveryAck: stagingAck},
			},
			{
				Payload: &agentpb.AgentMessage_Ready{
					Ready: &agentpb.Ready{
						Capacity: 1, Version: "integration", TerminalDeliveryClean: proto.Bool(true),
					},
				},
			},
		},
	}
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
	projectValue := appBackupEnvelope(t, "project", project)
	environmentValue := appBackupEnvelope(t, "environment", environment)
	connectorValue := appBackupEnvelope(t, "connector", connector)
	directCredentialsValue := appBackupEnvelope(t, "connector_credentials", directCredentials)
	secretValueValue := appBackupEnvelope(t, "secret_value", secretValue)

	source := testbackuppolicy.BackupSourceRecord{
		ID: sourceID, EnvironmentID: environmentID, Kind: core.BackupSourceVolume,
		TargetID: volumeID, CreatedAt: environmentCreatedAt,
	}
	storedDigest := sha256.Sum256([]byte("stored-artifact"))
	storedDigestHex := hex.EncodeToString(storedDigest[:])
	objectKey := "production/" + environmentID + "/" + sourceID + "/" + pointID + "/artifact.bin"
	point := testbackupruntime.BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: testbackupruntime.BackupRecoveryPointSnapshot{
			BackupRecoveryPointTargetSnapshot: testbackupruntime.BackupRecoveryPointTargetSnapshot{
				ID: pointID, EnvironmentID: environmentID, SourceID: sourceID,
				SourceKind: testbackupruntime.BackupRuntimeSourceVolume, TargetID: volumeID,
				ConnectorID: connectorID, ConnectorPrefix: connector.Connector.Prefix,
				ConnectorEndpoint: connector.Connector.Endpoint, ConnectorBucket: connector.Connector.Bucket,
				ConnectorRegion: connector.Connector.Region, ConnectorPathStyle: connector.Connector.PathStyle,
				ObjectKey: objectKey, SourceFormat: testbackupruntime.BackupRuntimeFormatVolume,
				Encryption: testbackupruntime.BackupRuntimeEncryptionNone, CreatedAt: pointCreatedAt,
			},
			Evidence: testbackupruntime.BackupArtifactEvidence{
				SourceSizeBytes: 4096, SourceSHA256: storedDigestHex,
				StoredSizeBytes: 4096, StoredSHA256: storedDigestHex,
			},
			VolumeArchive: testbackupruntime.BackupVolumeArchiveEvidence{
				EntryCount: 1, ContentManifestSHA256: storedDigestHex,
				FullTreeSHA256: storedDigestHex, SourceSizeBytes: 4096,
				Manifest: testbackupruntime.BackupVolumeManifestReference{
					TaskID: taskID, AssignmentID: assignmentID, StepID: stepID, TransferID: ids.NewULID(),
					AuthoritySHA256: storedDigestHex, AgentID: agentID,
					AgentGeneration: 1, AssignmentGeneration: 1, CursorRevision: appBackupPointRevision,
				},
			},
			Object: testbackupruntime.BackupObjectIdentity{
				Target: testbackupruntime.BackupObjectTarget{
					ConnectorID: connectorID, ConnectorPrefix: connector.Connector.Prefix,
					ConnectorEndpoint: connector.Connector.Endpoint, ConnectorBucket: connector.Connector.Bucket,
					ConnectorRegion: connector.Connector.Region, ConnectorPathStyle: connector.Connector.PathStyle,
					ObjectKey: objectKey,
				},
				Discriminator: testbackupruntime.BackupObjectDiscriminator{
					Kind: backupobject.DiscriminatorVersionID, Value: "version-1",
				},
			},
		},
		VerifiedAt: verifiedAt,
	}
	pointValue := appBackupEnvelope(t, "recovery-point", point)
	pointDigest := sha256.Sum256(pointValue)
	policyDigest := sha256.Sum256([]byte("app-backup-retention-policy"))
	pendingPrune := testbackupruntime.BackupRecoveryPointPruneRecord{
		Point: point.BackupRecoveryPointSnapshot, PointRevision: appBackupPointRevision,
		PolicyRevision: appBackupPolicyRevision, PolicySHA256: hex.EncodeToString(policyDigest[:]),
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
	startedAt := assignedAt
	task := etcd.TaskRecord{
		ID:                taskID,
		OperationID:       operationID,
		Owner:             owner,
		Actor:             testtaskjournal.TaskActorSystem,
		Executor:          testtaskjournal.TaskExecutorAgent,
		PlanID:            planID,
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
	pathStyle := connector.Connector.PathStyle
	connectorAuthority := &agentpb.BackupConnectorAuthority{
		ConnectorId: connectorID, Connector: appBackupRevisionDigest(connectorValue, appBackupConnectorRevision),
		CanonicalEndpointUrl: connector.Connector.Endpoint, Region: connector.Connector.Region,
		PathStyle: &pathStyle, Prefix: connector.Connector.Prefix,
		AccessKeySlotId: backupsecret.AccessKeySlotID, SecretKeySlotId: backupsecret.SecretKeySlotID,
		AccessKeySlot: appBackupRevisionDigest(directCredentialsValue, appBackupConnectorRevision),
		SecretKeySlot: appBackupRevisionDigest(secretValueValue, appBackupSecretRevision),
	}
	plan, err := testbackup.BuildBackupPrunePlan(testbackup.BackupPrunePlanInput{
		Task: task,
		Scope: &agentpb.BackupPlanScope{
			ProjectId: projectID, Project: appBackupRevisionDigest(projectValue, appBackupSecretRevision-1),
			EnvironmentId: environmentID,
			Environment:   appBackupRevisionDigest(environmentValue, appBackupEnvironmentRevision), TaskAttempt: 1,
		},
		Dispatch: dispatch,
		Evidence: []testbackupplanning.PruneExecutionEvidence{{
			Prune: pendingPrune, PruneRevision: appBackupPendingPruneRevision,
			PointRevision: appBackupPointRevision, SourceRevision: appBackupSourceRevision,
			EnvironmentRevision: appBackupEnvironmentRevision, ConnectorRevision: appBackupConnectorRevision,
			ConnectorEndpoint: connector.Connector.Endpoint, ConnectorBucket: connector.Connector.Bucket,
			ConnectorPrefix: connector.Connector.Prefix, ConnectorRegion: connector.Connector.Region,
			ConnectorPathStyle: connector.Connector.PathStyle,
			RetentionPolicy: &agentpb.RevisionDigest{
				ModRevision: appBackupPolicyRevision, Sha256: policyDigest[:],
			},
			PointSHA256: pointDigest[:], ConnectorAuthority: connectorAuthority,
		}},
		ExecutionIDs: []string{ids.NewULID()},
	})
	if err != nil {
		t.Fatalf("build backup prune plan: %v", err)
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	deadline := dispatchAt.Add(time.Duration(appBackupTaskTimeoutSeconds) * time.Second)
	recoveryDeadline := deadline.Add(time.Duration(appBackupTaskTimeoutSeconds) * time.Second)
	assignment := testtaskassignments.TaskAssignmentRecord{
		AssignmentID: assignmentID, TaskID: taskID, Executor: testtaskjournal.TaskExecutorAgent,
		AgentID: agentID, AgentGeneration: 1, ClaimedTaskRevision: appBackupPublicationRevision,
		AssignedAt: assignedAt, Deadline: deadline, RecoveryDeadline: recoveryDeadline,
		ExecutionMode: testtaskassignments.TaskExecutionModeForward, ExecutionEpoch: 1,
	}
	authority, authorityDigest, err := executionplan.BindBackupTaskAuthority(
		plan,
		executionplan.BackupAssignmentIdentity{
			TaskID: taskID, OperationID: operationID, AssignmentID: assignmentID,
			Generation: 1, DeadlineUnixNano: uint64(deadline.UnixNano()),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	assignment.BackupAuthorityFence = &testtaskassignments.BackupAuthorityFence{
		AssignmentGeneration: authority.AssignmentGeneration, AuthoritySHA256: hex.EncodeToString(authorityDigest),
	}
	for _, step := range authority.Steps {
		assignment.BackupAuthorityFence.Steps = append(assignment.BackupAuthorityFence.Steps,
			testtaskassignments.BackupStepAuthorityFence{
				StepID: step.StepId, ExecutionID: step.ExecutionId, AuthoritySHA256: hex.EncodeToString(step.StepDigest),
			})
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
		point:             pointValue,
		assignedPrune:     appBackupEnvelope(t, "recovery-point-prune", assignedPrune),
		source:            appBackupEnvelope(t, "backup-source", source),
		connector:         connectorValue,
		environment:       environmentValue,
		directCredentials: directCredentialsValue,
		project:           projectValue,
		secretID:          []byte(secretID),
		secretRecord:      appBackupEnvelope(t, "secret", secretRecord),
		secretValue:       secretValueValue,
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
		if request.Revision != appBackupAssignmentRevision || len(request.Keys) != 7 {
			return nil, errs.New(errs.KindInternal, "backup secret base read changed")
		}
		readRevision = appBackupAssignmentRevision
		assignment := store.assignmentValue()
		values = []appBackupSecretStoredValue{
			store.taskValue(), assignment, assignment, assignment, {}, store.dispatchValue(), {},
		}
	case 3:
		if request.Revision != appBackupAssignmentRevision || len(request.Keys) != 1 {
			return nil, errs.New(errs.KindInternal, "backup secret point planning read changed")
		}
		readRevision = appBackupAssignmentRevision
		values = []appBackupSecretStoredValue{appBackupStored(store.point, appBackupPointRevision)}
	case 4:
		if request.Revision != appBackupAssignmentRevision || len(request.Keys) != 8 {
			return nil, errs.New(errs.KindInternal, "backup secret first dynamic read changed")
		}
		readRevision = appBackupAssignmentRevision
		values = store.dynamicValues()
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

func appBackupRevisionDigest(value []byte, revision int64) *agentpb.RevisionDigest {
	digest := sha256.Sum256(value)
	return &agentpb.RevisionDigest{ModRevision: revision, Sha256: digest[:]}
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
	value, err := testtaskassignments.EncodeTaskAssignment(assignment)
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
	claim   etcd.TaskAssignment
	staging *testbackupruntime.BackupStagingDeliveryRecord
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

func (*appBackupSecretTaskStore) ReadBackupStagingSource(
	context.Context,
	string,
	uint64,
	[]byte,
) (etcd.BackupStagingSource, error) {
	return etcd.BackupStagingSource{}, errs.New(errs.KindInternal, "unexpected nonempty backup staging inventory")
}

func (*appBackupSecretTaskStore) ReadBackupStagingDelivery(
	context.Context,
	string,
) (testkeyvalue.Versioned[testbackupruntime.BackupStagingDeliveryRecord], bool, error) {
	return testkeyvalue.Versioned[testbackupruntime.BackupStagingDeliveryRecord]{}, false, nil
}

func (store *appBackupSecretTaskStore) PublishBackupStagingDelivery(
	_ context.Context,
	record testbackupruntime.BackupStagingDeliveryRecord,
	sources []etcd.BackupStagingSource,
) error {
	if len(sources) != 0 || executionplan.ValidateBackupStagingRecoveryPlan(record.Inventory, record.Plan) != nil {
		return errs.New(errs.KindInternal, "unexpected backup staging recovery plan")
	}
	store.staging = &record
	return nil
}

func (store *appBackupSecretTaskStore) ApplyBackupStagingDelivery(
	_ context.Context,
	agentID string,
	agentGeneration uint64,
	processGeneration [16]byte,
	ack *agentpb.BackupStagingRecoveryAck,
) (*agentpb.BackupStagingRecoveryAckReceipt, error) {
	if store.staging == nil || store.staging.AgentID != agentID || store.staging.AgentGeneration != agentGeneration ||
		store.staging.ProcessGeneration != processGeneration || !proto.Equal(store.staging.Inventory, &agentpb.BackupStagingInventory{}) {
		return nil, errs.New(errs.KindInternal, "unexpected backup staging acknowledgement identity")
	}
	planDigest, err := executionplan.BackupStagingRecoveryPlanSHA256(store.staging.Plan)
	if err != nil || !bytes.Equal(ack.GetInventorySha256(), store.staging.Plan.GetInventorySha256()) ||
		!bytes.Equal(ack.GetAppliedPlanSha256(), planDigest) || ack.GetAppliedDispositionCount() != 0 {
		return nil, errs.New(errs.KindInternal, "unexpected backup staging acknowledgement")
	}
	ackDigest, err := executionplan.BackupStagingRecoveryAckSHA256(ack)
	if err != nil {
		return nil, err
	}
	return &agentpb.BackupStagingRecoveryAckReceipt{
		ProcessGeneration: processGeneration[:],
		InventorySha256:   append([]byte(nil), ack.InventorySha256...),
		AppliedPlanSha256: append([]byte(nil), ack.AppliedPlanSha256...),
		RecoveryAckSha256: ackDigest,
	}, nil
}

func (store *appBackupSecretTaskStore) GetTaskAssignment(
	_ context.Context,
	taskID string,
) (etcd.TaskAssignment, error) {
	if taskID != store.claim.Task.Record.ID {
		return etcd.TaskAssignment{}, errs.New(errs.KindTaskNotFound, "unexpected task")
	}
	return store.claim, nil
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
	ctx         context.Context
	payloadDone chan struct{}
	slotFrames  int
	messages    []*agentpb.AgentMessage
	sent        []*agentpb.ControllerMessage
}

func (stream *appBackupSecretStream) Send(message *agentpb.ControllerMessage) error {
	stream.sent = append(stream.sent, proto.Clone(message).(*agentpb.ControllerMessage))
	if message.GetBackupSecretSlotTransfer() != nil {
		stream.slotFrames++
		if stream.slotFrames == 6 {
			close(stream.payloadDone)
		}
	}
	return nil
}

func (stream *appBackupSecretStream) Recv() (*agentpb.AgentMessage, error) {
	if len(stream.messages) == 0 {
		select {
		case <-stream.payloadDone:
			return nil, io.EOF
		case <-stream.ctx.Done():
			return nil, stream.ctx.Err()
		}
	}
	message := stream.messages[0]
	stream.messages = stream.messages[1:]
	return message, nil
}

func (*appBackupSecretStream) SetHeader(metadata.MD) error     { return nil }
func (*appBackupSecretStream) SendHeader(metadata.MD) error    { return nil }
func (*appBackupSecretStream) SetTrailer(metadata.MD)          {}
func (stream *appBackupSecretStream) Context() context.Context { return stream.ctx }
func (*appBackupSecretStream) SendMsg(any) error {
	return errs.New(errs.KindInternal, "unexpected generic stream send")
}
func (*appBackupSecretStream) RecvMsg(any) error {
	return errs.New(errs.KindInternal, "unexpected generic stream receive")
}

func appBackupSecretAuthenticate(agentID string) *agentpb.AgentMessage {
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Authenticate{
		Authenticate: &agentpb.Authenticate{
			AgentId: agentID, Token: bytes.Repeat([]byte{7}, 32),
			ExecutionPlanSchema: executionplan.SchemaVersion,
			ProcessGeneration:   bytes.Repeat([]byte{1}, 16),
		},
	}}
}

func appBackupSecretStagingExchange(
	t *testing.T,
) (*agentpb.BackupStagingInventory, *agentpb.BackupStagingRecoveryAck) {
	t.Helper()
	inventory := &agentpb.BackupStagingInventory{}
	inventoryDigest, err := executionplan.BackupStagingInventorySHA256(inventory)
	if err != nil {
		t.Fatal(err)
	}
	planDigest, err := executionplan.BackupStagingRecoveryPlanSHA256(
		&agentpb.BackupStagingRecoveryPlan{InventorySha256: inventoryDigest},
	)
	if err != nil {
		t.Fatal(err)
	}
	return inventory, &agentpb.BackupStagingRecoveryAck{
		InventorySha256: inventoryDigest, AppliedPlanSha256: planDigest,
	}
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
