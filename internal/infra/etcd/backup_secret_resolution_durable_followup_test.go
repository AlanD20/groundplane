package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
)

func newDurableBackupSecretCaptureFixture(
	t *testing.T,
	configure func(*memoryHierarchyStore, *testbackupruntime.BackupRunRecord),
	beforeClaim func(*memoryHierarchyStore, testbackupruntime.BackupRunRecord),
) BackupSecretResolutionFixture {
	t.Helper()
	repository, store, run := newBackupRuntimeBareFixture(t)
	if configure != nil {
		configure(store, &run)
	}
	fixedRevision := backupRuntimeCurrentRevision(t, store, run.EnvironmentID)
	publication, err := repository.prepareBackupRunPublication(
		context.Background(), run, backupRuntimeOperationLock(run), fixedRevision,
	)
	if err != nil {
		t.Fatalf("prepare Backup publication: %v", err)
	}
	task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	plan, err := publication.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		publication.clear()
		t.Fatalf("compose Backup publication: %v", err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		publication.clear()
		t.Fatal(err)
	}
	if _, err := idempotency.Apply(context.Background(), marker, plan); err != nil {
		publication.clear()
		t.Fatalf("publish Backup task: %v", err)
	}
	publication.clear()
	if beforeClaim != nil {
		beforeClaim(store, run)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 9300)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, time.Now().UTC(),
	)
	if err != nil || !found || claim.Task.Record.ID != run.TaskID {
		t.Fatalf("claim Backup task = %#v/%v/%v", claim, found, err)
	}
	return BackupSecretResolutionFixture{
		Store: store, Run: run,
		Request: backupsecret.Request{
			TaskID: run.TaskID, AssignmentID: claim.Assignment.Record.AssignmentID,
			AgentID: agentID, AgentGeneration: 1, Deadline: claim.Assignment.Record.Deadline,
			StepID: sealed.Steps[0].StepId,
			Plan:   sealed, Step: sealed.Steps[0],
		},
	}
}

type BackupSecretDeletingProjectFallbackFixture struct {
	BackupSecretResolutionFixture
	PlatformAccessSecretID string
}

func NewBackupSecretDeletingProjectFallbackFixture(t *testing.T) BackupSecretDeletingProjectFallbackFixture {
	t.Helper()

	var projectAccess, platformAccess testkeyvalue.Versioned[testsecrets.Record]
	fixture := newDurableBackupSecretCaptureFixture(
		t,
		func(store *memoryHierarchyStore, run *testbackupruntime.BackupRunRecord) {
			environmentEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentKey(run.EnvironmentID))
			environment, err := testhierarchy.DecodeEnvironment(environmentEntry.Value)
			if err != nil {
				t.Fatal(err)
			}
			projectEntry := mustOptionalKey(t, store, testhierarchy.ProjectKey(environment.ProjectID))
			project, err := testhierarchy.DecodeProject(projectEntry.Value)
			if err != nil {
				t.Fatal(err)
			}
			owner := testsecrets.ProjectOwner(testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
				Record:       project,
				Revision:     projectEntry.ModRevision,
				ReadRevision: projectEntry.ModRevision,
			})
			secrets, err := newSecretRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			projectAccess = createDurableBackupSecret(
				t,
				secrets,
				owner,
				project.ID,
				"C16_ACCESS",
				[]byte("project-access"),
				run.CreatedAt,
				9400,
			)
			platformAccess = createDurableBackupSecret(
				t,
				secrets, testsecrets.PlatformOwner(), "",
				"C16_ACCESS",
				[]byte("platform-access"),
				run.CreatedAt,
				9401,
			)
			createDurableBackupSecret(
				t,
				secrets,
				owner,
				project.ID,
				"C16_SECRET",
				[]byte("project-secret"),
				run.CreatedAt,
				9402,
			)

			connectorEntry := mustOptionalKey(t, store, testconnectors.RecordKey(run.ConnectorID))
			connector, err := testconnectors.DecodeRecord(connectorEntry.Value)
			if err != nil {
				t.Fatal(err)
			}
			connector.Connector.Credentials = map[core.ConnectorCredentialName]core.ConnectorCredential{
				core.ConnectorCredentialAccessKey: {
					Kind: core.ConnectorCredentialSecretRef, SecretRef: "C16_ACCESS",
				},
				core.ConnectorCredentialSecretKey: {
					Kind: core.ConnectorCredentialSecretRef, SecretRef: "C16_SECRET",
				},
			}
			connectorValue, err := testconnectors.EncodeRecord(connector)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(connectorValue)
			updated, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
				{
					Type:  testkeyvalue.MutationPut,
					Key:   testconnectors.RecordKey(run.ConnectorID),
					Value: connectorValue,
				},
				{Type: testkeyvalue.MutationDelete, Key: testconnectors.CredentialValueKey(run.ConnectorID)},
			})
			if err != nil || !updated.Succeeded {
				t.Fatalf("switch Connector to Secret references = %#v, %v", updated, err)
			}
			run.ConnectorRevision = updated.Revision
			run.ConnectorHasDirectCredentials = false
			run.ConnectorCredentialsRevision = 0
		},
		func(store *memoryHierarchyStore, run testbackupruntime.BackupRunRecord) {
			deletionTaskID := ids.NewAt(ids.KindTask, run.CreatedAt, 9450)
			tombstone := testdeletions.DeletionTombstoneRecord{
				TargetKind: testdeletions.DeletionTargetSecret, TargetID: projectAccess.Record.Secret.ID,
				TargetRevision: projectAccess.Revision, TaskID: deletionTaskID,
				Phase: testdeletions.DeletionPhaseFinalizing, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
			}
			value, err := testdeletions.EncodeDeletionTombstone(tombstone)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(value)
			started, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
				{
					Type: testkeyvalue.MutationPut,
					Key: testdeletions.TombstoneKey(
						string(testdeletions.DeletionTargetSecret),
						projectAccess.Record.Secret.ID,
					),
					Value: value,
				},
			})
			if err != nil || !started.Succeeded {
				t.Fatalf("start project Secret deletion = %#v, %v", started, err)
			}
		},
	)
	if platformAccess.Record.Secret.ID == "" {
		t.Fatal("platform fallback fixture did not create its access Secret")
	}
	return BackupSecretDeletingProjectFallbackFixture{
		BackupSecretResolutionFixture: fixture,
		PlatformAccessSecretID:        platformAccess.Record.Secret.ID,
	}
}

func createDurableBackupSecret(
	t *testing.T,
	repository *SecretRepository,
	owner testsecrets.Owner,
	projectID string,
	key string,
	ciphertext []byte,
	now time.Time,
	seed int64,
) testkeyvalue.Versioned[testsecrets.Record] {
	t.Helper()
	id := ids.NewAt(ids.KindSecret, now, seed)
	var record testsecrets.Record
	var err error
	if projectID == "" {
		record, err = testsecrets.NewPlatformRecord(id, key, core.SecretKindEnvVar, "", now)
	} else {
		record, err = testsecrets.NewProjectRecord(id, projectID, key, core.SecretKindEnvVar, "", now)
	}
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(ciphertext)
	value := testsecrets.EncryptedValue{
		SecretID:        id,
		EnvelopeVersion: 1,
		Cipher:          "age-x25519",
		DigestAlgorithm: "sha256",
		CiphertextSHA256: hex.EncodeToString(
			digest[:],
		),
		Ciphertext: append([]byte(nil), ciphertext...),
	}
	defer clear(value.Ciphertext)
	created, err := repository.CreateSecret(context.Background(), owner, record, value)
	if err != nil {
		t.Fatalf("create backup secret %s: %v", key, err)
	}
	return created
}

func NewBackupSecretPublishedPruneFixture(t *testing.T) BackupSecretResolutionFixture {
	t.Helper()
	repository, store, run := newBackupRuntimeBareFixture(t)
	environmentEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentKey(run.EnvironmentID))
	environment, err := testhierarchy.DecodeEnvironment(environmentEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	projectEntry := mustOptionalKey(t, store, testhierarchy.ProjectKey(environment.ProjectID))
	project, err := testhierarchy.DecodeProject(projectEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	createDurableBackupSecret(
		t,
		secrets, testsecrets.ProjectOwner(testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
			Record:       project,
			Revision:     projectEntry.ModRevision,
			ReadRevision: projectEntry.ModRevision,
		}), project.ID,
		"C16_SECRET",
		[]byte("published-prune-secret-envelope"),
		run.CreatedAt,
		9490,
	)
	connectorEntry := mustOptionalKey(t, store, testconnectors.RecordKey(run.ConnectorID))
	connector, err := testconnectors.DecodeRecord(connectorEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	connector.Connector.Credentials = map[core.ConnectorCredentialName]core.ConnectorCredential{
		core.ConnectorCredentialAccessKey: {Kind: core.ConnectorCredentialDirect},
		core.ConnectorCredentialSecretKey: {
			Kind: core.ConnectorCredentialSecretRef, SecretRef: "C16_SECRET",
		},
	}
	connectorValue, err := testconnectors.EncodeRecord(connector)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(connectorValue)
	directCredentials, err := testconnectors.NewEncryptedCredentials(
		run.ConnectorID,
		[]byte("published-prune-direct-envelope"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(directCredentials.Ciphertext)
	directValue, err := testconnectors.EncodeEncryptedCredentials(directCredentials)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(directValue)
	updatedConnector, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testconnectors.RecordKey(run.ConnectorID), Value: connectorValue},
		{Type: testkeyvalue.MutationPut, Key: testconnectors.CredentialValueKey(run.ConnectorID), Value: directValue},
	})
	if err != nil || !updatedConnector.Succeeded {
		t.Fatalf("publish mixed connector credentials = %#v, %v", updatedConnector, err)
	}
	run.ConnectorRevision = updatedConnector.Revision
	run.ConnectorCredentialsRevision = updatedConnector.Revision
	run.ConnectorHasDirectCredentials = true

	operationID := ids.NewAt(ids.KindOperation, run.CreatedAt, 9500)
	source := run.Sources[0]
	source.RecoveryPointID = ids.NewAt(ids.KindRecoveryPoint, run.CreatedAt, 9501)
	source.RecoveryPointCreatedAt = run.CreatedAt
	source.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
		source.RecoveryPointID + "/artifact.bin"
	source.State = testbackupruntime.BackupSourceAttemptStaged
	source.Phase = testbackupruntime.BackupSourcePhasePointCommit
	source.SizeBytes = 123
	source.SHA256 = testBackupDigest
	point := backupRuntimeTestPoint(run, source, run.CreatedAt.Add(time.Second))
	pointRevision := backupRuntimeCurrentRevision(t, store, run.EnvironmentID) + 1
	prune := testbackupruntime.BackupRecoveryPointPruneRecord{
		Point:         point.BackupRecoveryPointSnapshot,
		PointRevision: pointRevision,
		OperationID:   operationID,
		State:         testbackupruntime.BackupPrunePending,
		CreatedAt:     run.CreatedAt,
		UpdatedAt:     run.CreatedAt,
	}
	pruneValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(prune)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pruneValue)
	pointValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	environmentIndex, err := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	sourceIndex, err := testbackupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testbackupruntime.BackupRecoveryPointPruneKey(point.ID),
			Value: pruneValue,
		},
		{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointKey(point.ID), Value: pointValue},
		{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed prune evidence = %#v, %v", seeded, err)
	}
	if seeded.Revision != pointRevision {
		t.Fatalf("seed prune revision = %d, want %d", seeded.Revision, pointRevision)
	}
	pending := []testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{{
		Record: prune, Revision: seeded.Revision, ReadRevision: seeded.Revision,
	}}
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID: run.TaskID, OperationID: operationID, EnvironmentID: run.EnvironmentID,
		RecoveryPointIDs: []string{point.ID}, CreatedAt: run.CreatedAt.Add(2 * time.Second),
	}
	lock := testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID, OperationID: operationID, TaskID: run.TaskID,
		Kind: testbackupruntime.BackupOperationPrune, CreatedAt: dispatch.CreatedAt, UpdatedAt: dispatch.CreatedAt,
	}
	publication, err := repository.prepareBackupPrunePublication(
		context.Background(), pending, dispatch, lock,
	)
	if err != nil {
		t.Fatal(err)
	}
	task, sealed, marker, initiation := backupRuntimePrunePublicationTask(
		t, store, dispatch, pending, publication.conditions,
	)
	plan, err := publication.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		publication.clear()
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		publication.clear()
		t.Fatal(err)
	}
	if _, err := idempotency.Apply(context.Background(), marker, plan); err != nil {
		publication.clear()
		t.Fatalf("publish prune task: %v", err)
	}
	publication.clear()
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.NewAt(ids.KindAgent, dispatch.CreatedAt, 9510)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, time.Now().UTC(),
	)
	if err != nil || !found || claim.Task.Record.ID != dispatch.TaskID {
		t.Fatalf("claim prune task = %#v/%v/%v", claim, found, err)
	}
	storedSourceValue := mustOptionalKey(t, store, testbackuppolicy.BackupSourceKey(point.SourceID))
	storedSource, err := testbackuppolicy.DecodeBackupSourceRecord(storedSourceValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	if storedSource.ID != point.SourceID || storedSource.EnvironmentID != point.EnvironmentID ||
		string(
			storedSource.Kind,
		) != string(
			point.SourceKind,
		) || storedSource.TargetID != point.TargetID {
		t.Fatalf(
			"prune source fixture does not match point: source=%#v point=%#v",
			storedSource,
			point,
		)
	}
	storedEnvironmentValue := mustOptionalKey(t, store, testhierarchy.EnvironmentKey(point.EnvironmentID))
	storedEnvironment, err := testhierarchy.DecodeEnvironment(storedEnvironmentValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	storedConnectorValue := mustOptionalKey(t, store, testconnectors.RecordKey(point.ConnectorID))
	storedConnector, err := testconnectors.DecodeRecord(storedConnectorValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	if storedEnvironment.ID != point.EnvironmentID ||
		storedConnector.Connector.ID != point.ConnectorID ||
		storedConnector.Connector.EnvironmentID != point.EnvironmentID {
		t.Fatalf(
			"prune authority fixture does not match point: environment=%#v connector=%#v point=%#v",
			storedEnvironment,
			storedConnector,
			point,
		)
	}
	return BackupSecretResolutionFixture{
		Store: store,
		Run:   run,
		Request: backupsecret.Request{
			TaskID: dispatch.TaskID, AssignmentID: claim.Assignment.Record.AssignmentID,
			AgentID: agentID, AgentGeneration: 1, Deadline: claim.Assignment.Record.Deadline,
			StepID: sealed.Steps[0].StepId,
			Plan:   sealed, Step: sealed.Steps[0],
		},
	}
}
