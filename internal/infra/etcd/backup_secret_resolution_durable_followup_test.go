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
)

type durableBackupSecretCaptureFixture struct {
	reader  *BackupSecretResolutionReader
	store   *memoryHierarchyStore
	request backupsecret.Request
	run     BackupRunRecord
}

func newDurableBackupSecretCaptureFixture(
	t *testing.T,
	configure func(*memoryHierarchyStore, *BackupRunRecord),
	beforeClaim func(*memoryHierarchyStore, BackupRunRecord),
) durableBackupSecretCaptureFixture {
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
	idempotency, err := newIdempotencyRepository(store)
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
		context.Background(), agentID, 1, run.CreatedAt.Add(time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != run.TaskID {
		t.Fatalf("claim Backup task = %#v/%v/%v", claim, found, err)
	}
	reader, err := NewBackupSecretResolutionReader(store)
	if err != nil {
		t.Fatal(err)
	}
	reader.now = func() time.Time { return claim.Assignment.Record.AssignedAt }
	return durableBackupSecretCaptureFixture{
		reader: reader, store: store, run: run,
		request: backupsecret.Request{
			TaskID: run.TaskID, AssignmentID: claim.Assignment.Record.AssignmentID,
			AgentID: agentID, AgentGeneration: 1, Deadline: claim.Assignment.Record.Deadline,
			StepID: sealed.Steps[0].StepId,
			Plan:   sealed, Step: sealed.Steps[0],
		},
	}
}

// Rationale: direct Connector ciphertext is subordinate immutable evidence;
// replacing only that envelope after publication must invalidate delivery.
func TestResolveBackupSecretEvidenceRejectsDirectEnvelopeRevisionMutation(t *testing.T) {
	t.Parallel()

	fixture := newDurableBackupSecretCaptureFixture(
		t,
		nil,
		func(store *memoryHierarchyStore, run BackupRunRecord) {
			credentials, err := NewConnectorEncryptedCredentials(
				run.ConnectorID, []byte("mutated-direct-credentials"),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(credentials.Ciphertext)
			value, err := encodeConnectorEncryptedCredentials(credentials)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(value)
			mutated, err := store.Transact(context.Background(), nil, []Mutation{{
				Type: MutationPut, Key: connectorCredentialValueKey(run.ConnectorID), Value: value,
			}})
			if err != nil || !mutated.Succeeded {
				t.Fatalf("mutate direct credential envelope = %#v, %v", mutated, err)
			}
		},
	)
	evidence, err := fixture.reader.ResolveBackupSecretEvidence(
		context.Background(),
		fixture.request,
	)
	evidence.Clear()
	if err == nil {
		t.Fatal("direct credential envelope revision mutation was accepted")
	}
}

// Rationale: ADR0024's durable assignment deadline is an exclusive execution
// fence; reconnects at or after it must fail before timeout maintenance runs.
func TestResolveBackupSecretEvidenceAssignmentDeadlineBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		offset  time.Duration
		wantErr bool
	}{
		{name: "before", offset: -time.Nanosecond},
		{name: "at", wantErr: true},
		{name: "after before maintenance", offset: time.Nanosecond, wantErr: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			fixture := newDurableBackupSecretCaptureFixture(t, nil, nil)
			fixture.reader.now = func() time.Time {
				return fixture.request.Deadline.Add(test.offset)
			}
			evidence, err := fixture.reader.ResolveBackupSecretEvidence(
				context.Background(), fixture.request,
			)
			evidence.Clear()
			if test.wantErr && err == nil {
				t.Fatal("expired durable assignment was accepted")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("active durable assignment was rejected: %v", err)
			}
		})
	}
}

// Rationale: ADR0045 defines project-first lookup with platform fallback, so a
// finalizing project candidate is hidden and the valid platform value wins.
func TestResolveBackupSecretEvidenceSkipsDeletingProjectSecretForPlatformFallback(t *testing.T) {
	t.Parallel()

	var projectAccess, platformAccess Versioned[SecretRecord]
	fixture := newDurableBackupSecretCaptureFixture(
		t,
		func(store *memoryHierarchyStore, run *BackupRunRecord) {
			environmentEntry := mustOptionalKey(t, store, environmentKey(run.EnvironmentID))
			environment, err := decodeEnvironment(environmentEntry.Value)
			if err != nil {
				t.Fatal(err)
			}
			projectEntry := mustOptionalKey(t, store, projectKey(environment.ProjectID))
			project, err := decodeProject(projectEntry.Value)
			if err != nil {
				t.Fatal(err)
			}
			owner := ProjectSecretOwner(Versioned[ProjectRecord]{
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
				secrets,
				PlatformSecretOwner(),
				"",
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

			connectorEntry := mustOptionalKey(t, store, connectorRecordKey(run.ConnectorID))
			connector, err := decodeConnectorRecord(connectorEntry.Value)
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
			connectorValue, err := encodeConnectorRecord(connector)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(connectorValue)
			updated, err := store.Transact(context.Background(), nil, []Mutation{
				{
					Type:  MutationPut,
					Key:   connectorRecordKey(run.ConnectorID),
					Value: connectorValue,
				},
				{Type: MutationDelete, Key: connectorCredentialValueKey(run.ConnectorID)},
			})
			if err != nil || !updated.Succeeded {
				t.Fatalf("switch Connector to Secret references = %#v, %v", updated, err)
			}
			run.ConnectorRevision = updated.Revision
			run.ConnectorHasDirectCredentials = false
			run.ConnectorCredentialsRevision = 0
		},
		func(store *memoryHierarchyStore, run BackupRunRecord) {
			deletionTaskID := ids.NewAt(ids.KindTask, run.CreatedAt, 9450)
			tombstone := DeletionTombstoneRecord{
				TargetKind: DeletionTargetSecret, TargetID: projectAccess.Record.Secret.ID,
				TargetRevision: projectAccess.Revision, TaskID: deletionTaskID,
				Phase: DeletionPhaseFinalizing, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
			}
			value, err := encodeDeletionTombstone(tombstone)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(value)
			started, err := store.Transact(context.Background(), nil, []Mutation{
				{
					Type: MutationPut,
					Key: deletionTombstoneKey(
						string(DeletionTargetSecret),
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
	evidence, err := fixture.reader.ResolveBackupSecretEvidence(
		context.Background(),
		fixture.request,
	)
	defer evidence.Clear()
	if err != nil {
		t.Fatalf("deleting project Secret blocked valid platform fallback: %v", err)
	}
	for _, value := range evidence.SecretValues {
		if value.Name == backupsecret.CredentialAccessKey {
			if value.Value.SecretID != platformAccess.Record.Secret.ID {
				t.Fatalf(
					"access Secret = %q, want platform %q",
					value.Value.SecretID,
					platformAccess.Record.Secret.ID,
				)
			}
			return
		}
	}
	t.Fatal("platform access Secret was not resolved")
}

func createDurableBackupSecret(
	t *testing.T,
	repository *SecretRepository,
	owner SecretOwner,
	projectID string,
	key string,
	ciphertext []byte,
	now time.Time,
	seed int64,
) Versioned[SecretRecord] {
	t.Helper()
	id := ids.NewAt(ids.KindSecret, now, seed)
	var record SecretRecord
	var err error
	if projectID == "" {
		record, err = NewPlatformSecretRecord(id, key, core.SecretKindEnvVar, "", now)
	} else {
		record, err = NewProjectSecretRecord(id, projectID, key, core.SecretKindEnvVar, "", now)
	}
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(ciphertext)
	value := SecretEncryptedValue{
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

// Rationale: prune resolution must revalidate the historical pending authority,
// current assignment, dispatch, plan, object evidence, and mixed credentials.
func TestResolveBackupSecretEvidenceForPublishedPruneAssignment(t *testing.T) {
	t.Parallel()

	repository, store, run := newBackupRuntimeBareFixture(t)
	environmentEntry := mustOptionalKey(t, store, environmentKey(run.EnvironmentID))
	environment, err := decodeEnvironment(environmentEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	projectEntry := mustOptionalKey(t, store, projectKey(environment.ProjectID))
	project, err := decodeProject(projectEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	createDurableBackupSecret(
		t,
		secrets,
		ProjectSecretOwner(Versioned[ProjectRecord]{
			Record:       project,
			Revision:     projectEntry.ModRevision,
			ReadRevision: projectEntry.ModRevision,
		}),
		project.ID,
		"C16_SECRET",
		[]byte("published-prune-secret-envelope"),
		run.CreatedAt,
		9490,
	)
	connectorEntry := mustOptionalKey(t, store, connectorRecordKey(run.ConnectorID))
	connector, err := decodeConnectorRecord(connectorEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	connector.Connector.Credentials = map[core.ConnectorCredentialName]core.ConnectorCredential{
		core.ConnectorCredentialAccessKey: {Kind: core.ConnectorCredentialDirect},
		core.ConnectorCredentialSecretKey: {
			Kind: core.ConnectorCredentialSecretRef, SecretRef: "C16_SECRET",
		},
	}
	connectorValue, err := encodeConnectorRecord(connector)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(connectorValue)
	directCredentials, err := NewConnectorEncryptedCredentials(
		run.ConnectorID,
		[]byte("published-prune-direct-envelope"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(directCredentials.Ciphertext)
	directValue, err := encodeConnectorEncryptedCredentials(directCredentials)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(directValue)
	updatedConnector, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: connectorRecordKey(run.ConnectorID), Value: connectorValue},
		{Type: MutationPut, Key: connectorCredentialValueKey(run.ConnectorID), Value: directValue},
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
	source.State = BackupSourceAttemptStaged
	source.Phase = BackupSourcePhasePointCommit
	source.SizeBytes = 123
	source.SHA256 = testBackupDigest
	point := backupRuntimeTestPoint(run, source, run.CreatedAt.Add(time.Second))
	pointRevision := backupRuntimeCurrentRevision(t, store, run.EnvironmentID) + 1
	prune := BackupRecoveryPointPruneRecord{
		Point:         point.BackupRecoveryPointSnapshot,
		PointRevision: pointRevision,
		OperationID:   operationID,
		State:         BackupPrunePending,
		CreatedAt:     run.CreatedAt,
		UpdatedAt:     run.CreatedAt,
	}
	pruneValue, err := encodeBackupRecoveryPointPruneRecord(prune)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pruneValue)
	pointValue, err := encodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	environmentIndex, err := backupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	sourceIndex, err := backupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := backupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: backupRecoveryPointPruneKey(point.ID), Value: pruneValue},
		{Type: MutationPut, Key: backupRecoveryPointKey(point.ID), Value: pointValue},
		{Type: MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed prune evidence = %#v, %v", seeded, err)
	}
	if seeded.Revision != pointRevision {
		t.Fatalf("seed prune revision = %d, want %d", seeded.Revision, pointRevision)
	}
	pending := []Versioned[BackupRecoveryPointPruneRecord]{{
		Record: prune, Revision: seeded.Revision, ReadRevision: seeded.Revision,
	}}
	dispatch := BackupRecoveryPointPruneDispatchRecord{
		TaskID: run.TaskID, OperationID: operationID, EnvironmentID: run.EnvironmentID,
		RecoveryPointIDs: []string{point.ID}, CreatedAt: run.CreatedAt.Add(2 * time.Second),
	}
	lock := BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID, OperationID: operationID, TaskID: run.TaskID,
		Kind: BackupOperationPrune, CreatedAt: dispatch.CreatedAt, UpdatedAt: dispatch.CreatedAt,
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
	idempotency, err := newIdempotencyRepository(store)
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
		context.Background(), agentID, 1, dispatch.CreatedAt.Add(time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != dispatch.TaskID {
		t.Fatalf("claim prune task = %#v/%v/%v", claim, found, err)
	}
	reader, err := NewBackupSecretResolutionReader(store)
	if err != nil {
		t.Fatal(err)
	}
	reader.now = func() time.Time { return claim.Assignment.Record.AssignedAt }
	storedSourceValue := mustOptionalKey(t, store, backupSourceKey(point.SourceID))
	storedSource, err := decodeBackupSourceRecord(storedSourceValue.Value)
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
	storedEnvironmentValue := mustOptionalKey(t, store, environmentKey(point.EnvironmentID))
	storedEnvironment, err := decodeEnvironment(storedEnvironmentValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	storedConnectorValue := mustOptionalKey(t, store, connectorRecordKey(point.ConnectorID))
	storedConnector, err := decodeConnectorRecord(storedConnectorValue.Value)
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
	request := backupsecret.Request{
		TaskID: dispatch.TaskID, AssignmentID: claim.Assignment.Record.AssignmentID,
		AgentID: agentID, AgentGeneration: 1, Deadline: claim.Assignment.Record.Deadline,
		StepID: sealed.Steps[0].StepId,
		Plan:   sealed, Step: sealed.Steps[0],
	}
	evidence, err := reader.ResolveBackupSecretEvidence(context.Background(), request)
	defer evidence.Clear()
	if err != nil {
		t.Fatalf("resolve backup secret evidence for prune: %v", err)
	}
	if evidence.Dispatch == nil {
		t.Fatal("prune resolution did not return its durable dispatch evidence")
	}
	if !evidence.HasCredentials || len(evidence.SecretValues) != 1 ||
		evidence.SecretValues[0].Name != backupsecret.CredentialSecretKey ||
		evidence.SecretValues[0].Reference != "C16_SECRET" {
		t.Fatalf("published prune credential evidence = %#v", evidence)
	}
}
