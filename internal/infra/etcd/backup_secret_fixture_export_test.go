package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
)

type BackupSecretResolutionFixture struct {
	Store   *memoryHierarchyStore
	Run     testbackupruntime.BackupRunRecord
	Request backupsecret.Request
}

func NewBackupSecretResolutionFixture(
	t *testing.T,
	mixedCredentials bool,
) BackupSecretResolutionFixture {
	t.Helper()
	repository, store, run := newBackupRuntimeBareFixture(t)
	if mixedCredentials {
		configureBackupSecretMixedConnector(t, store, &run)
	}
	runPlan, err := repository.prepareBackupRunPublication(
		context.Background(), run, backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runPlan.clear()
	task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	publication, err := runPlan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idempotency.Apply(context.Background(), marker, publication); err != nil {
		t.Fatal(err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 8800)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 7, time.Now().UTC(),
	)
	if err != nil || !found {
		t.Fatalf("ClaimNextTask() = %#v, %t, %v", claim, found, err)
	}
	return BackupSecretResolutionFixture{
		Store: store, Run: run,
		Request: backupsecret.Request{
			TaskID: run.TaskID, AssignmentID: claim.Assignment.Record.AssignmentID,
			AgentID: agentID, AgentGeneration: 7, Deadline: claim.Assignment.Record.Deadline,
			StepID: sealed.Steps[0].StepId,
			Plan:   sealed, Step: sealed.Steps[0],
		},
	}
}

func configureBackupSecretMixedConnector(
	t *testing.T,
	store *memoryHierarchyStore,
	run *testbackupruntime.BackupRunRecord,
) {
	t.Helper()
	secretID := ids.NewAt(ids.KindSecret, run.CreatedAt, 8810)
	record, err := testsecrets.NewPlatformRecord(
		secretID, "BACKUP_SECRET_KEY", backupsecret.SecretKindEnvVar, "", run.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := secrets.CreateSecret(
		context.Background(), testsecrets.PlatformOwner(), record,
		testSecretEncryptedValue(secretID, "platform-secret"),
	); err != nil {
		t.Fatal(err)
	}
	connectorValue := mustOptionalKey(t, store, testconnectors.RecordKey(run.ConnectorID))
	connector, err := testconnectors.DecodeRecord(connectorValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	secretCredential := connector.Connector.Credentials[backupsecret.CredentialSecretKey]
	secretCredential.Kind = backupsecret.CredentialSourceSecretRef
	secretCredential.SecretRef = "BACKUP_SECRET_KEY"
	connector.Connector.Credentials[backupsecret.CredentialSecretKey] = secretCredential
	encodedConnector, err := testconnectors.EncodeRecord(connector)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := testconnectors.NewEncryptedCredentials(
		run.ConnectorID, []byte(`{"access_key":"direct-access"}`),
	)
	if err != nil {
		clear(encodedConnector)
		t.Fatal(err)
	}
	defer clear(direct.Ciphertext)
	encodedDirect, err := testconnectors.EncodeEncryptedCredentials(direct)
	if err != nil {
		clear(encodedConnector)
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testconnectors.RecordKey(run.ConnectorID), Value: encodedConnector},
		{Type: testkeyvalue.MutationPut, Key: testconnectors.CredentialValueKey(run.ConnectorID), Value: encodedDirect},
	})
	clear(encodedConnector)
	clear(encodedDirect)
	if err != nil || !result.Succeeded {
		t.Fatalf("configure mixed Connector = %#v, %v", result, err)
	}
	run.ConnectorRevision = result.Revision
	run.ConnectorCredentialsRevision = result.Revision
}
