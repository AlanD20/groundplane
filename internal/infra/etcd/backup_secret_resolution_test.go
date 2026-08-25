package etcd

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: evidence ownership ends explicitly, so every returned encrypted
// credential buffer must be released even after partial consumption.
func TestBackupSecretResolutionEvidenceClearZeroesCiphertext(t *testing.T) {
	// Rationale: the reader transfers encrypted bytes to the Controller, which
	// must be able to release every partial buffer on success or failure.
	connectorCiphertext := bytes.Repeat([]byte{0xA1}, 16)
	secretCiphertext := bytes.Repeat([]byte{0xB2}, 16)
	evidence := BackupSecretResolutionEvidence{
		Credentials: ConnectorEncryptedCredentials{Ciphertext: connectorCiphertext},
		SecretValues: []BackupSecretValueEvidence{{
			Value: SecretEncryptedValue{Ciphertext: secretCiphertext},
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
	value, err := encodeDeletionTombstone(DeletionTombstoneRecord{
		TargetKind:     DeletionTargetProject,
		TargetID:       "prj_01H00000000000000000000000",
		TargetRevision: 1,
		TaskID:         "task_01H00000000000000000000000",
		Phase:          DeletionPhaseFinalizing,
		CreatedAt:      time.Unix(1, 0).UTC(),
		UpdatedAt:      time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("encodeDeletionTombstone() error = %v", err)
	}
	defer clear(value)
	if err := requireNoDeletionFence(
		&KeyValue{Value: value}, DeletionTargetEnvironment, "env_01H00000000000000000000000",
	); err == nil {
		t.Fatal("requireNoDeletionFence() error = nil")
	}
}

// Rationale: a Connector mutation after run publication must not replace the
// exact metadata revision pinned by every capture step.
func TestBackupSecretResolutionRejectsChangedPinnedConnector(t *testing.T) {
	fixture := newBackupSecretResolutionFixture(t, nil)
	connector := mustOptionalKey(t, fixture.store, connectorRecordKey(fixture.run.ConnectorID))
	value := append([]byte(nil), connector.Value...)
	result, err := fixture.store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: connectorRecordKey(fixture.run.ConnectorID), Value: value,
	}})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("mutate Connector = %#v, %v", result, err)
	}
	if _, err := fixture.reader.ResolveBackupSecretEvidence(
		context.Background(), fixture.request,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("ResolveBackupSecretEvidence() error = %v, want state conflict", err)
	}
}

// Rationale: token authentication identifies a session, but decryption
// authority additionally requires the exact durable assignment generation.
func TestBackupSecretResolutionRejectsStaleAssignmentAndGeneration(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*backupsecret.Request)
	}{
		{name: "assignment", mutate: func(request *backupsecret.Request) {
			request.AssignmentID = ids.NewAt(ids.KindAssignment, time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC), 8801)
		}},
		{name: "generation", mutate: func(request *backupsecret.Request) {
			request.AgentGeneration++
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBackupSecretResolutionFixture(t, nil)
			request := fixture.request
			test.mutate(&request)
			if _, err := fixture.reader.ResolveBackupSecretEvidence(
				context.Background(), request,
			); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				t.Fatalf("ResolveBackupSecretEvidence() error = %v, want state conflict", err)
			}
		})
	}
}

// Rationale: a sealed plan naming another Connector cannot borrow the active
// Task assignment even when its protobuf shape is otherwise valid.
func TestBackupSecretResolutionRejectsConnectorPlanMismatch(t *testing.T) {
	fixture := newBackupSecretResolutionFixture(t, nil)
	mutated := proto.Clone(fixture.request.Plan).(*agentpb.ExecutionPlan)
	mutated.PlanHash = nil
	mutated.Steps[0].GetBackupSourceCapture().ConnectorRevision++
	sealed, err := executionplan.Seal(mutated)
	if err != nil {
		t.Fatal(err)
	}
	request := fixture.request
	request.Plan = sealed
	request.Step = sealed.Steps[0]
	if _, err := fixture.reader.ResolveBackupSecretEvidence(
		context.Background(), request,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("ResolveBackupSecretEvidence() error = %v, want state conflict", err)
	}
}

// Rationale: a missing Project override must resolve the same late-bound key
// from platform scope while retaining the run-pinned mixed Connector bundle.
func TestBackupSecretResolutionUsesPlatformFallbackForMixedCredentials(t *testing.T) {
	fixture := newBackupSecretResolutionFixture(t, configureBackupSecretMixedConnector)
	evidence, err := fixture.reader.ResolveBackupSecretEvidence(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("ResolveBackupSecretEvidence() error = %v", err)
	}
	defer evidence.Clear()
	if !evidence.HasCredentials || string(evidence.Credentials.Ciphertext) != `{"access_key":"direct-access"}` ||
		len(evidence.SecretValues) != 1 || evidence.SecretValues[0].Name != backupsecret.CredentialSecretKey ||
		string(evidence.SecretValues[0].Value.Ciphertext) != "platform-secret" {
		t.Fatalf("mixed fallback evidence = %#v", evidence)
	}
}

// Rationale: Volume snapshots have no arbitrary Service-count cap; their
// fixed-revision reads must be split only by the accepted etcd op ceiling.
func TestBackupSecretFixedReadBatchesArbitraryVolumeServiceEvidence(t *testing.T) {
	store := newMemoryHierarchyStore()
	keys := make([]string, maximumTransactionOperations*2+7)
	mutations := make([]Mutation, len(keys))
	for index := range keys {
		keys[index] = "/test/backup-volume-services/" + ids.NewAt(
			ids.KindService, time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC), int64(index+1),
		)
		mutations[index] = Mutation{Type: MutationPut, Key: keys[index], Value: []byte{byte(index)}}
	}
	result, err := store.Transact(context.Background(), nil, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed fixed read = %#v, %v", result, err)
	}
	bounded := &boundedBackupSecretGetManyStore{backupSecretResolutionStore: store}
	reader, err := NewBackupSecretResolutionReader(bounded)
	if err != nil {
		t.Fatal(err)
	}
	read, err := reader.readFixed(context.Background(), keys, result.Revision)
	if err != nil {
		t.Fatalf("readFixed() error = %v", err)
	}
	defer clearKeyValues(read.Values)
	if len(read.Values) != len(keys) || bounded.calls != 3 {
		t.Fatalf("read values/calls = %d/%d, want %d/3", len(read.Values), bounded.calls, len(keys))
	}
}

type backupSecretResolutionFixture struct {
	store   *memoryHierarchyStore
	run     BackupRunRecord
	reader  *BackupSecretResolutionReader
	request backupsecret.Request
}

func newBackupSecretResolutionFixture(
	t *testing.T,
	configure func(*testing.T, *memoryHierarchyStore, *BackupRunRecord),
) backupSecretResolutionFixture {
	t.Helper()
	repository, store, run := newBackupRuntimeBareFixture(t)
	if configure != nil {
		configure(t, store, &run)
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
	idempotency, err := newIdempotencyRepository(store)
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
		context.Background(), agentID, 7, run.CreatedAt.Add(time.Second),
	)
	if err != nil || !found {
		t.Fatalf("ClaimNextTask() = %#v, %t, %v", claim, found, err)
	}
	reader, err := NewBackupSecretResolutionReader(store)
	if err != nil {
		t.Fatal(err)
	}
	reader.now = func() time.Time { return claim.Assignment.Record.AssignedAt }
	return backupSecretResolutionFixture{
		store: store, run: run, reader: reader,
		request: backupsecret.Request{
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
	run *BackupRunRecord,
) {
	t.Helper()
	secretID := ids.NewAt(ids.KindSecret, run.CreatedAt, 8810)
	record, err := NewPlatformSecretRecord(
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
		context.Background(), PlatformSecretOwner(), record,
		testSecretEncryptedValue(secretID, "platform-secret"),
	); err != nil {
		t.Fatal(err)
	}
	connectorValue := mustOptionalKey(t, store, connectorRecordKey(run.ConnectorID))
	connector, err := decodeConnectorRecord(connectorValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	secretCredential := connector.Connector.Credentials[backupsecret.CredentialSecretKey]
	secretCredential.Kind = backupsecret.CredentialSourceSecretRef
	secretCredential.SecretRef = "BACKUP_SECRET_KEY"
	connector.Connector.Credentials[backupsecret.CredentialSecretKey] = secretCredential
	encodedConnector, err := encodeConnectorRecord(connector)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := NewConnectorEncryptedCredentials(
		run.ConnectorID, []byte(`{"access_key":"direct-access"}`),
	)
	if err != nil {
		clear(encodedConnector)
		t.Fatal(err)
	}
	defer clear(direct.Ciphertext)
	encodedDirect, err := encodeConnectorEncryptedCredentials(direct)
	if err != nil {
		clear(encodedConnector)
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: connectorRecordKey(run.ConnectorID), Value: encodedConnector},
		{Type: MutationPut, Key: connectorCredentialValueKey(run.ConnectorID), Value: encodedDirect},
	})
	clear(encodedConnector)
	clear(encodedDirect)
	if err != nil || !result.Succeeded {
		t.Fatalf("configure mixed Connector = %#v, %v", result, err)
	}
	run.ConnectorRevision = result.Revision
	run.ConnectorCredentialsRevision = result.Revision
}

type boundedBackupSecretGetManyStore struct {
	backupSecretResolutionStore
	calls int
}

func (store *boundedBackupSecretGetManyStore) GetMany(
	ctx context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	if len(request.Keys) > maximumTransactionOperations {
		return nil, errs.New(errs.KindInternal, "test observed oversized fixed read")
	}
	store.calls++
	return store.backupSecretResolutionStore.GetMany(ctx, request)
}
