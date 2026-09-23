package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	testbackupsources "github.com/AlanD20/groundplane/internal/infra/etcd/backupsources"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func newConnectorDeletionFixture(t *testing.T) *connectorDeletionFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 8, 23, 18, 0, 0, 0, time.UTC)
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenantID := ids.NewAt(ids.KindTenant, now, 2600)
	if _, err := hierarchy.CreateTenant(ctx, testhierarchy.TenantRecord{
		ID: tenantID, Slug: "sample-tenant", Name: "Sample Tenant",
	}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := hierarchy.CreateProject(ctx, testhierarchy.ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 2601), TenantID: tenantID,
		Slug: "sample-project", Name: "Sample Project", Kind: testhierarchy.ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, now, 2602)
	environment, err := hierarchy.CreateEnvironment(ctx, testhierarchy.EnvironmentRecord{
		ID:                environmentID,
		ProjectID:         project.Record.ID,
		Name:              "production",
		NetworkPool:       "10.242.0.0/24",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + project.Record.ID + "/" + environmentID,
		ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, now, 2603),
		CreatedAt:         now,
	})
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	repository, err := newBackupPolicyRepository(store)
	if err != nil {
		t.Fatalf("newBackupPolicyRepository() error = %v", err)
	}
	repository.now = func() time.Time { return now }
	configSource, err := testbackupsources.EnsureBackupSource(
		ctx, store, repository.now, environment, project, "config", environment.Record.ID,
	)
	if err != nil {
		t.Fatalf("EnsureBackupSource(config) error = %v", err)
	}
	fixture := &connectorDeletionFixture{
		repository:  repository,
		store:       store,
		environment: environment,
		project:     project,
		source:      configSource,
		now:         now,
	}
	record := testConnectorRecord(t, environment.Record.ID, now, 2610, "primary-backups")
	credentials, err := testconnectors.NewEncryptedCredentials(
		record.Connector.ID, []byte("sealed-connector-credentials"),
	)
	if err != nil {
		t.Fatalf("NewConnectorEncryptedCredentials() error = %v", err)
	}
	connectors, err := newConnectorRepository(store)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	fixture.connector, err = connectors.CreateConnector(
		ctx, environment, project, record, credentials,
	)
	if err != nil {
		t.Fatalf("CreateConnector() error = %v", err)
	}
	return fixture
}

func connectorDeletionPolicyCandidate(
	t *testing.T,
	fixture *connectorDeletionFixture,
) testbackuppolicymutations.PreparedBackupPolicyReplacement {
	t.Helper()
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(
		context.Background(),
		testbackuppolicy.BackupPolicyReplacementInput{
			EnvironmentID: fixture.environment.Record.ID,
			Enabled:       true,
			Frequency:     "*-*-* 03:00:00",
			Keep:          7,
			Encryption:    "age",
			ConnectorID:   fixture.connector.Record.Connector.ID,
			Sources: []testbackuppolicy.BackupPolicySourceSelection{{
				Kind: fixture.source.Record.Kind, TargetID: fixture.source.Record.TargetID,
			}},
		},
	)
	if err != nil {
		t.Fatalf("PrepareBackupPolicyReplacement() error = %v", err)
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity() error = %v", err)
	}
	prepared, err = fixture.repository.SupplyBackupPolicyInitialKey(
		context.Background(),
		prepared,
		testbackuppolicymutations.BackupPolicyInitialKeyMaterial{
			Recipient: identity.Recipient().String(), Ciphertext: []byte("controller-sealed-age-identity"),
		},
	)
	if err != nil {
		t.Fatalf("SupplyBackupPolicyInitialKey() error = %v", err)
	}
	return prepared
}

func assertConnectorDeletionKeyState(
	t *testing.T,
	fixture *connectorDeletionFixture,
	task TaskRecord,
	targetPresent bool,
	fencePresent bool,
) {
	t.Helper()
	targetKeys := []string{
		testconnectors.RecordKey(task.Target),
		testconnectors.ConnectorEnvironmentKey(fixture.environment.Record.ID, task.Target),
		testconnectors.ConnectorNameKey(fixture.environment.Record.ID, fixture.connector.Record.Connector.Name),
		testconnectors.CredentialValueKey(task.Target),
	}
	for _, key := range targetKeys {
		entry := mustOptionalKey(t, fixture.store, key)
		if (entry != nil) != targetPresent {
			t.Fatalf("target key %s present = %t, want %t", key, entry != nil, targetPresent)
		}
	}
	fenceKeys := []string{
		testdeletions.TombstoneKey(string(testdeletions.DeletionTargetConnector), task.Target),
		testconnectors.RemovalIntentKey(task.ID),
	}
	for _, key := range fenceKeys {
		entry := mustOptionalKey(t, fixture.store, key)
		if (entry != nil) != fencePresent {
			t.Fatalf("fence key %s present = %t, want %t", key, entry != nil, fencePresent)
		}
	}
	replay := mustOptionalKey(t, fixture.store, connectorDeletionReplayTargetKey(t, task))
	if replay == nil {
		t.Fatal("connector deletion replay target is missing")
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment,
		ScopeID:   fixture.environment.Record.ID,
		Method:    http.MethodDelete,
		Route:     connectorDeletionRoute,
		Key:       task.IdempotencyKey,
	})
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	if err := testidempotency.DecodeReplayTargetReference(replay.Value, markerKey); err != nil {
		t.Fatalf("decodeReplayTargetReference() error = %v", err)
	}
}

func connectorDeletionReplayTargetKey(t *testing.T, task TaskRecord) string {
	t.Helper()
	key, err := testidempotency.IdempotencyReplayTargetKey(
		testidempotency.IdempotencyReplayTarget{
			Kind: testidempotency.IdempotencyReplayTargetConnector,
			ID:   task.Target,
		},
		http.MethodDelete,
		connectorDeletionRoute,
		task.IdempotencyKey,
	)
	if err != nil {
		t.Fatalf("idempotencyReplayTargetKey() error = %v", err)
	}
	return key
}

func connectorDeletionPutKey(t *testing.T, store *memoryHierarchyStore, key string, value []byte) {
	t.Helper()
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: key, Value: value,
	}})
	if err != nil || !result.Succeeded {
		t.Fatalf("put %s result/error = %#v/%v", key, result, err)
	}
}

func connectorDeletionDeleteKey(t *testing.T, store *memoryHierarchyStore, key string) {
	t.Helper()
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationDelete, Key: key,
	}})
	if err != nil || !result.Succeeded {
		t.Fatalf("delete %s result/error = %#v/%v", key, result, err)
	}
}

func assertConnectorDeletionVisible(
	t *testing.T,
	repository *ConnectorRepository,
	connectorID string,
) {
	t.Helper()
	current, err := repository.GetConnector(context.Background(), connectorID)
	if err != nil || current.Record.Connector.ID != connectorID {
		t.Fatalf("GetConnector(visible) = %#v/%v", current, err)
	}
}

func assertConnectorCredentialPresence(
	t *testing.T,
	store *memoryHierarchyStore,
	connectorID string,
	want bool,
) {
	t.Helper()
	entry := mustOptionalKey(t, store, testconnectors.CredentialValueKey(connectorID))
	if (entry != nil) != want {
		t.Fatalf("Connector credential present = %t, want %t", entry != nil, want)
	}
}

func assertConnectorIntentPresence(
	t *testing.T,
	store *memoryHierarchyStore,
	taskID string,
	want bool,
) {
	t.Helper()
	entry := mustOptionalKey(t, store, testconnectors.RemovalIntentKey(taskID))
	if (entry != nil) != want {
		t.Fatalf("Connector removal intent present = %t, want %t", entry != nil, want)
	}
}

func mustOptionalKey(t *testing.T, store *memoryHierarchyStore, key string) *testkeyvalue.KeyValue {
	t.Helper()
	result, err := store.Get(context.Background(), key)
	if err != nil || result == nil {
		t.Fatalf("Get(%s) = %#v/%v", key, result, err)
	}
	return result.Entry
}
