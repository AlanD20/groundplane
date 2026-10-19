package etcd

import (
	"context"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	testbackupsources "github.com/AlanD20/groundplane/internal/infra/etcd/backupsources"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
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
			Frequency:     "0 3 * * *",
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

func mustOptionalKey(t *testing.T, store *memoryHierarchyStore, key string) *testkeyvalue.KeyValue {
	t.Helper()
	result, err := store.Get(context.Background(), key)
	if err != nil || result == nil {
		t.Fatalf("Get(%s) = %#v/%v", key, result, err)
	}
	return result.Entry
}

type connectorDeletionFixture struct {
	repository  *BackupPolicyRepository
	store       *memoryHierarchyStore
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	project     testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	source      testkeyvalue.Versioned[testbackuppolicy.BackupSourceRecord]
	connector   testkeyvalue.Versioned[testconnectors.Record]
	now         time.Time
}
