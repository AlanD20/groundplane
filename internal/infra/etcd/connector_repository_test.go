package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestConnectorRecordNormalizesCompleteS3Decision(t *testing.T) {
	// Rationale: persistence must not leave endpoint addressing or object-key
	// prefix behavior to an SDK default.
	record, err := NewConnectorRecord(core.Connector{
		ID: ids.NewAt(
			ids.KindConnector,
			time.Date(2026, 8, 23, 13, 0, 0, 0, time.UTC),
			1,
		),
		EnvironmentID: ids.NewAt(
			ids.KindEnvironment,
			time.Date(2026, 8, 23, 13, 0, 0, 0, time.UTC),
			2,
		),
		Name:      "primary-backups",
		Kind:      core.ConnectorKindS3Compatible,
		Endpoint:  "https://objects.example.test/",
		Bucket:    "groundplane-backups",
		Prefix:    "production/daily",
		Region:    "auto",
		PathStyle: true,
		Credentials: map[core.ConnectorCredentialName]core.ConnectorCredential{
			core.ConnectorCredentialAccessKey: {Kind: core.ConnectorCredentialDirect},
			core.ConnectorCredentialSecretKey: {Kind: core.ConnectorCredentialDirect},
		},
	})
	if err != nil {
		t.Fatalf("NewConnectorRecord() error = %v", err)
	}
	if record.Connector.Endpoint != "https://objects.example.test" ||
		record.Connector.Prefix != "production/daily/" {
		t.Fatalf("NewConnectorRecord() = %#v", record.Connector)
	}

	record.Connector.Credentials[core.ConnectorCredentialAccessKey] = core.ConnectorCredential{
		Kind: core.ConnectorCredentialSecretRef,
	}
	if _, err := encodeConnectorRecord(record); func() bool {
		kind, ok := errs.KindOf(err)
		return !ok || kind != errs.KindValidationFailed
	}() {
		t.Fatalf("encodeConnectorRecord(invalid credential) error = %v", err)
	}
}

func TestConnectorRepositoryCreatesListsAndReadsEncryptedCredentialsAtomically(t *testing.T) {
	// Rationale: listable metadata, scoped indexes, and encrypted direct values
	// must become visible at one etcd revision or not at all.
	store, environment, project := testConnectorHierarchy(t)
	repository, err := newConnectorRepository(store)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 23, 14, 0, 0, 0, time.UTC)
	record := testConnectorRecord(t, environment.Record.ID, now, 10, "primary-backups")
	credentials, err := NewConnectorEncryptedCredentials(
		record.Connector.ID,
		[]byte("age-ciphertext"),
	)
	if err != nil {
		t.Fatalf("NewConnectorEncryptedCredentials() error = %v", err)
	}
	created, err := repository.CreateConnector(
		context.Background(), environment, project, record, credentials,
	)
	if err != nil {
		t.Fatalf("CreateConnector() error = %v", err)
	}
	if created.Revision <= 0 || created.ReadRevision != created.Revision {
		t.Fatalf("CreateConnector() version = %#v", created)
	}

	got, err := repository.GetConnector(context.Background(), record.Connector.ID)
	if err != nil || got.Record.Connector.Name != "primary-backups" {
		t.Fatalf("GetConnector() = %#v, %v", got, err)
	}
	storedCredentials, err := repository.GetConnectorCredentials(context.Background(), got)
	if err != nil || string(storedCredentials.Ciphertext) != "age-ciphertext" {
		t.Fatalf("GetConnectorCredentials() = %#v, %v", storedCredentials, err)
	}
	clear(storedCredentials.Ciphertext)
	page, err := repository.ListConnectors(
		context.Background(), environment.Record.ID, PageRequest{Limit: 10},
	)
	if err != nil || len(page.Items) != 1 ||
		page.Items[0].Record.Connector.ID != record.Connector.ID {
		t.Fatalf("ListConnectors() = %#v, %v", page, err)
	}

	duplicate := testConnectorRecord(
		t,
		environment.Record.ID,
		now.Add(time.Second),
		11,
		"primary-backups",
	)
	duplicateCredentials, err := NewConnectorEncryptedCredentials(
		duplicate.Connector.ID, []byte("other-ciphertext"),
	)
	if err != nil {
		t.Fatalf("NewConnectorEncryptedCredentials(duplicate) error = %v", err)
	}
	_, err = repository.CreateConnector(
		context.Background(), environment, project, duplicate, duplicateCredentials,
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("CreateConnector(duplicate name) error = %v", err)
	}
	for _, key := range []string{
		connectorRecordKey(duplicate.Connector.ID),
		connectorCredentialValueKey(duplicate.Connector.ID),
	} {
		value, getErr := store.Get(context.Background(), key)
		if getErr != nil || value.Entry != nil {
			t.Fatalf("duplicate artifact %q = %#v, %v", key, value, getErr)
		}
	}
}

func TestConnectorRepositoryRejectsCrossEnvironmentOwnership(t *testing.T) {
	store, environment, project := testConnectorHierarchy(t)
	repository, err := newConnectorRepository(store)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	record := testConnectorRecord(
		t,
		ids.NewAt(ids.KindEnvironment, time.Date(2026, 8, 23, 15, 0, 0, 0, time.UTC), 30),
		time.Date(2026, 8, 23, 15, 0, 0, 0, time.UTC),
		31,
		"wrong-owner",
	)
	credentials, err := NewConnectorEncryptedCredentials(record.Connector.ID, []byte("ciphertext"))
	if err != nil {
		t.Fatalf("NewConnectorEncryptedCredentials() error = %v", err)
	}
	_, err = repository.CreateConnector(
		context.Background(), environment, project, record, credentials,
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindConnectorScopeInvalid {
		t.Fatalf("CreateConnector(cross environment) error = %v", err)
	}
}

func testConnectorHierarchy(
	t *testing.T,
) (*memoryHierarchyStore, Versioned[EnvironmentRecord], Versioned[ProjectRecord]) {
	t.Helper()
	_, store, environment, project, _ := routeRepositoryTestHierarchy(t)
	return store, environment, project
}

func testConnectorRecord(
	t *testing.T,
	environmentID string,
	now time.Time,
	seed int64,
	name string,
) ConnectorRecord {
	t.Helper()
	record, err := NewConnectorRecord(core.Connector{
		ID:            ids.NewAt(ids.KindConnector, now, seed),
		EnvironmentID: environmentID,
		Name:          name,
		Kind:          core.ConnectorKindS3Compatible,
		Endpoint:      "https://objects.example.test",
		Bucket:        "groundplane-backups",
		Prefix:        "production/",
		Region:        "auto",
		PathStyle:     true,
		Credentials: map[core.ConnectorCredentialName]core.ConnectorCredential{
			core.ConnectorCredentialAccessKey: {Kind: core.ConnectorCredentialDirect},
			core.ConnectorCredentialSecretKey: {Kind: core.ConnectorCredentialDirect},
		},
	})
	if err != nil {
		t.Fatalf("NewConnectorRecord() error = %v", err)
	}
	return record
}
