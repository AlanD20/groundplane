package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func testConnectorHierarchy(
	t *testing.T,
) (*memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], testkeyvalue.Versioned[testhierarchy.ProjectRecord]) {
	t.Helper()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	return store, environment, project
}

func testConnectorRecord(
	t *testing.T,
	environmentID string,
	now time.Time,
	seed int64,
	name string,
) testconnectors.Record {
	t.Helper()
	record, err := testconnectors.NewRecord(core.Connector{
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
