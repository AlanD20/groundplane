package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"testing"
	"time"
)

// CON-09: rename, credential replacement and replay must be one atomic mutation;
// a replay must not rewrite ciphertext or move the Connector a second time.
func TestConnectorEditAtomicRenameAndReplay(t *testing.T) {
	store, environment, project := testConnectorHierarchy(t)
	repository, err := newConnectorRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record := testConnectorRecord(t, environment.Record.ID, time.Now().UTC(), 4000, "old-name")
	credentials, err := testconnectors.NewEncryptedCredentials(record.Connector.ID, []byte("old-ciphertext"))
	if err != nil {
		t.Fatal(err)
	}
	current, err := repository.CreateConnector(context.Background(), environment, project, record, credentials)
	if err != nil {
		t.Fatal(err)
	}
	record.Connector.Name = "new-name"
	replacement, err := testconnectors.NewEncryptedCredentials(record.Connector.ID, []byte("new-ciphertext"))
	if err != nil {
		t.Fatal(err)
	}
	marker := connectorCreateMarker(environment.Record.ID, "connector-edit-key-0001")
	marker.Locator.Method, marker.Locator.Route, marker.Response.Status = "PATCH", "/api/v1/connectors/{id}", 200
	result, err := repository.EditConnectorIdempotent(
		context.Background(),
		environment,
		project,
		current,
		credentials.CiphertextSHA256,
		record,
		replacement,
		marker,
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("edit: %v %v %v", outcome, conflict, err)
	}
	stored, err := repository.GetConnector(context.Background(), record.Connector.ID)
	if err != nil || stored.Record.Connector.Name != "new-name" {
		t.Fatalf("metadata: %v %v", stored, err)
	}
	encrypted, err := repository.GetConnectorCredentials(context.Background(), stored)
	if err != nil || string(encrypted.Ciphertext) != "new-ciphertext" {
		t.Fatalf("credentials not replaced: %v", err)
	}
	old, err := store.Get(context.Background(), testconnectors.ConnectorNameKey(environment.Record.ID, "old-name"))
	if err != nil || (old != nil && old.Entry != nil) {
		t.Fatalf("old name still indexed: %v", err)
	}
	newName, err := store.Get(context.Background(), testconnectors.ConnectorNameKey(environment.Record.ID, "new-name"))
	if err != nil || newName == nil || newName.Entry == nil || string(newName.Entry.Value) != record.Connector.ID ||
		newName.Entry.ModRevision != stored.Revision {
		t.Fatal("new name was not committed with metadata")
	}
	result, err = repository.EditConnectorIdempotent(
		context.Background(),
		environment,
		project,
		current,
		credentials.CiphertextSHA256,
		record,
		replacement,
		marker,
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
		outcome != IdempotencyKnownExisting {
		t.Fatalf("replay: %v %v %v", outcome, conflict, err)
	}
	after, err := repository.GetConnector(context.Background(), record.Connector.ID)
	if err != nil || after.Revision != stored.Revision {
		t.Fatal("replay rewrote connector")
	}
}

// CON-09: retained objects keep their locator, including a new reference racing
// the edit transaction. A credential rotation at that locator remains permitted.
func TestConnectorEditRetainedDestinationAndRaces(t *testing.T) {
	for _, scenario := range []string{"point", "orphan", "point-race", "metadata-race", "credential-race", "name-race", "active-backup", "backup-race"} {
		t.Run(scenario, func(t *testing.T) {
			store, environment, project := testConnectorHierarchy(t)
			racing := &connectorSecretRaceStore{hierarchyStore: store}
			repository, err := newConnectorRepository(racing)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			record := testConnectorRecord(t, environment.Record.ID, now, 4010, "backup-store")
			credentials, err := testconnectors.NewEncryptedCredentials(record.Connector.ID, []byte("old-ciphertext"))
			if err != nil {
				t.Fatal(err)
			}
			current, err := repository.CreateConnector(context.Background(), environment, project, record, credentials)
			if err != nil {
				t.Fatal(err)
			}
			pointID := ids.NewAt(ids.KindRecoveryPoint, now, 4011)
			key, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(record.Connector.ID, pointID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "orphan" {
				key, err = testbackupruntime.BackupOrphanConnectorIndexKey(record.Connector.ID, pointID)
				if err != nil {
					t.Fatal(err)
				}
			}
			mutation := testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: []byte(pointID)}
			switch scenario {
			case "metadata-race":
				mutation.Key = testconnectors.RecordKey(record.Connector.ID)
				mutation.Value, err = testconnectors.EncodeRecord(record)
			case "credential-race":
				mutation.Key = testconnectors.CredentialValueKey(record.Connector.ID)
				changed, encodeErr := testconnectors.NewEncryptedCredentials(
					record.Connector.ID,
					[]byte("raced-ciphertext"),
				)
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				mutation.Value, err = testconnectors.EncodeEncryptedCredentials(changed)
			case "name-race":
				record.Connector.Name = "another-name"
				mutation.Key = testconnectors.ConnectorNameKey(environment.Record.ID, record.Connector.Name)
				mutation.Value = []byte(ids.NewAt(ids.KindConnector, now, 4012))
			}
			if err != nil {
				t.Fatal(err)
			}
			mutate := func() {
				result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{mutation})
				if err != nil || !result.Succeeded {
					t.Fatalf("race injection: %v", err)
				}
			}
			if scenario == "active-backup" || scenario == "backup-race" {
				mutate = func() {
					putEnvironmentMutationFenceTestLock(
						t,
						store,
						environment.Record.ID,
						environmentMutationFenceTestOwner(now, 4013),
					)
				}
			}
			if scenario == "point" || scenario == "orphan" || scenario == "active-backup" {
				mutate()
			} else {
				racing.beforeTransact = mutate
			}
			if scenario != "name-race" {
				record.Connector.Bucket = "different-bucket"
			}
			marker := connectorCreateMarker(environment.Record.ID, "connector-edit-key-0002")
			marker.Locator.Method, marker.Locator.Route, marker.Response.Status = "PATCH", "/api/v1/connectors/{id}", 200
			result, err := repository.EditConnectorIdempotent(
				context.Background(),
				environment,
				project,
				current,
				credentials.CiphertextSHA256,
				record,
				credentials,
				marker,
			)
			if err == nil {
				_, _, err, _ = result.Classify()
			}
			if kind, ok := errs.KindOf(err); !ok || (kind != errs.KindResourceInUse && kind != errs.KindStateConflict) {
				t.Fatalf("unsafe edit accepted: %v", err)
			}
			stored, err := repository.GetConnector(context.Background(), record.Connector.ID)
			if err != nil || stored.Record.Connector.Bucket != "groundplane-backups" ||
				stored.Record.Connector.Name != "backup-store" {
				t.Fatal("rejected edit changed metadata")
			}
			if scenario == "point" || scenario == "orphan" {
				record.Connector.Bucket = "groundplane-backups"
				record.Connector.Name = "renamed-store"
				result, err = repository.EditConnectorIdempotent(
					context.Background(),
					environment,
					project,
					current,
					credentials.CiphertextSHA256,
					record,
					credentials,
					marker,
				)
				if err != nil {
					t.Fatalf("safe rename rejected: %v", err)
				}
				if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
					outcome != IdempotencyKnownApplied {
					t.Fatalf("safe rename failed: %v %v", conflict, err)
				}
			}
		})
	}
}
