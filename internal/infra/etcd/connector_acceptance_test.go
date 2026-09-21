package etcd

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: acceptance requires immutable Environment ownership, name
// uniqueness, fixed-revision pagination, and complete reconstruction from
// durable records rather than process-local repository state.
func TestC15ConnectorRestartPaginationOwnershipAndNameUniqueness(t *testing.T) {
	ctx := context.Background()
	store, environment, project := testConnectorHierarchy(t)
	repository, err := newConnectorRepository(store)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC)
	create := func(seed int64, name string) testkeyvalue.Versioned[testconnectors.Record] {
		t.Helper()
		record := testConnectorRecord(t, environment.Record.ID, now, seed, name)
		credentials, createErr := testconnectors.NewEncryptedCredentials(record.Connector.ID, []byte("age-ciphertext"))
		if createErr != nil {
			t.Fatalf("NewConnectorEncryptedCredentials() error = %v", createErr)
		}
		defer clear(credentials.Ciphertext)
		created, createErr := repository.CreateConnector(ctx, environment, project, record, credentials)
		if createErr != nil {
			t.Fatalf("CreateConnector(%s) error = %v", name, createErr)
		}
		return created
	}

	first := create(1, "first")
	second := create(2, "second")
	page, err := repository.ListConnectors(ctx, environment.Record.ID, testkeyvalue.PageRequest{Limit: 1})
	if err != nil || len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatalf("ListConnectors(first page) = %#v, %v", page, err)
	}
	third := create(3, "third")
	restarted, err := newConnectorRepository(store)
	if err != nil {
		t.Fatalf("newConnectorRepository(restart) error = %v", err)
	}
	continued, err := restarted.ListConnectors(
		ctx, environment.Record.ID, testkeyvalue.PageRequest{Limit: 1, Cursor: page.NextCursor},
	)
	if err != nil || continued.Revision != page.Revision || len(continued.Items) != 1 ||
		continued.Items[0].Record.Connector.ID != second.Record.Connector.ID ||
		continued.Items[0].Record.Connector.ID == third.Record.Connector.ID {
		t.Fatalf("ListConnectors(fixed continuation) = %#v, %v", continued, err)
	}
	shown, err := restarted.GetConnector(ctx, first.Record.Connector.ID)
	if err != nil || shown.Record.Connector.ID != first.Record.Connector.ID ||
		shown.Record.Connector.Name != "first" || shown.Record.Connector.EnvironmentID != environment.Record.ID {
		t.Fatalf("GetConnector(after restart) = %#v, %v", shown, err)
	}
	otherEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 20)
	isolated, err := restarted.ListConnectors(ctx, otherEnvironmentID, testkeyvalue.PageRequest{Limit: 10})
	if err != nil || len(isolated.Items) != 0 {
		t.Fatalf("ListConnectors(other Environment) = %#v, %v", isolated, err)
	}

	duplicate := testConnectorRecord(t, environment.Record.ID, now, 21, "first")
	duplicateCredentials, err := testconnectors.NewEncryptedCredentials(
		duplicate.Connector.ID,
		[]byte("other-ciphertext"),
	)
	if err != nil {
		t.Fatalf("NewConnectorEncryptedCredentials(duplicate) error = %v", err)
	}
	defer clear(duplicateCredentials.Ciphertext)
	_, err = restarted.CreateConnector(ctx, environment, project, duplicate, duplicateCredentials)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("CreateConnector(duplicate immutable name) error = %v", err)
	}
}

// Rationale: a configured but disabled policy retains its Connector choice
// without owning the active reverse-reference fence; enabling that same policy
// must make removal refuse before a finalizer Task is published.
func TestC15ConnectorRemovalDistinguishesDisabledAndEnabledPolicyReferences(t *testing.T) {
	for _, test := range []struct {
		name        string
		enabled     bool
		wantBlocked bool
	}{
		{name: "disabled configured policy", enabled: false, wantBlocked: false},
		{name: "enabled policy", enabled: true, wantBlocked: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newConnectorDeletionFixture(t)
			policy := testbackuppolicy.BackupPolicyRecord{
				EnvironmentID: fixture.environment.Record.ID,
				Enabled:       test.enabled,
				Frequency:     "*-*-* 03:00:00",
				Keep:          7,
				Encryption:    "age",
				ConnectorID:   fixture.connector.Record.Connector.ID,
				SourceIDs:     []string{fixture.source.Record.ID},
				UpdatedAt:     fixture.now,
			}
			value, err := testbackuppolicy.EncodeBackupPolicyRecord(policy)
			if err != nil {
				t.Fatalf("encodeBackupPolicyRecord() error = %v", err)
			}
			defer clear(value)
			connectorDeletionPutKey(
				t,
				fixture.store,
				testbackuppolicy.BackupPolicyKey(fixture.environment.Record.ID),
				value,
			)
			if test.enabled {
				connectorDeletionPutKey(
					t,
					fixture.store, testbackuppolicy.BackupPolicyConnectorReferenceKey(
						fixture.connector.Record.Connector.ID,
						fixture.environment.Record.ID,
					), []byte(fixture.environment.Record.ID),
				)
			}
			repository, err := newConnectorRepository(fixture.store)
			if err != nil {
				t.Fatalf("newConnectorRepository() error = %v", err)
			}
			task, marker, tombstone, intent := connectorDeletionTestTask(
				t,
				fixture.connector,
				fixture.project,
				fixture.environment,
				fixture.now.Add(time.Minute),
				3000,
			)
			marker.Locator.Method = http.MethodDelete
			result, err := repository.BeginConnectorDeletionWithTask(
				context.Background(), fixture.environment, fixture.project, fixture.connector,
				tombstone, intent, task, marker,
			)
			if test.wantBlocked {
				if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
					t.Fatalf("BeginConnectorDeletionWithTask(enabled policy) error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("BeginConnectorDeletionWithTask(disabled policy) error = %v", err)
			}
			outcome, _, conflict, classifyErr := result.Classify()
			if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
				t.Fatalf("disabled policy deletion result = %v/%v/%v", outcome, conflict, classifyErr)
			}
		})
	}
}

// Rationale: CRUD must reject every authority value that execution would
// reject, before storing credentials or attempting endpoint connectivity.
func TestC15ConnectorDurableValidationMatchesExecutionAuthority(t *testing.T) {
	now := time.Date(2026, 8, 26, 9, 0, 0, 0, time.UTC)
	base := testConnectorRecord(t, ids.NewAt(ids.KindEnvironment, now, 1), now, 2, "strict")
	for name, mutate := range map[string]func(*testconnectors.Record){
		"endpoint bound": func(record *testconnectors.Record) {
			record.Connector.Endpoint = "https://" + strings.Repeat("a", 2041)
		},
		"numeric bucket":  func(record *testconnectors.Record) { record.Connector.Bucket = "999.999.999.999" },
		"reserved bucket": func(record *testconnectors.Record) { record.Connector.Bucket = "xn--groundplane" },
		"prefix bound": func(record *testconnectors.Record) {
			record.Connector.Prefix = strings.Repeat("a", 1024) + "/"
		},
		"region bound":      func(record *testconnectors.Record) { record.Connector.Region = strings.Repeat("r", 65) },
		"region whitespace": func(record *testconnectors.Record) { record.Connector.Region = "eu west 1" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			candidate.Connector.Credentials = maps.Clone(base.Connector.Credentials)
			mutate(&candidate)
			if err := testconnectors.ValidateRecord(candidate); !errors.Is(
				err,
				errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("validateConnectorRecord() error = %v", err)
			}
		})
	}
}
