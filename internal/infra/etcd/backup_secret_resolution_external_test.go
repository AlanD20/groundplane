package etcd_test

import (
	"context"
	"errors"
	base "github.com/AlanD20/groundplane/internal/infra/etcd"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackupsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/backupsecrets"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: a Connector mutation after run publication must not replace the
// exact metadata revision pinned by every capture step.
func TestBackupSecretResolutionRejectsChangedPinnedConnector(t *testing.T) {
	fixture := newBackupSecretFixture(t, false)
	connector := backupSecretConnectorValue(t, fixture)
	value := append([]byte(nil), connector.Value...)
	result, err := fixture.Store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testconnectors.RecordKey(fixture.Run.ConnectorID), Value: value,
	}})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("mutate Connector = %#v, %v", result, err)
	}
	if _, err := fixture.reader.ResolveBackupSecretEvidence(
		context.Background(), fixture.Request,
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
			fixture := newBackupSecretFixture(t, false)
			request := fixture.Request
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
	fixture := newBackupSecretFixture(t, false)
	mutated := proto.Clone(fixture.Request.Plan).(*agentpb.ExecutionPlan)
	mutated.PlanHash = nil
	mutated.Steps[0].GetBackupSourceCapture().ConnectorRevision++
	sealed, err := executionplan.Seal(mutated)
	if err != nil {
		t.Fatal(err)
	}
	request := fixture.Request
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
	fixture := newBackupSecretFixture(t, true)
	evidence, err := fixture.reader.ResolveBackupSecretEvidence(context.Background(), fixture.Request)
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

type backupSecretFixture struct {
	base.BackupSecretResolutionFixture
	reader *testbackupsecrets.Reader
}

func newBackupSecretFixture(t *testing.T, mixed bool) backupSecretFixture {
	t.Helper()
	fixture := base.NewBackupSecretResolutionFixture(t, mixed)
	reader, err := testbackupsecrets.NewReader(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	return backupSecretFixture{BackupSecretResolutionFixture: fixture, reader: reader}
}
func backupSecretConnectorValue(t *testing.T, fixture backupSecretFixture) *testkeyvalue.KeyValue {
	t.Helper()
	read, err := fixture.Store.GetMany(
		context.Background(),
		testkeyvalue.GetManyRequest{Keys: []string{testconnectors.RecordKey(fixture.Run.ConnectorID)}},
	)
	if err != nil || read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		t.Fatalf("read fixture connector: %v, %v", read, err)
	}
	t.Cleanup(func() { testkeyvalue.ClearValues(read.Values) })
	return read.Values[0]
}

// Rationale: direct Connector ciphertext is subordinate immutable evidence;
// replacing only that envelope after publication must invalidate delivery.
func TestBackupSecretResolutionRejectsDirectEnvelopeRevisionMutation(t *testing.T) {
	fixture := newBackupSecretFixture(t, false)
	credentials, err := testconnectors.NewEncryptedCredentials(
		fixture.Run.ConnectorID, []byte("mutated-direct-credentials"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(credentials.Ciphertext)
	value, err := testconnectors.EncodeEncryptedCredentials(credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	mutated, err := fixture.Store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testconnectors.CredentialValueKey(fixture.Run.ConnectorID), Value: value,
	}})
	if err != nil || !mutated.Succeeded {
		t.Fatalf("mutate direct credential envelope = %#v, %v", mutated, err)
	}
	evidence, err := fixture.reader.ResolveBackupSecretEvidence(context.Background(), fixture.Request)
	evidence.Clear()
	if err == nil {
		t.Fatal("direct credential envelope revision mutation was accepted")
	}
}

// Rationale: ADR0045 defines project-first lookup with platform fallback, so a
// finalizing project candidate is hidden and the valid platform value wins.
func TestBackupSecretResolutionSkipsDeletingProjectSecretForPlatformFallback(t *testing.T) {
	fixture := base.NewBackupSecretDeletingProjectFallbackFixture(t)
	reader, err := testbackupsecrets.NewReader(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := reader.ResolveBackupSecretEvidence(context.Background(), fixture.Request)
	defer evidence.Clear()
	if err != nil {
		t.Fatalf("deleting project Secret blocked valid platform fallback: %v", err)
	}
	for _, value := range evidence.SecretValues {
		if value.Name != backupsecret.CredentialAccessKey {
			continue
		}
		if value.Value.SecretID != fixture.PlatformAccessSecretID {
			t.Fatalf(
				"access Secret = %q, want platform %q",
				value.Value.SecretID,
				fixture.PlatformAccessSecretID,
			)
		}
		return
	}
	t.Fatal("platform access Secret was not resolved")
}

// Rationale: prune resolution must revalidate the historical pending authority,
// current assignment, dispatch, plan, object evidence, and mixed credentials.
func TestBackupSecretResolutionForPublishedPruneAssignment(t *testing.T) {
	fixture := base.NewBackupSecretPublishedPruneFixture(t)
	reader, err := testbackupsecrets.NewReader(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := reader.ResolveBackupSecretEvidence(context.Background(), fixture.Request)
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
