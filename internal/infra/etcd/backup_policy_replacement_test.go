package etcd

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	testbackupqueries "github.com/AlanD20/groundplane/internal/infra/etcd/backupqueries"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testbackupscheduling "github.com/AlanD20/groundplane/internal/infra/etcd/backupscheduling"
	testbackupsources "github.com/AlanD20/groundplane/internal/infra/etcd/backupsources"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testenvironmentcoordination "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testenvironmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: public preparation and protected publication must commit ordered
// sources, Connector reference, era-1 key pair, and exact replay evidence.
func TestBackupPolicyProtectedReplacementCommitsAgeStateAndReplays(t *testing.T) {
	t.Parallel()
	fixture := newBackupPolicyReplacementFixture(t, true)
	prepared := fixture.prepared(t, fixture.policyInput(true, "age", fixture.connector.Record.Connector.ID))
	defer prepared.Destroy()
	projection := prepared.Projection()
	marker := backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-age-0001")
	result, err := fixture.repository.ReplaceBackupPolicyProtected(context.Background(), prepared, marker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("ReplaceBackupPolicyProtected() = %#v, %v", result, err)
	}
	replayed, err := fixture.repository.ReplaceBackupPolicyProtected(context.Background(), prepared, marker)
	if err != nil || replayed.kind != idempotencyTransactionExisting ||
		string(replayed.marker.Response.Body) != string(marker.Response.Body) {
		t.Fatalf("ReplaceBackupPolicyProtected(replay) = %#v, %v", replayed, err)
	}
	stored, found, err := fixture.repository.GetBackupPolicy(context.Background(), fixture.environment.Record.ID)
	if err != nil || !found || len(stored.Record.SourceIDs) != len(projection.Sources) {
		t.Fatalf("GetBackupPolicy() = %#v, %t, %v", stored, found, err)
	}
	for index := range projection.Sources {
		if stored.Record.SourceIDs[index] != projection.Sources[index].ID {
			t.Fatalf("stored source order = %#v", stored.Record.SourceIDs)
		}
	}
	key := mustBackupKey(t, fixture.store, fixture.environment.Record.ID)
	defer clear(key.Encrypted.Ciphertext)
	if key.Record.KeyEra != 1 || key.Record.Recipient != projection.AgeRecipient ||
		string(key.Encrypted.Ciphertext) != "controller-sealed-age-identity" {
		t.Fatalf("GetBackupKey() = %#v", key)
	}
	assertBackupPolicyReference(t, fixture.store, fixture.connector.Record.Connector.ID,
		fixture.environment.Record.ID, true)
}

// Rationale: encryption none never creates private key state, while Connector
// moves and disablement retain the existing age key without revision churn.
func TestBackupPolicyProtectedReplacementAvoidsAndRetainsKeys(t *testing.T) {
	t.Parallel()
	t.Run("none never creates", func(t *testing.T) {
		fixture := newBackupPolicyReplacementFixture(t, true)
		prepared := fixture.prepared(t, fixture.policyInput(true, "none", fixture.connector.Record.Connector.ID))
		defer prepared.Destroy()
		if _, err := fixture.repository.ReplaceBackupPolicyProtected(context.Background(), prepared,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-none-0001")); err != nil {
			t.Fatal(err)
		}
		if _, found, err := backupPolicyKeyReader(t, fixture.store).GetBackupKey(
			context.Background(), fixture.environment.Record.ID,
		); err != nil || found {
			t.Fatalf("GetBackupKey(none) found/error = %t/%v", found, err)
		}
	})

	t.Run("move and disable retain", func(t *testing.T) {
		fixture := newBackupPolicyReplacementFixture(t, true)
		first := fixture.prepared(t, fixture.policyInput(true, "age", fixture.connector.Record.Connector.ID))
		if _, err := fixture.repository.ReplaceBackupPolicyProtected(context.Background(), first,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-move-0001")); err != nil {
			t.Fatal(err)
		}
		first.Destroy()
		existing := mustBackupKey(t, fixture.store, fixture.environment.Record.ID)
		defer clear(existing.Encrypted.Ciphertext)
		second := fixture.createConnector(t, 2450, "archive-backups")
		move := fixture.prepared(t, fixture.policyInput(true, "none", second.Record.Connector.ID))
		if _, err := fixture.repository.ReplaceBackupPolicyProtected(context.Background(), move,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-move-0002")); err != nil {
			t.Fatal(err)
		}
		move.Destroy()
		assertBackupPolicyReference(t, fixture.store, fixture.connector.Record.Connector.ID,
			fixture.environment.Record.ID, false)
		assertBackupPolicyReference(t, fixture.store, second.Record.Connector.ID,
			fixture.environment.Record.ID, true)
		retained := mustBackupKey(t, fixture.store, fixture.environment.Record.ID)
		defer clear(retained.Encrypted.Ciphertext)
		if retained.RecordRevision != existing.RecordRevision ||
			retained.EncryptedRevision != existing.EncryptedRevision {
			t.Fatalf("move changed retained key revisions: %#v -> %#v", existing, retained)
		}
		disable := fixture.policyInput(false, "none", second.Record.Connector.ID)
		disabled := fixture.prepared(t, disable)
		if _, err := fixture.repository.ReplaceBackupPolicyProtected(context.Background(), disabled,
			backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-disable-0001")); err != nil {
			t.Fatal(err)
		}
		disabled.Destroy()
		assertBackupPolicyReference(t, fixture.store, second.Record.Connector.ID,
			fixture.environment.Record.ID, false)
		after := mustBackupKey(t, fixture.store, fixture.environment.Record.ID)
		defer clear(after.Encrypted.Ciphertext)
		if after.RecordRevision != retained.RecordRevision || after.EncryptedRevision != retained.EncryptedRevision {
			t.Fatalf("disable changed retained key revisions: %#v -> %#v", retained, after)
		}
	})
}

// Rationale: marker identity and every deletion fence are consumed by the
// same protected CAS as the policy publication.
func TestBackupPolicyProtectedReplacementRejectsMarkerAndDeletionFences(t *testing.T) {
	t.Parallel()
	t.Run("marker scope", func(t *testing.T) {
		fixture := newBackupPolicyReplacementFixture(t, true)
		prepared := fixture.prepared(t, fixture.policyInput(true, "none", fixture.connector.Record.Connector.ID))
		defer prepared.Destroy()
		marker := backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-scope-0001")
		marker.Locator.ScopeID = ids.NewAt(ids.KindEnvironment, marker.CreatedAt, 2500)
		if _, err := fixture.repository.ReplaceBackupPolicyProtected(
			context.Background(), prepared, marker,
		); !isKind(err, errs.KindValidationFailed) {
			t.Fatalf("ReplaceBackupPolicyProtected(scope) error = %v", err)
		}
	})

	for _, test := range []struct {
		name string
		kind testdeletions.DeletionTargetKind
		id   func(*backupPolicyReplacementFixture) string
	}{
		{name: "environment", kind: testdeletions.DeletionTargetEnvironment,
			id: func(f *backupPolicyReplacementFixture) string { return f.environment.Record.ID }},
		{name: "project", kind: testdeletions.DeletionTargetProject,
			id: func(f *backupPolicyReplacementFixture) string { return f.project.Record.ID }},
		{name: "tenant", kind: testdeletions.DeletionTargetTenant,
			id: func(f *backupPolicyReplacementFixture) string { return f.project.Record.TenantID }},
		{name: "connector", kind: testdeletions.DeletionTargetConnector,
			id: func(f *backupPolicyReplacementFixture) string { return f.connector.Record.Connector.ID }},
	} {
		t.Run(test.name+" fence", func(t *testing.T) {
			fixture := newBackupPolicyReplacementFixture(t, true)
			prepared := fixture.prepared(t, fixture.policyInput(true, "none", fixture.connector.Record.Connector.ID))
			defer prepared.Destroy()
			id := test.id(fixture)
			transaction, err := fixture.store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
				Type: testkeyvalue.MutationPut,
				Key:  testdeletions.TombstoneKey(string(test.kind), id), Value: []byte("fenced"),
			}})
			if err != nil || !transaction.Succeeded {
				t.Fatalf("seed deletion fence = %#v, %v", transaction, err)
			}
			result, err := fixture.repository.ReplaceBackupPolicyProtected(
				context.Background(), prepared,
				backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-fence-0001"),
			)
			if err != nil || result.kind != idempotencyTransactionConflict ||
				!isKind(result.conflict, errs.KindResourceInUse) {
				t.Fatalf("ReplaceBackupPolicyProtected(fenced) = %#v, %v", result, err)
			}
		})
	}
}

// Rationale: ambiguous commit delivery must preserve the storage error while
// exact retry resolves through the committed marker and another key conflicts.
func TestBackupPolicyProtectedReplacementPreservesUnknownOutcomeAndConflicts(t *testing.T) {
	t.Parallel()
	fixture := newBackupPolicyReplacementFixture(t, true)
	prepared := fixture.prepared(t, fixture.policyInput(true, "none", fixture.connector.Record.Connector.ID))
	defer prepared.Destroy()
	unknown := errs.New(errs.KindStorageUnavailable, "unknown backup policy write outcome")
	store := &backupPolicyReplacementUnknownStore{memoryHierarchyStore: fixture.store, failNext: unknown}
	repository, err := newBackupPolicyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	marker := backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-unknown-0001")
	if _, err := repository.ReplaceBackupPolicyProtected(
		context.Background(), prepared, marker,
	); !errors.Is(err, unknown) {
		t.Fatalf("ReplaceBackupPolicyProtected(unknown) error = %v", err)
	}
	replayed, err := repository.ReplaceBackupPolicyProtected(context.Background(), prepared, marker)
	if err != nil || replayed.kind != idempotencyTransactionExisting {
		t.Fatalf("ReplaceBackupPolicyProtected(after unknown) = %#v, %v", replayed, err)
	}
	conflict, err := repository.ReplaceBackupPolicyProtected(
		context.Background(), prepared,
		backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-conflict-0001"),
	)
	if err != nil || conflict.kind != idempotencyTransactionConflict ||
		!isKind(conflict.conflict, errs.KindStateConflict) {
		t.Fatalf("ReplaceBackupPolicyProtected(stale) = %#v, %v", conflict, err)
	}
}

// Rationale: operation-lock and coordination changes after public preparation
// must lose without adding any replacement write.
func TestBackupPolicyProtectedReplacementRejectsChangedCoordinationWithoutWrites(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		mutation func(*testing.T, *backupPolicyReplacementFixture) testkeyvalue.Mutation
		wantKind errs.Kind
	}{
		{
			name: "operation lock", wantKind: errs.KindResourceInUse,
			mutation: func(_ *testing.T, fixture *backupPolicyReplacementFixture) testkeyvalue.Mutation {
				return testkeyvalue.Mutation{
					Type:  testkeyvalue.MutationPut,
					Key:   testhierarchy.EnvironmentOperationLockKey(fixture.environment.Record.ID),
					Value: []byte("held"),
				}
			},
		},
		{
			name: "coordination revision", wantKind: errs.KindStateConflict,
			mutation: func(t *testing.T, fixture *backupPolicyReplacementFixture) testkeyvalue.Mutation {
				coordination := mustBackupPolicyCoordination(t, fixture.store, fixture.environment.Record.ID)
				value, err := testenvironmentcoordination.Encode(coordination.Record)
				if err != nil {
					t.Fatal(err)
				}
				return testkeyvalue.Mutation{
					Type:  testkeyvalue.MutationPut,
					Key:   testenvironmentcoordination.Key(fixture.environment.Record.ID),
					Value: value,
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBackupPolicyReplacementFixture(t, true)
			if test.name == "coordination revision" {
				value, err := testenvironmentcoordination.Encode(
					testenvironmentcoordination.EnvironmentCoordinationRecord{
						EnvironmentID: fixture.environment.Record.ID, ScheduleClockFloor: fixture.now,
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.store.Transact(t.Context(), nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: testenvironmentcoordination.Key(fixture.environment.Record.ID), Value: value}}); err != nil {
					t.Fatal(err)
				}
			}
			prepared := fixture.prepared(t, fixture.policyInput(true, "none", fixture.connector.Record.Connector.ID))
			defer prepared.Destroy()
			transaction, err := fixture.store.Transact(
				context.Background(), nil, []testkeyvalue.Mutation{test.mutation(t, fixture)},
			)
			if err != nil || !transaction.Succeeded {
				t.Fatalf("advance fence = %#v, %v", transaction, err)
			}
			revisionBefore := fixture.store.revision
			result, err := fixture.repository.ReplaceBackupPolicyProtected(
				context.Background(), prepared,
				backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-policy-stale-fence-0001"),
			)
			if err != nil || result.kind != idempotencyTransactionConflict ||
				!isKind(result.conflict, test.wantKind) {
				t.Fatalf("ReplaceBackupPolicyProtected(stale fence) = %#v, %v", result, err)
			}
			if fixture.store.revision != revisionBefore {
				t.Fatalf("stale replacement revision = %d, want %d", fixture.store.revision, revisionBefore)
			}
		})
	}
}

type backupPolicyReplacementFixture struct {
	repository    *BackupPolicyRepository
	store         *memoryHierarchyStore
	environment   testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	project       testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	mutationEpoch testkeyvalue.Versioned[testbackupruntime.EnvironmentMutationEpochRecord]
	sources       []testbackuppolicymutations.SourceEvidence
	connector     testkeyvalue.Versioned[testconnectors.Record]
	now           time.Time
}

func newBackupPolicyReplacementFixture(t *testing.T, includeVolume bool) *backupPolicyReplacementFixture {
	t.Helper()
	now := time.Date(2026, 8, 23, 16, 0, 0, 0, time.UTC)
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenantID := ids.NewAt(ids.KindTenant, now, 2300)
	if _, err := hierarchy.CreateTenant(context.Background(), testhierarchy.TenantRecord{
		ID: tenantID, Slug: "backup-tenant", Name: "Backup Tenant",
	}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := hierarchy.CreateProject(context.Background(), testhierarchy.ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 2301), TenantID: tenantID,
		Slug: "backup-project", Name: "Backup Project", Kind: testhierarchy.ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, now, 2302)
	environment, err := hierarchy.CreateEnvironment(context.Background(), testhierarchy.EnvironmentRecord{
		ID: environmentID, ProjectID: project.Record.ID,
		Name: "production", NetworkPool: "10.240.0.0/24",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + project.Record.ID + "/" + environmentID,
		ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, now, 2303), CreatedAt: now,
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
		context.Background(), store, repository.now,
		environment, project, core.BackupSourceConfig, environment.Record.ID,
	)
	if err != nil {
		t.Fatalf("EnsureBackupSource(config) error = %v", err)
	}
	sources := []testbackuppolicymutations.SourceEvidence{backupPolicyReplacementSourceEvidence(
		t, store, configSource, nil, nil,
	)}
	if includeVolume {
		volume := testenvironmentprojection.EnvironmentVolumeIdentity{
			ID: ids.NewAt(ids.KindVolume, now, 2400), Slug: "backup-data", Key: "backup-data",
		}
		createdVolume := seedBackupPolicyVolumeProjection(
			t, store, environment, project, volume, 2401,
		)
		volumeSource, err := testbackupsources.EnsureBackupSource(
			context.Background(), store, repository.now,
			environment, project, core.BackupSourceVolume, volume.ID,
		)
		if err != nil {
			t.Fatalf("EnsureBackupSource(volume) error = %v", err)
		}
		sources = []testbackuppolicymutations.SourceEvidence{
			backupPolicyReplacementSourceEvidence(t, store, volumeSource, nil, &createdVolume),
			backupPolicyReplacementSourceEvidence(t, store, configSource, nil, nil),
		}
	}
	fixture := &backupPolicyReplacementFixture{
		repository: repository, store: store, environment: environment, project: project,
		sources: sources, now: now,
	}
	fixture.connector = fixture.createConnector(t, 2410, "primary-backups")
	fixture.mutationEpoch = mustBackupPolicyMutationEpoch(t, store, environment.Record.ID)
	return fixture
}

func (fixture *backupPolicyReplacementFixture) createConnector(
	t *testing.T,
	seed int64,
	name string,
) testkeyvalue.Versioned[testconnectors.Record] {
	t.Helper()
	repository, err := newConnectorRepository(fixture.store)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	record := testConnectorRecord(t, fixture.environment.Record.ID, fixture.now, seed, name)
	credentials, err := testconnectors.NewEncryptedCredentials(
		record.Connector.ID,
		[]byte("sealed-connector-credentials"),
	)
	if err != nil {
		t.Fatalf("NewConnectorEncryptedCredentials() error = %v", err)
	}
	created, err := repository.CreateConnector(
		context.Background(), fixture.environment, fixture.project, record, credentials,
	)
	if err != nil {
		t.Fatalf("CreateConnector() error = %v", err)
	}
	return created
}

func (fixture *backupPolicyReplacementFixture) policyInput(
	enabled bool,
	encryption string,
	connectorID string,
) testbackuppolicy.BackupPolicyReplacementInput {
	selectedSources := fixture.sources
	if encryption == "none" {
		selectedSources = nil
		for _, source := range fixture.sources {
			if source.Source.Record.Kind != core.BackupSourceConfig {
				selectedSources = append(selectedSources, source)
			}
		}
	}
	input := testbackuppolicy.BackupPolicyReplacementInput{
		EnvironmentID: fixture.environment.Record.ID,
		Enabled:       enabled,
		Frequency:     "*-*-* 03:00:00",
		Keep:          7,
		Encryption:    encryption,
		ConnectorID:   connectorID,
		Sources:       make([]testbackuppolicy.BackupPolicySourceSelection, len(selectedSources)),
	}
	for index, source := range selectedSources {
		input.Sources[index] = testbackuppolicy.BackupPolicySourceSelection{
			Kind: source.Source.Record.Kind, TargetID: source.Source.Record.TargetID,
		}
	}
	return input
}

func (fixture *backupPolicyReplacementFixture) prepared(
	t *testing.T,
	input testbackuppolicy.BackupPolicyReplacementInput,
) testbackuppolicymutations.PreparedBackupPolicyReplacement {
	t.Helper()
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(context.Background(), input)
	if err != nil {
		t.Fatalf("PrepareBackupPolicyReplacement() error = %v", err)
	}
	if prepared.RequiresInitialKey() {
		identity, err := age.GenerateX25519Identity()
		if err != nil {
			t.Fatalf("age.GenerateX25519Identity() error = %v", err)
		}
		prepared, err = fixture.repository.SupplyBackupPolicyInitialKey(
			context.Background(), prepared, testbackuppolicymutations.BackupPolicyInitialKeyMaterial{
				Recipient: identity.Recipient().String(), Ciphertext: []byte("controller-sealed-age-identity"),
			},
		)
		if err != nil {
			t.Fatalf("SupplyBackupPolicyInitialKey() error = %v", err)
		}
	}
	return prepared
}

func backupPolicyReplacementMarker(environmentID string, key string) testidempotency.IdempotencyMarker {
	marker := testDirectMarker()
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment,
		ScopeID:   environmentID,
		Method:    http.MethodPut,
		Route:     "/environments/{id}/backup-policy",
		Key:       key,
	}
	marker.Response = testidempotency.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"status":"saved"}`),
	}
	return marker
}

func backupPolicyReplacementSourceEvidence(
	t *testing.T,
	store *memoryHierarchyStore,
	source testkeyvalue.Versioned[testbackuppolicy.BackupSourceRecord],
	attach *testkeyvalue.Versioned[testattachments.Record],
	volume *testenvironmentqueries.BackupVolumeProjectionEvidence,
) testbackuppolicymutations.SourceEvidence {
	t.Helper()
	evidence := testbackuppolicymutations.SourceEvidence{
		Source: source,
		EnvironmentIndex: mustBackupPolicyIndex(
			t, store, testbackuppolicy.BackupSourceEnvironmentKey(source.Record.EnvironmentID, source.Record.ID),
		),
		IdentityIndex: mustBackupPolicyIndex(
			t,
			store,
			testbackuppolicy.BackupSourceIdentityKey(
				source.Record.EnvironmentID,
				source.Record.Kind,
				source.Record.TargetID,
			),
		),
		Attach: attach,
		Volume: volume,
	}
	if attach != nil {
		evidence.TargetOwnerIndex = mustBackupPolicyIndex(
			t, store, testattachments.AttachOwnerKey(source.Record.EnvironmentID, attach.Record.ID),
		)
	}
	return evidence
}

func seedBackupPolicyVolumeProjection(
	t *testing.T,
	store *memoryHierarchyStore,
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord],
	project testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	volume testenvironmentprojection.EnvironmentVolumeIdentity,
	seed int64,
) testenvironmentqueries.BackupVolumeProjectionEvidence {
	t.Helper()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, seed)
	projection := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	projection.Volumes = []testenvironmentprojection.EnvironmentVolumeIdentity{volume}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	artifact.Volumes = []*agentpb.ComposeVolume{{VolumeId: volume.ID, ComposeName: volume.Key}}
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	revision := environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n")
	marker := environmentBlueprintTestMarker(task, environment.Record.ID)
	stageEnvironmentBlueprintForPublicationTest(t, hierarchy, 0, revision, projection, marker)
	headValue, err := testidempotency.EncodeTaskReference(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), []testkeyvalue.Condition{{
		Key: testblueprints.EnvironmentBlueprintHeadKey(environment.Record.ID), ModRevision: 0,
	}}, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(environment.Record.ID), Value: headValue,
	}})
	clear(headValue)
	if err != nil || !result.Succeeded {
		t.Fatalf("publish backup Volume fixture = %#v, %v", result, err)
	}
	evidence, err := testenvironmentqueries.LoadBackupVolumeProjectionEvidence(
		context.Background(), store, environment.Record.ID, volume.ID, result.Revision,
	)
	if err != nil {
		t.Fatalf("load backup Volume fixture = %v", err)
	}
	return evidence
}

func mustBackupPolicyIndex(t *testing.T, store *memoryHierarchyStore, key string) *testkeyvalue.KeyValue {
	t.Helper()
	result, err := store.Get(context.Background(), key)
	if err != nil || result == nil || result.Entry == nil {
		t.Fatalf("index %q read = %#v, %v", key, result, err)
	}
	return result.Entry
}

func mustBackupPolicy(
	t *testing.T,
	repository *BackupPolicyRepository,
	environmentID string,
) testkeyvalue.Versioned[testbackuppolicy.BackupPolicyRecord] {
	t.Helper()
	record, found, err := repository.GetBackupPolicy(context.Background(), environmentID)
	if err != nil || !found {
		t.Fatalf("GetBackupPolicy() = %#v, %t, %v", record, found, err)
	}
	return record
}

func mustBackupKey(
	t *testing.T,
	store *memoryHierarchyStore,
	environmentID string,
) testbackuppolicy.VersionedBackupKey {
	t.Helper()
	key, found, err := backupPolicyKeyReader(t, store).GetBackupKey(context.Background(), environmentID)
	if err != nil || !found {
		t.Fatalf("GetBackupKey() = %#v, %t, %v", key, found, err)
	}
	return key
}

func mustBackupPolicyReference(
	t *testing.T,
	store *memoryHierarchyStore,
	connectorID string,
	environmentID string,
) *testkeyvalue.KeyValue {
	t.Helper()
	result, err := store.Get(
		context.Background(), testbackuppolicy.BackupPolicyConnectorReferenceKey(connectorID, environmentID),
	)
	if err != nil || result == nil || result.Entry == nil {
		t.Fatalf("Connector reference read = %#v, %v", result, err)
	}
	return result.Entry
}

func assertBackupPolicyReference(
	t *testing.T,
	store *memoryHierarchyStore,
	connectorID string,
	environmentID string,
	want bool,
) {
	t.Helper()
	result, err := store.Get(
		context.Background(), testbackuppolicy.BackupPolicyConnectorReferenceKey(connectorID, environmentID),
	)
	if err != nil || result == nil || (result.Entry != nil) != want {
		t.Fatalf("Connector reference present = %t, want %t; result/error = %#v/%v",
			result != nil && result.Entry != nil, want, result, err)
	}
}

type backupPolicyReplacementUnknownStore struct {
	*memoryHierarchyStore
	failNext error
}

func (store *backupPolicyReplacementUnknownStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	result, err := store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.failNext != nil {
		failure := store.failNext
		store.failNext = nil
		return testkeyvalue.TransactionResult{}, failure
	}
	return result, err
}

func replaceBackupSchedulePolicy(
	t *testing.T,
	fixture *backupPolicyReplacementFixture,
	input testbackuppolicy.BackupPolicyReplacementInput,
	at time.Time,
	key string,
) testbackupqueries.BackupPolicyProjection {
	t.Helper()
	prepared, err := fixture.repository.PrepareBackupPolicyReplacement(
		context.Background(), input,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Destroy()
	prepared, err = prepared.FinalizeSchedule(at)
	if err != nil {
		t.Fatal(err)
	}
	projection := prepared.Projection()
	result, err := fixture.repository.ReplaceBackupPolicyProtected(
		context.Background(),
		prepared,
		backupPolicyReplacementMarker(fixture.environment.Record.ID, key),
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("ReplaceBackupPolicyProtected() = %#v, %v", result, err)
	}
	return projection
}

func backupSchedulePolicyInput(
	fixture *backupPolicyReplacementFixture,
	enabled bool,
) testbackuppolicy.BackupPolicyReplacementInput {
	volume := fixture.sources[0].Source.Record
	return testbackuppolicy.BackupPolicyReplacementInput{
		EnvironmentID: fixture.environment.Record.ID,
		Enabled:       enabled, Frequency: "*-*-* 03:00:00", Keep: 7,
		Encryption: "none", ConnectorID: fixture.connector.Record.Connector.ID,
		Sources: []testbackuppolicy.BackupPolicySourceSelection{{
			Kind: core.BackupSourceVolume, TargetID: volume.TargetID,
		}},
	}
}

// Rationale: the policy, nullable next_run_at source state, and coordination
// transition are one protected commit; a delayed first tick still selects only
// the latest occurrence since that exact transition boundary.
func TestBackupPolicyReplacementPublishesScheduleAtomicallyAndLatestCatchUp(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, true)
	transition := fixture.now.Add(time.Minute)
	projection := replaceBackupSchedulePolicy(
		t, fixture, backupSchedulePolicyInput(fixture, true), transition,
		"backup-schedule-atomic-0001",
	)
	wantNext := time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)
	if projection.NextRunAt == nil || !projection.NextRunAt.Equal(wantNext) {
		t.Fatalf("next_run_at = %v, want %s", projection.NextRunAt, wantNext)
	}
	read, err := fixture.store.GetMany(
		context.Background(),
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testbackuppolicy.BackupPolicyKey(fixture.environment.Record.ID),
				testenvironmentcoordination.Key(fixture.environment.Record.ID),
			},
		},
	)
	if err != nil || read == nil || len(read.Values) != 2 ||
		read.Values[0] == nil || read.Values[1] == nil ||
		read.Values[0].ModRevision != read.Values[1].ModRevision {
		t.Fatalf("atomic policy/coordination read = %#v, %v", read, err)
	}
	runtime := testbackupscheduling.New(fixture.store)
	delayed := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	evaluation, err := runtime.EvaluateBackupSchedule(
		context.Background(), fixture.environment.Record.ID, delayed,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantLatest := time.Date(2026, 8, 27, 3, 0, 0, 0, time.UTC)
	if !evaluation.Due || !evaluation.ScheduledAt.Equal(wantLatest) ||
		!evaluation.EvaluatedAt.Equal(delayed) {
		t.Fatalf("delayed evaluation = %#v, want latest %s", evaluation, wantLatest)
	}
}

// Rationale: disable clears only schedule state while advancing the monotonic
// floor, and re-enable seeds strictly from its own later transition boundary.
func TestBackupPolicyDisableReenableCannotReplayDisabledOccurrences(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, true)
	input := backupSchedulePolicyInput(fixture, true)
	replaceBackupSchedulePolicy(
		t, fixture, input, fixture.now.Add(time.Minute),
		"backup-schedule-enable-0001",
	)
	disabledAt := fixture.now.Add(48 * time.Hour)
	input.Enabled = false
	disabled := replaceBackupSchedulePolicy(
		t, fixture, input, disabledAt,
		"backup-schedule-disable-0001",
	)
	if disabled.NextRunAt != nil {
		t.Fatalf("disabled next_run_at = %v", disabled.NextRunAt)
	}
	reenabledAt := disabledAt.Add(2 * time.Hour)
	input.Enabled = true
	reenabled := replaceBackupSchedulePolicy(
		t, fixture, input, reenabledAt,
		"backup-schedule-reenable-0001",
	)
	wantNext := time.Date(2026, 8, 26, 3, 0, 0, 0, time.UTC)
	if reenabled.NextRunAt == nil || !reenabled.NextRunAt.Equal(wantNext) {
		t.Fatalf("re-enabled next_run_at = %v, want %s", reenabled.NextRunAt, wantNext)
	}
	runtime := testbackupscheduling.New(fixture.store)
	beforeNext, err := runtime.EvaluateBackupSchedule(
		context.Background(), fixture.environment.Record.ID, reenabledAt.Add(30*time.Minute),
	)
	if err != nil || beforeNext.Due {
		t.Fatalf("re-enabled pre-occurrence evaluation = %#v, %v", beforeNext, err)
	}
}

// Rationale: a held Environment operation lock publishes one immutable overlap
// outcome with coordination progress and never creates a competing Task.
func TestBackupScheduleOverlapPublishesOutcomeAtomically(t *testing.T) {
	fixture := newBackupPolicyReplacementFixture(t, true)
	replaceBackupSchedulePolicy(
		t, fixture, backupSchedulePolicyInput(fixture, true), fixture.now,
		"backup-schedule-overlap-0001",
	)
	lock, err := fixture.store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type:  testkeyvalue.MutationPut,
		Key:   testhierarchy.EnvironmentOperationLockKey(fixture.environment.Record.ID),
		Value: []byte("held"),
	}})
	if err != nil || !lock.Succeeded {
		t.Fatalf("seed operation lock = %#v, %v", lock, err)
	}
	runtime := testbackupscheduling.New(fixture.store)
	now := time.Date(2026, 8, 24, 4, 0, 0, 0, time.UTC)
	evaluation, err := runtime.EvaluateBackupSchedule(
		context.Background(), fixture.environment.Record.ID, now,
	)
	if err != nil || !evaluation.Due || !evaluation.Overlap {
		t.Fatalf("overlap evaluation = %#v, %v", evaluation, err)
	}
	if err := runtime.SkipScheduledBackup(context.Background(), evaluation, now); err != nil {
		t.Fatal(err)
	}
	dueKey, err := testbackupruntime.BackupDueOutcomeKey(
		evaluation.EnvironmentID, evaluation.PolicyRevision, evaluation.ScheduledAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	dueValue, err := fixture.store.Get(context.Background(), dueKey)
	if err != nil || dueValue == nil || dueValue.Entry == nil {
		t.Fatalf("due outcome read = %#v, %v", dueValue, err)
	}
	due, err := testbackupruntime.DecodeBackupDueOutcomeRecord(dueValue.Entry.Value)
	if err != nil || due.Outcome != testbackupruntime.BackupDueSkippedOverlap || due.TaskID != "" {
		t.Fatalf("overlap due outcome = %#v, %v", due, err)
	}
	coordinationValue, err := fixture.store.Get(
		context.Background(), testenvironmentcoordination.Key(evaluation.EnvironmentID),
	)
	if err != nil || coordinationValue == nil || coordinationValue.Entry == nil ||
		coordinationValue.Entry.ModRevision != dueValue.Entry.ModRevision {
		t.Fatalf("atomic overlap coordination = %#v, %v", coordinationValue, err)
	}
}

func mustBackupPolicyCoordination(
	t *testing.T,
	store *memoryHierarchyStore,
	environmentID string,
) testkeyvalue.Versioned[testenvironmentcoordination.EnvironmentCoordinationRecord] {
	t.Helper()
	value, err := store.Get(context.Background(), testenvironmentcoordination.Key(environmentID))
	if err != nil || value == nil || value.Entry == nil {
		t.Fatalf("get Environment coordination = %#v, %v", value, err)
	}
	record, err := testenvironmentcoordination.Decode(value.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	return testkeyvalue.Versioned[testenvironmentcoordination.EnvironmentCoordinationRecord]{
		Record: record, Revision: value.Entry.ModRevision, ReadRevision: value.ReadRevision,
	}
}
