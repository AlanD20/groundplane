package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorremoval"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Connector publication must derive every Environment fence fact
// from its domain anchor and advance the epoch at the publication revision.
func TestConnectorMutationUsesFixedRevisionAndAdvancesEpoch(t *testing.T) {
	t.Parallel()
	store, environment, project := testConnectorHierarchy(t)
	audited := &entryMutationRevisionAuditStore{hierarchyStore: store}
	repository, err := newConnectorRepository(audited)
	if err != nil {
		t.Fatal(err)
	}
	record := testConnectorRecord(
		t,
		environment.Record.ID,
		serviceRecordTestTime(),
		13020,
		"fixed-revision",
	)
	credentials, err := testconnectors.NewEncryptedCredentials(
		record.Connector.ID,
		[]byte("sealed-credentials"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(credentials.Ciphertext)
	created, err := repository.CreateConnector(
		context.Background(),
		environment,
		project,
		record,
		credentials,
	)
	if err != nil {
		t.Fatalf("CreateConnector() error = %v", err)
	}
	if audited.anchorRevision <= 0 || len(audited.fixedRevisions) == 0 {
		t.Fatalf("anchor/fixed revisions = %d/%v", audited.anchorRevision, audited.fixedRevisions)
	}
	for _, revision := range audited.fixedRevisions {
		if revision != audited.anchorRevision {
			t.Fatalf("fixed revision = %d, want %d", revision, audited.anchorRevision)
		}
	}
	if epoch := mustEnvironmentMutationEpochRevision(
		t,
		store,
		environment.Record.ID,
	); epoch != created.Revision {
		t.Fatalf("Connector epoch revision = %d, want %d", epoch, created.Revision)
	}
}

// Rationale: advancing the Environment epoch after Connector evidence is read
// must reject the complete publication without leaking a primary or indexes.
func TestConnectorEpochRacePerformsNoDomainWrites(t *testing.T) {
	t.Parallel()
	base, environment, project := testConnectorHierarchy(t)
	racing := &entryVolumeEpochRaceStore{hierarchyStore: base}
	racing.beforeTransact = func() {
		advanceEnvironmentMutationFenceEpoch(t, base, environment.Record.ID)
	}
	repository, err := newConnectorRepository(racing)
	if err != nil {
		t.Fatal(err)
	}
	record := testConnectorRecord(
		t,
		environment.Record.ID,
		serviceRecordTestTime(),
		13030,
		"epoch-race",
	)
	credentials, err := testconnectors.NewEncryptedCredentials(
		record.Connector.ID,
		[]byte("sealed-credentials"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(credentials.Ciphertext)
	if _, err := repository.CreateConnector(
		context.Background(), environment, project, record, credentials,
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("CreateConnector(epoch race) error = %v", err)
	}
	for _, key := range []string{testconnectors.RecordKey(record.Connector.ID), testconnectors.ConnectorEnvironmentKey(environment.Record.ID, record.Connector.ID), testconnectors.ConnectorNameKey(environment.Record.ID, record.Connector.Name), testconnectors.CredentialValueKey(record.Connector.ID)} {
		result, getErr := base.Get(context.Background(), key)
		if getErr != nil || result.Entry != nil {
			t.Fatalf("failed Connector publication key %q = %#v, %v", key, result, getErr)
		}
	}
}

// Rationale: a completed Connector create replay is read-only and remains
// available after an Environment operation acquires the ordinary mutation lock.
func TestConnectorCreateReplayDoesNotAdvanceEpoch(t *testing.T) {
	t.Parallel()
	store, environment, project := testConnectorHierarchy(t)
	repository, err := newConnectorRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record := testConnectorRecord(
		t,
		environment.Record.ID,
		serviceRecordTestTime(),
		13040,
		"replay",
	)
	credentials, err := testconnectors.NewEncryptedCredentials(
		record.Connector.ID,
		[]byte("sealed-credentials"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(credentials.Ciphertext)
	marker := testDirectMarker()
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment,
		ScopeID:   environment.Record.ID,
		Method:    http.MethodPost,
		Route:     "/connectors",
		Key:       "connector-fence-replay-key-0001",
	}
	marker.Response.Status = http.StatusCreated
	first, err := repository.CreateConnectorIdempotent(
		context.Background(), environment, project, record, credentials, marker,
	)
	if err != nil {
		t.Fatalf("CreateConnectorIdempotent() error = %v", err)
	}
	if outcome, _, conflict, classifyErr := first.Classify(); classifyErr != nil ||
		conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("Connector create = %v/%v/%v", outcome, conflict, classifyErr)
	}
	epoch := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID)
	putEnvironmentMutationFenceTestLock(
		t,
		store,
		environment.Record.ID,
		environmentMutationFenceTestOwner(serviceRecordTestTime(), 13041),
	)
	replayed, err := repository.CreateConnectorIdempotent(
		context.Background(), environment, project, record, credentials, marker,
	)
	if err != nil {
		t.Fatalf("CreateConnectorIdempotent(replay) error = %v", err)
	}
	outcome, existing, conflict, classifyErr := replayed.Classify()
	defer clear(existing.Intent.Ciphertext)
	defer clear(existing.Response.Body)
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownExisting {
		t.Fatalf("Connector replay = %v/%v/%v", outcome, conflict, classifyErr)
	}
	if after := mustEnvironmentMutationEpochRevision(
		t,
		store,
		environment.Record.ID,
	); after != epoch {
		t.Fatalf("Connector replay epoch = %d, want %d", after, epoch)
	}
}

// Rationale: Connector deletion Task publication is an ordinary mutation and
// must stop before every Task, tombstone, and intent write while the lock is held.
func TestConnectorDeletionRejectsHeldEnvironmentLockWithoutWrites(t *testing.T) {
	t.Parallel()
	fixture := nativeConnectorRemovalFixture(t)
	epoch := mustEnvironmentMutationEpochRevision(t, fixture.store, fixture.environment.Record.ID)
	putEnvironmentMutationFenceTestLock(
		t,
		fixture.store,
		fixture.environment.Record.ID,
		environmentMutationFenceTestOwner(serviceRecordTestTime(), 13051),
	)
	if _, err := fixture.repository.PublishConnectorRemoval(
		context.Background(), fixture.environment, fixture.project, fixture.current,
		fixture.input, fixture.task, fixture.marker,
	); !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("PublishConnectorRemoval(held lock) error = %v", err)
	}
	for _, key := range []string{workqueue.TaskKey(fixture.task.ID), testdeletions.TombstoneKey(string(testdeletions.DeletionTargetConnector), fixture.current.Record.Connector.ID), connectorremoval.InputKey(fixture.input.WorkItemID)} {
		result, getErr := fixture.store.Get(context.Background(), key)
		if getErr != nil || result.Entry != nil {
			t.Fatalf("blocked Connector deletion key %q = %#v, %v", key, result, getErr)
		}
	}
	if after := mustEnvironmentMutationEpochRevision(
		t,
		fixture.store,
		fixture.environment.Record.ID,
	); after != epoch {
		t.Fatalf("blocked Connector deletion epoch = %d, want %d", after, epoch)
	}
}

// Rationale: a held Environment lock must reject Connector creation before
// exposing credentials or partial indexes.
func TestConnectorCreationRejectsHeldEnvironmentLock(t *testing.T) {
	t.Parallel()
	store, environment, project := testConnectorHierarchy(t)
	putEnvironmentMutationFenceTestLock(
		t,
		store,
		environment.Record.ID,
		environmentMutationFenceTestOwner(time.Date(2026, 8, 24, 23, 0, 0, 0, time.UTC), 13000),
	)
	repository, err := newConnectorRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record := testConnectorRecord(
		t,
		environment.Record.ID,
		serviceRecordTestTime(),
		13001,
		"locked",
	)
	credentials, err := testconnectors.NewEncryptedCredentials(record.Connector.ID, []byte("sealed"))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(credentials.Ciphertext)
	if _, err := repository.CreateConnector(
		context.Background(), environment, project, record, credentials,
	); !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("CreateConnector() error = %v", err)
	}
}

// Rationale: Blueprint file content may consume exactly the accepted 768 KiB
// payload ceiling but must reject the first byte beyond it.
func TestEnvironmentBlueprintTotalFileByteCeilingIsExact(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 24, 23, 20, 0, 0, time.UTC)
	revision := testblueprints.EnvironmentBlueprintRevision{
		EnvironmentID:  "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RevisionID:     "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RootPath:       "one.yaml",
		ComposeSources: []string{"one.yaml"},
		Files: []testblueprints.EnvironmentBlueprintFile{
			{Path: "one.yaml", Content: make([]byte, testblueprints.EnvironmentBlueprintMaxFileBytes)},
			{Path: "three.yaml", Content: make([]byte, testblueprints.EnvironmentBlueprintMaxFileBytes)},
			{Path: "two.yaml", Content: make([]byte, testblueprints.EnvironmentBlueprintMaxFileBytes)},
		},
		CreatedAt: at,
	}
	if err := testblueprints.ValidateEnvironmentBlueprintRevision(revision); err != nil {
		t.Fatalf("validateEnvironmentBlueprintRevision(exact) error = %v", err)
	}
	revision.Files = append(
		revision.Files, testblueprints.EnvironmentBlueprintFile{Path: "four.yaml", Content: []byte("x")},
	)
	if err := testblueprints.ValidateEnvironmentBlueprintRevision(
		revision,
	); !isKind(
		err,
		errs.KindValidationFailed,
	) {
		t.Fatalf("validateEnvironmentBlueprintRevision(above) error = %v", err)
	}
}
