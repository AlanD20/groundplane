package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

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
	credentials, err := NewConnectorEncryptedCredentials(
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
	credentials, err := NewConnectorEncryptedCredentials(
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
	for _, key := range []string{
		connectorRecordKey(record.Connector.ID),
		connectorEnvironmentKey(environment.Record.ID, record.Connector.ID),
		connectorNameKey(environment.Record.ID, record.Connector.Name),
		connectorCredentialValueKey(record.Connector.ID),
	} {
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
	credentials, err := NewConnectorEncryptedCredentials(
		record.Connector.ID,
		[]byte("sealed-credentials"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(credentials.Ciphertext)
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment,
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
	fixture := newConnectorDeletionFixture(t)
	repository, err := newConnectorRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	task, marker, tombstone, intent := connectorDeletionTestTask(
		t,
		fixture.connector,
		fixture.project,
		fixture.environment,
		fixture.now.Add(time.Minute),
		13050,
	)
	epoch := mustEnvironmentMutationEpochRevision(t, fixture.store, fixture.environment.Record.ID)
	putEnvironmentMutationFenceTestLock(
		t,
		fixture.store,
		fixture.environment.Record.ID,
		environmentMutationFenceTestOwner(serviceRecordTestTime(), 13051),
	)
	if _, err := repository.BeginConnectorDeletionWithTask(
		context.Background(), fixture.environment, fixture.project, fixture.connector,
		tombstone, intent, task, marker,
	); !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("BeginConnectorDeletionWithTask(held lock) error = %v", err)
	}
	for _, key := range []string{
		taskKey(task.ID),
		deletionTombstoneKey(string(DeletionTargetConnector), fixture.connector.Record.Connector.ID),
		connectorRemovalIntentKey(task.ID),
	} {
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

// Rationale: Blueprint publication must use one fixed revision, advance the
// epoch with its desired-state writes, and replay read-only under a later lock.
func TestEnvironmentBlueprintUsesFixedRevisionAdvancesEpochAndReplaysReadOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	base, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, base)
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 13060)
	revision := environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n")
	projection := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	zoneChanges := environmentBlueprintTestZoneChanges(t, base, projection)
	serviceChanges := environmentBlueprintTestServiceChanges(t, base, projection)
	routeChanges := environmentBlueprintTestRouteChanges(t, base, projection)
	marker := environmentBlueprintTestMarker(task, environment.Record.ID)
	claim := stageEnvironmentBlueprintForPublicationTest(t, base, 0, revision, projection, marker)
	audited := &entryMutationRevisionAuditStore{hierarchyStore: store}
	repository, err := newHierarchyRepository(audited)
	if err != nil {
		t.Fatal(err)
	}
	first, err := repository.PublishEnvironmentDesiredRevisionWithTask(
		ctx, project, environment, 0, claim,
		EnvironmentDesiredRevisionIdentity{
			EnvironmentID: revision.EnvironmentID,
			RevisionID:    revision.RevisionID,
		},
		projection, zoneChanges, serviceChanges, routeChanges, ReleaseGroupBlueprintPreparedMutation{}, ComponentTaskPreparation{}, BlueprintAttachTaskPreparation{}, task, marker,
	)
	if err != nil {
		t.Fatalf("PublishEnvironmentDesiredRevisionWithTask() error = %v", err)
	}
	if outcome, _, conflict, classifyErr := first.Classify(); classifyErr != nil ||
		conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("Blueprint apply = %v/%v/%v", outcome, conflict, classifyErr)
	}
	if audited.anchorRevision <= 0 || len(audited.fixedRevisions) == 0 {
		t.Fatalf(
			"Blueprint anchor/fixed revisions = %d/%v",
			audited.anchorRevision,
			audited.fixedRevisions,
		)
	}
	for _, fixed := range audited.fixedRevisions {
		if fixed != audited.anchorRevision {
			t.Fatalf("Blueprint fixed revision = %d, want %d", fixed, audited.anchorRevision)
		}
	}
	head, found, err := repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("GetEnvironmentBlueprintHead() = %#v/%v/%v", head, found, err)
	}
	epoch := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID)
	if epoch != head.Revision {
		t.Fatalf("Blueprint epoch revision = %d, want %d", epoch, head.Revision)
	}
	putEnvironmentMutationFenceTestLock(
		t,
		store,
		environment.Record.ID,
		environmentMutationFenceTestOwner(serviceRecordTestTime(), 13061),
	)
	replayed, err := repository.PublishEnvironmentDesiredRevisionWithTask(
		ctx, project, environment, 0, claim,
		EnvironmentDesiredRevisionIdentity{
			EnvironmentID: revision.EnvironmentID,
			RevisionID:    revision.RevisionID,
		},
		projection, zoneChanges, serviceChanges, routeChanges, ReleaseGroupBlueprintPreparedMutation{}, ComponentTaskPreparation{}, BlueprintAttachTaskPreparation{}, task, marker,
	)
	if err != nil {
		t.Fatalf("PublishEnvironmentDesiredRevisionWithTask(replay) error = %v", err)
	}
	outcome, existing, conflict, classifyErr := replayed.Classify()
	defer clear(existing.Intent.Ciphertext)
	defer clear(existing.Response.Body)
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownExisting {
		t.Fatalf("Blueprint replay = %v/%v/%v", outcome, conflict, classifyErr)
	}
	if after := mustEnvironmentMutationEpochRevision(
		t,
		store,
		environment.Record.ID,
	); after != epoch {
		t.Fatalf("Blueprint replay epoch = %d, want %d", after, epoch)
	}
}

// Rationale: a Blueprint epoch race must prevent its head, Task, immutable
// revision, projection, and every desired record from becoming visible.
func TestEnvironmentBlueprintEpochRacePerformsNoDomainWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	base, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, base)
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 13070)
	revision := environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n")
	projection := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	zoneChanges := environmentBlueprintTestZoneChanges(t, base, projection)
	serviceChanges := environmentBlueprintTestServiceChanges(t, base, projection)
	routeChanges := environmentBlueprintTestRouteChanges(t, base, projection)
	marker := environmentBlueprintTestMarker(task, environment.Record.ID)
	claim := stageEnvironmentBlueprintForPublicationTest(t, base, 0, revision, projection, marker)
	racing := &entryVolumeEpochRaceStore{hierarchyStore: store}
	racing.beforeTransact = func() {
		advanceEnvironmentMutationFenceEpoch(t, store, environment.Record.ID)
	}
	repository, err := newHierarchyRepository(racing)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repository.PublishEnvironmentDesiredRevisionWithTask(
		ctx,
		project,
		environment,
		0,
		claim,
		EnvironmentDesiredRevisionIdentity{
			EnvironmentID: revision.EnvironmentID,
			RevisionID:    revision.RevisionID,
		},
		projection,
		zoneChanges,
		serviceChanges,
		routeChanges,
		ReleaseGroupBlueprintPreparedMutation{},
		ComponentTaskPreparation{},
		BlueprintAttachTaskPreparation{},
		task,
		marker,
	)
	if err != nil {
		t.Fatalf("PublishEnvironmentDesiredRevisionWithTask(epoch race) error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || outcome != IdempotencyKnownConflict ||
		!isKind(conflict, errs.KindStateConflict) {
		t.Fatalf("Blueprint race = %v/%v/%v", outcome, conflict, classifyErr)
	}
	for _, key := range []string{
		environmentBlueprintHeadKey(environment.Record.ID),
		taskKey(task.ID),
	} {
		stored, getErr := store.Get(ctx, key)
		if getErr != nil || stored.Entry != nil {
			t.Fatalf("failed Blueprint publication key %q = %#v, %v", key, stored, getErr)
		}
	}
}

// Rationale: Connector and Blueprint ordinary publication must reject a held
// Environment persistence lock before exposing any partial durable state.
func TestConnectorAndBlueprintPublicationRejectHeldEnvironmentLock(t *testing.T) {
	t.Parallel()
	t.Run("Connector", func(t *testing.T) {
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
		credentials, err := NewConnectorEncryptedCredentials(record.Connector.ID, []byte("sealed"))
		if err != nil {
			t.Fatal(err)
		}
		defer clear(credentials.Ciphertext)
		if _, err := repository.CreateConnector(
			context.Background(), environment, project, record, credentials,
		); !isKind(err, errs.KindResourceInUse) {
			t.Fatalf("CreateConnector() error = %v", err)
		}
	})
	t.Run("Blueprint", func(t *testing.T) {
		t.Parallel()
		store := newMemoryHierarchyStore()
		repository, err := newHierarchyRepository(store)
		if err != nil {
			t.Fatal(err)
		}
		project, environment := createEnvironmentBlueprintOwners(t, repository)
		task := environmentBlueprintTestTask(t, project.Record, environment.Record, 70)
		revision := environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n")
		projection := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
		marker := environmentBlueprintTestMarker(task, environment.Record.ID)
		claim := stageEnvironmentBlueprintForPublicationTest(
			t, repository, 0, revision, projection, marker,
		)
		putEnvironmentMutationFenceTestLock(
			t,
			store,
			environment.Record.ID,
			environmentMutationFenceTestOwner(
				time.Date(2026, 8, 24, 23, 10, 0, 0, time.UTC),
				13010,
			),
		)
		_, err = repository.PublishEnvironmentDesiredRevisionWithTask(
			context.Background(),
			project,
			environment,
			0,
			claim,
			EnvironmentDesiredRevisionIdentity{
				EnvironmentID: revision.EnvironmentID,
				RevisionID:    revision.RevisionID,
			},
			projection,
			nil,
			nil,
			nil,
			ReleaseGroupBlueprintPreparedMutation{},
			ComponentTaskPreparation{},
			BlueprintAttachTaskPreparation{},
			task,
			marker,
		)
		if !isKind(err, errs.KindResourceInUse) {
			t.Fatalf("PublishEnvironmentDesiredRevisionWithTask() error = %v", err)
		}
	})
}

// Rationale: the fully wrapped Blueprint transaction, not an incomplete base
// estimate, must fit the exact 96 compare-and-mutation ceiling.
func TestEnvironmentBlueprintTransactionOperationCountIncludesIdempotency(t *testing.T) {
	t.Parallel()
	marker := testDirectMarker()
	plan := &idempotencyMutationPlan{
		conditions: make([]Condition, 47),
		mutations:  make([]Mutation, 46),
	}
	if got := environmentBlueprintTransactionOperationCount(plan, marker); got != 96 {
		t.Fatalf("operation count = %d, want 96", got)
	}
	plan.conditions = append(plan.conditions, Condition{})
	if got := environmentBlueprintTransactionOperationCount(plan, marker); got != 97 {
		t.Fatalf("above-bound operation count = %d, want 97", got)
	}
}

// Rationale: Blueprint file content may consume exactly the accepted 768 KiB
// payload ceiling but must reject the first byte beyond it.
func TestEnvironmentBlueprintTotalFileByteCeilingIsExact(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 24, 23, 20, 0, 0, time.UTC)
	revision := EnvironmentBlueprintRevision{
		EnvironmentID:  "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RevisionID:     "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RootPath:       "one.yaml",
		ComposeSources: []string{"one.yaml"},
		Files: []EnvironmentBlueprintFile{
			{Path: "one.yaml", Content: make([]byte, environmentBlueprintMaxFileBytes)},
			{Path: "three.yaml", Content: make([]byte, environmentBlueprintMaxFileBytes)},
			{Path: "two.yaml", Content: make([]byte, environmentBlueprintMaxFileBytes)},
		},
		CreatedAt: at,
	}
	if err := validateEnvironmentBlueprintRevision(revision); err != nil {
		t.Fatalf("validateEnvironmentBlueprintRevision(exact) error = %v", err)
	}
	revision.Files = append(
		revision.Files,
		EnvironmentBlueprintFile{Path: "four.yaml", Content: []byte("x")},
	)
	if err := validateEnvironmentBlueprintRevision(
		revision,
	); !isKind(
		err,
		errs.KindValidationFailed,
	) {
		t.Fatalf("validateEnvironmentBlueprintRevision(above) error = %v", err)
	}
}
