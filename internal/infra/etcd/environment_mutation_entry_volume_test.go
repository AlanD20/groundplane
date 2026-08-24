package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: ordinary Entry and Volume publication must stop before writing
// whenever an Environment persistence operation owns the canonical lock.
func TestEntryAndVolumePublicationRejectHeldEnvironmentLock(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		run  func(*testing.T, *memoryHierarchyStore, Versioned[EnvironmentRecord], Versioned[ProjectRecord]) error
	}{
		{name: "Entry", run: func(
			t *testing.T,
			store *memoryHierarchyStore,
			environment Versioned[EnvironmentRecord],
			project Versioned[ProjectRecord],
		) error {
			repository, err := newEntryRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			record, generation := environmentMutationTestEntry(t, environment.Record.ID, 12000)
			_, err = repository.CreateEntry(context.Background(), environment, project, record, generation)
			return err
		}},
		{name: "Volume", run: func(
			t *testing.T,
			store *memoryHierarchyStore,
			environment Versioned[EnvironmentRecord],
			project Versioned[ProjectRecord],
		) error {
			repository, err := newVolumeRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			_, err = repository.CreateVolume(
				context.Background(),
				environment,
				project,
				volumeRepositoryTestRecord(t, environment.Record.ID, 12001, "locked"),
			)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, store, environment, project, _ := routeRepositoryTestHierarchy(t)
			if test.name == "Volume" {
				environment.Record.ProvisioningState = EnvironmentProvisioningReady
			}
			putEnvironmentMutationFenceTestLock(
				t,
				store,
				environment.Record.ID,
				environmentMutationFenceTestOwner(
					time.Date(2026, 8, 24, 21, 0, 0, 0, time.UTC),
					12002,
				),
			)
			if err := test.run(t, store, environment, project); !isKind(err, errs.KindResourceInUse) {
				t.Fatalf("publication error = %v", err)
			}
		})
	}
}

// Rationale: an epoch race after fixed evidence is captured must reject all
// Entry or Volume writes, including the epoch rewrite itself.
func TestEntryAndVolumeEpochRacePerformsNoWrites(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		key  func(string) string
		run  func(*testing.T, hierarchyStore, Versioned[EnvironmentRecord], Versioned[ProjectRecord]) error
	}{
		{name: "Entry", key: entryRecordKey, run: func(
			t *testing.T,
			store hierarchyStore,
			environment Versioned[EnvironmentRecord],
			project Versioned[ProjectRecord],
		) error {
			repository, err := newEntryRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			record, generation := environmentMutationTestEntry(t, environment.Record.ID, 12100)
			_, err = repository.CreateEntry(context.Background(), environment, project, record, generation)
			return err
		}},
		{name: "Volume", key: volumeKey, run: func(
			t *testing.T,
			store hierarchyStore,
			environment Versioned[EnvironmentRecord],
			project Versioned[ProjectRecord],
		) error {
			repository, err := newVolumeRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			_, err = repository.CreateVolume(
				context.Background(),
				environment,
				project,
				volumeRepositoryTestRecord(t, environment.Record.ID, 12100, "racing"),
			)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, base, environment, project, _ := routeRepositoryTestHierarchy(t)
			if test.name == "Volume" {
				environment.Record.ProvisioningState = EnvironmentProvisioningReady
			}
			stableID := ids.NewAt(ids.KindEnvEntry, serviceRecordTestTime(), 12100)
			if test.name == "Volume" {
				stableID = ids.NewAt(ids.KindVolume, serviceRecordTestTime(), 12100)
			}
			racing := &entryVolumeEpochRaceStore{hierarchyStore: base}
			racing.beforeTransact = func() {
				advanceEnvironmentMutationFenceEpoch(t, base, environment.Record.ID)
			}
			if err := test.run(t, racing, environment, project); !isKind(err, errs.KindStateConflict) {
				t.Fatalf("publication error = %v", err)
			}
			stored, err := base.Get(context.Background(), test.key(stableID))
			if err != nil || stored.Entry != nil {
				t.Fatalf("failed publication primary = %#v, %v", stored, err)
			}
		})
	}
}

// Rationale: Entry owner proof and every Environment fence read must share
// the current domain anchor revision rather than mixing independently current reads.
func TestEntryReplacementUsesFixedRevisionOwnerEvidence(t *testing.T) {
	t.Parallel()
	store, environment, project := testEntryRepositoryHierarchy(t)
	base, err := newEntryRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record, generation := environmentMutationTestEntry(t, environment.Record.ID, 12200)
	created, err := base.CreateEntry(context.Background(), environment, project, record, generation)
	if err != nil {
		t.Fatalf("CreateEntry() error = %v", err)
	}
	audited := &entryMutationRevisionAuditStore{hierarchyStore: store}
	repository, err := newEntryRepository(audited)
	if err != nil {
		t.Fatal(err)
	}
	desired := created.Record.Entry
	desired.Source.Literal = "fixed"
	generationID := ids.NewAt(ids.KindConfig, serviceRecordTestTime(), 12202)
	next := testPlainGeneration(
		environment.Record.ID,
		created.Record.Entry.ID,
		generationID,
		"fixed",
		serviceRecordTestTime(),
	)
	if _, err := repository.ReplaceEntry(
		context.Background(),
		environment,
		project,
		created,
		desired,
		generationID,
		EntryValueGeneration{Plain: &next},
	); err != nil {
		t.Fatalf("ReplaceEntry() error = %v", err)
	}
	if audited.anchorRevision <= 0 || len(audited.fixedRevisions) != 3 {
		t.Fatalf("anchor/fixed revisions = %d/%v", audited.anchorRevision, audited.fixedRevisions)
	}
	for _, revision := range audited.fixedRevisions {
		if revision != audited.anchorRevision {
			t.Fatalf("fixed revision = %d, want %d", revision, audited.anchorRevision)
		}
	}
}

// Rationale: successful resource writes advance the epoch exactly once, while
// an idempotency replay is a read-only response and must leave it unchanged.
func TestEntryAndVolumeWritesAdvanceEpochButReplayDoesNot(t *testing.T) {
	t.Parallel()
	_, store, environment, project, _ := routeRepositoryTestHierarchy(t)
	before := mustBackupPolicyMutationEpoch(t, store, environment.Record.ID)
	entries, err := newEntryRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record, generation := environmentMutationTestEntry(t, environment.Record.ID, 12300)
	if _, err := entries.CreateEntry(context.Background(), environment, project, record, generation); err != nil {
		t.Fatalf("CreateEntry() error = %v", err)
	}
	afterEntry := mustBackupPolicyMutationEpoch(t, store, environment.Record.ID)
	if afterEntry.Revision <= before.Revision {
		t.Fatalf("Entry epoch = %d, want > %d", afterEntry.Revision, before.Revision)
	}
	volumes, err := newVolumeRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	environment.Record.ProvisioningState = EnvironmentProvisioningReady
	volume := volumeRepositoryTestRecord(t, environment.Record.ID, 12301, "replay")
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment,
		ScopeID:   environment.Record.ID,
		Method:    http.MethodPost,
		Route:     "/volumes",
		Key:       "volume-fence-replay-key-0001",
	}
	marker.Response.Status = http.StatusCreated
	if _, err := volumes.CreateVolumeIdempotent(
		context.Background(), environment, project, volume, marker,
	); err != nil {
		t.Fatalf("CreateVolumeIdempotent() error = %v", err)
	}
	afterVolume := mustBackupPolicyMutationEpoch(t, store, environment.Record.ID)
	if afterVolume.Revision <= afterEntry.Revision {
		t.Fatalf("Volume epoch = %d, want > %d", afterVolume.Revision, afterEntry.Revision)
	}
	result, err := volumes.CreateVolumeIdempotent(
		context.Background(), environment, project, volume, marker,
	)
	if err != nil {
		t.Fatalf("CreateVolumeIdempotent(replay) error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownExisting {
		t.Fatalf("replay = %v/%v/%v", outcome, conflict, classifyErr)
	}
	afterReplay := mustBackupPolicyMutationEpoch(t, store, environment.Record.ID)
	if afterReplay.Revision != afterVolume.Revision {
		t.Fatalf("replay epoch = %d, want %d", afterReplay.Revision, afterVolume.Revision)
	}
}

type entryVolumeEpochRaceStore struct {
	hierarchyStore
	beforeTransact func()
}

func (store *entryVolumeEpochRaceStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if store.beforeTransact != nil {
		before := store.beforeTransact
		store.beforeTransact = nil
		before()
	}
	return store.hierarchyStore.Transact(ctx, conditions, mutations)
}

type entryMutationRevisionAuditStore struct {
	hierarchyStore
	anchorRevision int64
	fixedRevisions []int64
}

func (store *entryMutationRevisionAuditStore) GetMany(
	ctx context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	result, err := store.hierarchyStore.GetMany(ctx, request)
	if err == nil && request.Revision == 0 && store.anchorRevision == 0 {
		store.anchorRevision = result.ReadRevision
	} else if err == nil && request.Revision > 0 {
		store.fixedRevisions = append(store.fixedRevisions, request.Revision)
	}
	return result, err
}

func environmentMutationTestEntry(
	t *testing.T,
	environmentID string,
	seed int64,
) (EntryRecord, EntryValueGeneration) {
	t.Helper()
	at := serviceRecordTestTime()
	entryID := ids.NewAt(ids.KindEnvEntry, at, seed)
	generationID := ids.NewAt(ids.KindConfig, at, seed+1)
	desired := core.EnvEntry{
		ID: entryID, Kind: core.EntryKindEnv, Key: "FENCE_VALUE",
		Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: "initial"},
		Exposure: []string{"all"},
	}
	record, err := NewEntryRecord(environmentID, desired, generationID)
	if err != nil {
		t.Fatalf("NewEntryRecord() error = %v", err)
	}
	generation := testPlainGeneration(environmentID, entryID, generationID, "initial", at)
	return record, EntryValueGeneration{Plain: &generation}
}
