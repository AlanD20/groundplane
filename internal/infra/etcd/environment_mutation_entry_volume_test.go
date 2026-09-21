package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: ordinary Entry publication must stop before writing
// whenever an Environment persistence operation owns the canonical lock.
func TestEntryPublicationRejectsHeldEnvironmentLock(t *testing.T) {
	t.Parallel()
	_, store, environment, project, _ := routeRepositoryTestHierarchy(t)
	putEnvironmentMutationFenceTestLock(
		t, store, environment.Record.ID,
		environmentMutationFenceTestOwner(time.Date(2026, 8, 24, 21, 0, 0, 0, time.UTC), 12002),
	)
	repository, err := newEntryRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record, generation := environmentMutationTestEntry(t, environment.Record.ID, 12000)
	if _, err := repository.CreateEntry(
		context.Background(), environment, project, record, generation,
	); !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("publication error = %v", err)
	}
}

// Rationale: an epoch race after fixed evidence is captured must reject all
// Entry writes, including the epoch rewrite itself.
func TestEntryEpochRacePerformsNoWrites(t *testing.T) {
	t.Parallel()
	_, base, environment, project, _ := routeRepositoryTestHierarchy(t)
	stableID := ids.NewAt(ids.KindEnvEntry, serviceRecordTestTime(), 12100)
	racing := &entryVolumeEpochRaceStore{hierarchyStore: base}
	racing.beforeTransact = func() {
		advanceEnvironmentMutationFenceEpoch(t, base, environment.Record.ID)
	}
	repository, err := newEntryRepository(racing)
	if err != nil {
		t.Fatal(err)
	}
	record, generation := environmentMutationTestEntry(t, environment.Record.ID, 12100)
	if _, err := repository.CreateEntry(
		context.Background(), environment, project, record, generation,
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("publication error = %v", err)
	}
	stored, err := base.Get(context.Background(), testentries.RecordKey(stableID))
	if err != nil || stored.Entry != nil {
		t.Fatalf("failed publication primary = %#v, %v", stored, err)
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
		generationID, testentries.EntryValueGeneration{Plain: &next},
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

// Rationale: successful Entry writes advance the Environment epoch.
func TestEntryWriteAdvancesEpoch(t *testing.T) {
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
}

type entryVolumeEpochRaceStore struct {
	hierarchyStore
	beforeTransact func()
}

func (store *entryVolumeEpochRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
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
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
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
) (testentries.Record, testentries.EntryValueGeneration) {
	t.Helper()
	at := serviceRecordTestTime()
	entryID := ids.NewAt(ids.KindEnvEntry, at, seed)
	generationID := ids.NewAt(ids.KindConfig, at, seed+1)
	desired := core.EnvEntry{
		ID: entryID, Kind: core.EntryKindEnv, Key: "FENCE_VALUE",
		Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: "initial"},
		Exposure: []string{"all"},
	}
	record, err := testentries.NewRecord(environmentID, desired, generationID)
	if err != nil {
		t.Fatalf("NewEntryRecord() error = %v", err)
	}
	generation := testPlainGeneration(environmentID, entryID, generationID, "initial", at)
	return record, testentries.EntryValueGeneration{Plain: &generation}
}
