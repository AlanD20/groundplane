package etcd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestBlueprintScriptGenerationAccepts64UnchangedScripts(t *testing.T) {
	store := newMemoryHierarchyStore()
	repository, current := scriptBlueprintSeedActiveSet(t, store, 64)
	desired := make([]testscripts.Record, len(current))
	for index := range current {
		desired[index] = current[index].Record
	}
	publication, err := repository.PrepareBlueprintScriptPublication(
		context.Background(), current[0].Record.EnvironmentID, current[0].ReadRevision,
		scriptBlueprintGenerationID(900), current, desired, nil,
	)
	if err != nil {
		t.Fatalf("PrepareBlueprintScriptPublication(64 unchanged) error = %v", err)
	}
	publication.Clear()
}

func TestBlueprintScriptGenerationAccepts64Creates(t *testing.T) {
	store := newMemoryHierarchyStore()
	repository, environmentID, revision := scriptBlueprintEmptyActiveSet(t, store)
	desired := scriptBlueprintRecords(t, environmentID, 64, "created")
	generations := make([]testscripts.BodyGenerationRecord, len(desired))
	for index := range desired {
		generations[index] = scriptBlueprintGeneration(desired[index])
	}
	publication, err := repository.PrepareBlueprintScriptPublication(
		context.Background(), environmentID, revision, scriptBlueprintGenerationID(901), nil, desired, generations,
	)
	if err != nil {
		t.Fatalf("PrepareBlueprintScriptPublication(64 create) error = %v", err)
	}
	publication.Clear()
}

func TestBlueprintScriptPartialStageIsInvisible(t *testing.T) {
	base := newMemoryHierarchyStore()
	_, environmentID, revision := scriptBlueprintEmptyActiveSet(t, base)
	store := &scriptBlueprintStageStore{memoryHierarchyStore: base, failAt: 2}
	repository, err := newScriptRepository(store)
	if err != nil {
		t.Fatalf("newScriptRepository() error = %v", err)
	}
	desired := scriptBlueprintRecords(t, environmentID, 16, "partial")
	generations := scriptBlueprintGenerations(desired)
	_, err = repository.PrepareBlueprintScriptPublication(
		context.Background(), environmentID, revision, scriptBlueprintGenerationID(902), nil, desired, generations,
	)
	if err == nil {
		t.Fatal("PrepareBlueprintScriptPublication(partial) error = nil")
	}
	_, getErr := repository.GetScript(context.Background(), desired[0].Desired.ID)
	if !errors.Is(getErr, errs.New(errs.KindScriptNotFound, "")) {
		t.Fatalf("GetScript(partially staged) error = %v, want script.not_found", getErr)
	}
}

func TestBlueprintScriptFinalFlipMakesCompleteSetVisible(t *testing.T) {
	store := newMemoryHierarchyStore()
	repository, environmentID, revision := scriptBlueprintEmptyActiveSet(t, store)
	desired := scriptBlueprintRecords(t, environmentID, 10, "visible")
	publication, err := repository.PrepareBlueprintScriptPublication(
		context.Background(), environmentID, revision, scriptBlueprintGenerationID(903), nil,
		desired, scriptBlueprintGenerations(desired),
	)
	if err != nil {
		t.Fatalf("PrepareBlueprintScriptPublication() error = %v", err)
	}
	defer publication.Clear()
	if _, getErr := repository.GetScript(context.Background(), desired[0].Desired.ID); !errors.Is(
		getErr, errs.New(errs.KindScriptNotFound, ""),
	) {
		t.Fatalf("GetScript(before flip) error = %v", getErr)
	}
	result, err := store.Transact(context.Background(), publication.conditions, publication.mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("final Script-set flip = %#v, %v", result, err)
	}
	page, err := repository.ListScripts(context.Background(), environmentID, testkeyvalue.PageRequest{Limit: 64})
	if err != nil || len(page.Items) != len(desired) {
		t.Fatalf("ListScripts(after flip) = %#v, %v", page, err)
	}
}

func TestBlueprintScriptFinalFlipConflictsWithConcurrentDirectEditCAS(t *testing.T) {
	store := newMemoryHierarchyStore()
	repository, current := scriptBlueprintSeedActiveSet(t, store, 1)
	desired := current[0].Record
	desired.Desired.Slug = "blueprint-edit"
	publication, err := repository.PrepareBlueprintScriptPublication(
		context.Background(), desired.EnvironmentID, current[0].ReadRevision,
		scriptBlueprintGenerationID(904), current, []testscripts.Record{desired}, nil,
	)
	if err != nil {
		t.Fatalf("PrepareBlueprintScriptPublication() error = %v", err)
	}
	defer publication.Clear()
	active, err := testscripts.ReadActiveScriptSet(context.Background(), store, desired.EnvironmentID, 0)
	if err != nil {
		t.Fatalf("readActiveScriptSet() error = %v", err)
	}
	direct := current[0].Record
	direct.Desired.Slug = "direct-edit"
	primary, err := testscripts.EncodeRecord(direct)
	if err != nil {
		t.Fatalf("encodeScriptRecord(direct) error = %v", err)
	}
	activeValue, err := testscripts.EncodeScriptSetGeneration(active.Record)
	if err != nil {
		t.Fatalf("encodeScriptSetGeneration() error = %v", err)
	}
	directResult, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testscripts.ScriptSetActiveKey(direct.EnvironmentID), ModRevision: active.Revision},
		{
			Key: testscripts.ScriptSetScriptKey(
				direct.EnvironmentID,
				direct.ScriptSetGeneration,
				direct.Desired.ID,
			),
			ModRevision: current[0].Revision,
		},
	}, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testscripts.ScriptSetScriptKey(direct.EnvironmentID, direct.ScriptSetGeneration, direct.Desired.ID),
			Value: primary,
		},
		{Type: testkeyvalue.MutationPut, Key: testscripts.ScriptSetActiveKey(direct.EnvironmentID), Value: activeValue},
	})
	clear(primary)
	clear(activeValue)
	if err != nil || !directResult.Succeeded {
		t.Fatalf("direct edit CAS = %#v, %v", directResult, err)
	}
	flip, err := store.Transact(context.Background(), publication.conditions, publication.mutations)
	if err != nil || flip.Succeeded {
		t.Fatalf("Blueprint flip after direct edit = %#v, %v", flip, err)
	}
}

func TestBlueprintScriptStageReplayAcceptsExactCommittedBatch(t *testing.T) {
	store := newMemoryHierarchyStore()
	repository, environmentID, revision := scriptBlueprintEmptyActiveSet(t, store)
	desired := scriptBlueprintRecords(t, environmentID, 10, "replay")
	generationID := scriptBlueprintGenerationID(906)
	first, err := repository.PrepareBlueprintScriptPublication(
		context.Background(), environmentID, revision, generationID, nil,
		desired, scriptBlueprintGenerations(desired),
	)
	if err != nil {
		t.Fatalf("PrepareBlueprintScriptPublication(first) error = %v", err)
	}
	first.Clear()
	second, err := repository.PrepareBlueprintScriptPublication(
		context.Background(), environmentID, revision, generationID, nil,
		desired, scriptBlueprintGenerations(desired),
	)
	if err != nil {
		t.Fatalf("PrepareBlueprintScriptPublication(replay) error = %v", err)
	}
	second.Clear()
}

func TestBlueprintScriptStageReplayRetainsNewLocatorMutations(t *testing.T) {
	base := newMemoryHierarchyStore()
	_, environmentID, _ := scriptBlueprintEmptyActiveSet(t, base)
	desired := scriptBlueprintRecords(t, environmentID, 1, "locator-replay")
	record := desired[0]
	locator, err := testscripts.EncodeScriptLocator(testscripts.LocatorRecord{
		ScriptID: record.Desired.ID, EnvironmentID: environmentID,
	})
	if err != nil {
		t.Fatalf("encodeScriptLocator() error = %v", err)
	}
	seed, err := base.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testscripts.ScriptLocatorKey(record.Desired.ID)},
		{Key: testscripts.ScriptEnvironmentLocatorKey(environmentID, record.Desired.ID)},
	}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testscripts.ScriptLocatorKey(record.Desired.ID), Value: locator},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testscripts.ScriptEnvironmentLocatorKey(environmentID, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
	})
	clear(locator)
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed replay locators = %#v, %v", seed, err)
	}
	store := &scriptBlueprintStageStore{memoryHierarchyStore: base}
	repository, err := newScriptRepository(store)
	if err != nil {
		t.Fatalf("newScriptRepository() error = %v", err)
	}
	publication, err := repository.PrepareBlueprintScriptPublication(
		context.Background(), environmentID, seed.Revision, scriptBlueprintGenerationID(909), nil,
		desired, scriptBlueprintGenerations(desired),
	)
	if err != nil {
		t.Fatalf("PrepareBlueprintScriptPublication() error = %v", err)
	}
	publication.Clear()
	want := map[string]bool{
		testscripts.ScriptLocatorKey(record.Desired.ID):                           false,
		testscripts.ScriptEnvironmentLocatorKey(environmentID, record.Desired.ID): false,
	}
	for _, mutation := range store.batches[0] {
		if _, expected := want[mutation.Key]; expected && mutation.Type == testkeyvalue.MutationPut {
			want[mutation.Key] = true
		}
	}
	for key, found := range want {
		if !found {
			t.Fatalf("replay batch is missing deterministic locator mutation %s", key)
		}
	}
}

func TestBlueprintScriptFlipRejectsActiveExecutionReferences(t *testing.T) {
	store := newMemoryHierarchyStore()
	repository, current := scriptBlueprintSeedActiveSet(t, store, 1)
	current[0].Record.ActiveReferences = 1
	_, err := repository.PrepareBlueprintScriptPublication(
		context.Background(), current[0].Record.EnvironmentID, current[0].ReadRevision,
		scriptBlueprintGenerationID(907), current, []testscripts.Record{current[0].Record}, nil,
	)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("PrepareBlueprintScriptPublication(active reference) error = %v, want state.conflict", err)
	}
}

func TestBlueprintScriptStageBatchesContainAtMost16Records(t *testing.T) {
	base := newMemoryHierarchyStore()
	_, environmentID, revision := scriptBlueprintEmptyActiveSet(t, base)
	store := &scriptBlueprintStageStore{memoryHierarchyStore: base}
	repository, err := newScriptRepository(store)
	if err != nil {
		t.Fatalf("newScriptRepository() error = %v", err)
	}
	desired := scriptBlueprintRecords(t, environmentID, 64, "bounded")
	publication, err := repository.PrepareBlueprintScriptPublication(
		context.Background(), environmentID, revision, scriptBlueprintGenerationID(905), nil,
		desired, scriptBlueprintGenerations(desired),
	)
	if err != nil {
		t.Fatalf("PrepareBlueprintScriptPublication() error = %v", err)
	}
	publication.Clear()
	if len(store.batches) != 10 {
		t.Fatalf("stage transaction count = %d, want 10", len(store.batches))
	}
	for index, batch := range store.batches {
		if store.operationCounts[index] > testkeyvalue.MaximumOperations {
			t.Fatalf(
				"batch %d transaction operations = %d, want <=%d",
				index,
				store.operationCounts[index], testkeyvalue.MaximumOperations,
			)
		}
		records := 0
		for _, mutation := range batch {
			if strings.Contains(mutation.Key, "/generations/") &&
				(strings.Contains(mutation.Key, "/scripts/") || strings.Contains(mutation.Key, "/bodies/")) {
				records++
			}
		}
		if records > 16 {
			t.Fatalf("batch %d Script/body records = %d, want <=16", index, records)
		}
	}
}

func TestBlueprintScriptStageShrinksForEncodedByteCeiling(t *testing.T) {
	base := newMemoryHierarchyStore()
	_, environmentID, revision := scriptBlueprintEmptyActiveSet(t, base)
	store := &scriptBlueprintStageStore{memoryHierarchyStore: base}
	repository, err := newScriptRepository(store)
	if err != nil {
		t.Fatalf("newScriptRepository() error = %v", err)
	}
	desired := scriptBlueprintRecords(t, environmentID, 7, "encoded")
	for index := range desired {
		desired[index].Desired.Body = strings.Repeat("\x01", 60_000)
	}
	publication, err := repository.PrepareBlueprintScriptPublication(
		context.Background(), environmentID, revision, scriptBlueprintGenerationID(908), nil,
		desired, scriptBlueprintGenerations(desired),
	)
	if err != nil {
		t.Fatalf("PrepareBlueprintScriptPublication() error = %v", err)
	}
	publication.Clear()
	if len(store.batches) != 4 {
		t.Fatalf("stage transaction count = %d, want 4", len(store.batches))
	}
	for index, encodedBytes := range store.encodedBytes {
		if encodedBytes > testkeyvalue.MaximumBytes {
			t.Fatalf(
				"batch %d encoded bytes = %d, want <=%d",
				index,
				encodedBytes, testkeyvalue.MaximumBytes,
			)
		}
	}
}

type scriptBlueprintStageStore struct {
	*memoryHierarchyStore
	transacts       int
	failAt          int
	batches         [][]testkeyvalue.Mutation
	operationCounts []int
	encodedBytes    []int
}

func (store *scriptBlueprintStageStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.transacts++
	store.batches = append(store.batches, testkeyvalue.CloneMutations(mutations))
	store.operationCounts = append(store.operationCounts, len(conditions)+len(mutations))
	store.encodedBytes = append(store.encodedBytes, scriptStageEncodedBytes(conditions, mutations))
	if store.failAt != 0 && store.transacts == store.failAt {
		return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal, "injected Script staging failure")
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

func scriptBlueprintEmptyActiveSet(t *testing.T, store *memoryHierarchyStore) (*ScriptRepository, string, int64) {
	t.Helper()
	at := time.Date(2026, 8, 30, 16, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	active := testscripts.SetGenerationRecord{EnvironmentID: environmentID, GenerationID: environmentID}
	value, err := testscripts.EncodeScriptSetGeneration(active)
	if err != nil {
		t.Fatalf("encodeScriptSetGeneration() error = %v", err)
	}
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testscripts.ScriptSetActiveKey(environmentID)}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testscripts.ScriptSetActiveKey(environmentID), Value: value,
		}},
	)
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed active Script set = %#v, %v", result, err)
	}
	repository, err := newScriptRepository(store)
	if err != nil {
		t.Fatalf("newScriptRepository() error = %v", err)
	}
	return repository, environmentID, result.Revision
}

func scriptBlueprintSeedActiveSet(
	t *testing.T,
	store *memoryHierarchyStore,
	count int,
) (*ScriptRepository, []testkeyvalue.Versioned[testscripts.Record]) {
	t.Helper()
	repository, environmentID, _ := scriptBlueprintEmptyActiveSet(t, store)
	records := scriptBlueprintRecords(t, environmentID, count, "existing")
	for start := 0; start < len(records); start += 8 {
		end := min(start+8, len(records))
		mutations := make([]testkeyvalue.Mutation, 0, (end-start)*6)
		for index := start; index < end; index++ {
			record := records[index]
			record.ScriptSetGeneration = environmentID
			records[index] = record
			primary, err := testscripts.EncodeRecord(record)
			if err != nil {
				t.Fatalf("encodeScriptRecord() error = %v", err)
			}
			body, err := testscripts.EncodeScriptBodyGeneration(scriptBlueprintGeneration(record))
			if err != nil {
				t.Fatalf("encodeScriptBodyGeneration() error = %v", err)
			}
			locator, err := testscripts.EncodeScriptLocator(
				testscripts.LocatorRecord{ScriptID: record.Desired.ID, EnvironmentID: environmentID},
			)
			if err != nil {
				t.Fatalf("encodeScriptLocator() error = %v", err)
			}
			mutations = append(
				mutations, testkeyvalue.Mutation{
					Type:  testkeyvalue.MutationPut,
					Key:   testscripts.ScriptSetScriptKey(environmentID, environmentID, record.Desired.ID),
					Value: primary,
				}, testkeyvalue.Mutation{
					Type:  testkeyvalue.MutationPut,
					Key:   testscripts.ScriptSetBodyGenerationKey(environmentID, environmentID, record.Desired.ID, 1),
					Value: body,
				}, testkeyvalue.Mutation{
					Type:  testkeyvalue.MutationPut,
					Key:   testscripts.ScriptSetOwnerKey(environmentID, environmentID, record.Desired.ID),
					Value: []byte(record.Desired.ID),
				}, testkeyvalue.Mutation{
					Type:  testkeyvalue.MutationPut,
					Key:   testscripts.ScriptSetSlugKey(environmentID, environmentID, record.Desired.Slug),
					Value: []byte(record.Desired.ID),
				}, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: testscripts.ScriptLocatorKey(record.Desired.ID), Value: locator}, testkeyvalue.Mutation{
					Type:  testkeyvalue.MutationPut,
					Key:   testscripts.ScriptEnvironmentLocatorKey(environmentID, record.Desired.ID),
					Value: []byte(record.Desired.ID),
				},
			)
		}
		result, err := store.Transact(context.Background(), nil, mutations)
		testkeyvalue.ClearMutationValues(mutations)
		if err != nil || !result.Succeeded {
			t.Fatalf("seed active Scripts = %#v, %v", result, err)
		}
	}
	page, err := repository.ListScripts(context.Background(), environmentID, testkeyvalue.PageRequest{Limit: 64})
	if err != nil || len(page.Items) != count {
		t.Fatalf("ListScripts(seed) = %#v, %v", page, err)
	}
	return repository, page.Items
}

func scriptBlueprintRecords(t *testing.T, environmentID string, count int, bodyPrefix string) []testscripts.Record {
	t.Helper()
	at := time.Date(2026, 8, 30, 16, 0, 0, 0, time.UTC)
	records := make([]testscripts.Record, count)
	for index := range records {
		record, err := testscripts.NewRecord(
			environmentID,
			ids.NewAt(ids.KindService, at, int64(index+100)),
			core.Script{
				ID: ids.NewAt(ids.KindScript, at, int64(index+200)), Slug: fmt.Sprintf("script-%02d", index),
				ServiceName: "api", Body: fmt.Sprintf("%s-%02d", bodyPrefix, index), When: core.ScriptManual,
			},
		)
		if err != nil {
			t.Fatalf("NewScriptRecord(%d) error = %v", index, err)
		}
		record.Origin = "blueprint"
		record.ReconciliationKey = fmt.Sprintf("key-%02d", index)
		records[index] = record
	}
	return records
}

func scriptBlueprintGenerations(records []testscripts.Record) []testscripts.BodyGenerationRecord {
	result := make([]testscripts.BodyGenerationRecord, len(records))
	for index := range records {
		result[index] = scriptBlueprintGeneration(records[index])
	}
	return result
}

func scriptBlueprintGeneration(record testscripts.Record) testscripts.BodyGenerationRecord {
	generation, err := testscripts.NewScriptBodyGeneration(record)
	if err != nil {
		panic(err)
	}
	return generation
}

func scriptBlueprintGenerationID(seed int64) string {
	return ids.NewAt(ids.KindTask, time.Date(2026, 8, 30, 16, 0, 0, 0, time.UTC), seed)
}
