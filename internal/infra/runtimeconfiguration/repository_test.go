package runtimeconfiguration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
)

var fixtureTime = time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC)

// Rationale: SVC-15/JOURNEY-02 recovery must retain the exact acknowledged
// byte identity, metadata, and source generations after the earlier Task is removed.
func TestSnapshotSurvivesEarlierTaskRemoval(t *testing.T) {
	store := newMemoryStore()
	repository := mustRepository(t, store)
	ctx := context.Background()
	taskKey := "/v1/records/tasks/" + fixtureID(ids.KindTask, 90)
	if result, err := store.Transact(ctx, []Condition{{Key: taskKey}}, []Mutation{
		{Type: MutationPut, Key: taskKey, Value: []byte("completed task")},
	}); err != nil || !result.Succeeded {
		t.Fatalf("seed Task = %#v, %v", result, err)
	}
	snapshot := fixtureSnapshot(1, fixtureRecords())
	want, err := validateAndSortSnapshot(snapshot)
	if err != nil {
		t.Fatalf("canonical fixture: %v", err)
	}
	reference, err := repository.Stage(ctx, snapshot)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if result, deleteErr := store.Transact(ctx, nil, []Mutation{{Type: MutationDelete, Key: taskKey}}); deleteErr != nil {
		t.Fatalf("remove earlier Task: %v", deleteErr)
	} else if !result.Succeeded {
		t.Fatal("remove earlier Task did not commit")
	}
	loaded, err := repository.Load(ctx, reference, store.currentRevision())
	if err != nil {
		t.Fatalf("Load after Task removal: %v", err)
	}
	if !reflect.DeepEqual(loaded, want) {
		t.Fatalf("loaded snapshot = %#v, want %#v", loaded, want)
	}
	loaded.Files[0].Source.BlueprintFile.Path = "mutated-by-caller"
	reloaded, err := repository.Load(ctx, reference, store.currentRevision())
	if err != nil || !reflect.DeepEqual(reloaded, want) {
		t.Fatalf("reload after caller mutation = %#v, %v, want %#v", reloaded, err, want)
	}
	exactReplay, err := repository.Stage(ctx, snapshot)
	if err != nil || exactReplay != reference {
		t.Fatalf("exact Stage replay = %#v, %v, want %#v", exactReplay, err, reference)
	}
}

// Rationale: SVC-15 recovery must fail closed when any retained root/member is
// missing or corrupt; a valid-looking caller reference cannot substitute for it.
func TestLoadRejectsMissingAndCorruptSources(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *memoryStore, canonicalSnapshot, Reference)
	}{
		{
			name: "missing member",
			mutate: func(t *testing.T, store *memoryStore, canonical canonicalSnapshot, _ Reference) {
				t.Helper()
				mustMutate(t, store, Mutation{Type: MutationDelete, Key: canonical.members[0].key})
			},
		},
		{
			name: "corrupt member digest",
			mutate: func(t *testing.T, store *memoryStore, canonical canonicalSnapshot, _ Reference) {
				t.Helper()
				mustMutate(t, store, Mutation{
					Type: MutationPut, Key: canonical.members[0].key, Value: []byte(`{"schema":1}`),
				})
			},
		},
		{
			name: "noncanonical root",
			mutate: func(t *testing.T, store *memoryStore, canonical canonicalSnapshot, _ Reference) {
				t.Helper()
				mustMutate(t, store, Mutation{
					Type: MutationPut, Key: publishedRootKey(canonical.snapshot.ID),
					Value: append(append([]byte(nil), canonical.rootValue...), '\n'),
				})
			},
		},
		{
			name: "mismatched reference",
			mutate: func(_ *testing.T, _ *memoryStore, _ canonicalSnapshot, reference Reference) {
				reference.SHA256 = strings.Repeat("f", 64)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newMemoryStore()
			repository := mustRepository(t, store)
			snapshot := fixtureSnapshot(1, fixtureRecords())
			canonical, canonicalErr := canonicalizeSnapshot(snapshot)
			if canonicalErr != nil {
				t.Fatalf("canonical fixture: %v", canonicalErr)
			}
			reference, stageErr := repository.Stage(context.Background(), snapshot)
			if stageErr != nil {
				t.Fatalf("Stage: %v", stageErr)
			}
			if test.name == "mismatched reference" {
				reference.SHA256 = strings.Repeat("f", 64)
			} else {
				test.mutate(t, store, canonical, reference)
			}
			if _, loadErr := repository.Load(
				context.Background(), reference, store.currentRevision(),
			); !hasKind(loadErr, errs.KindInternal) {
				t.Fatalf("Load error = %v, want internal failure", loadErr)
			}
		})
	}
}

// Rationale: partial staging must remain unobservable to JOURNEY-02 recovery,
// reserve the ID against changed bytes, and allow only exact replay to seal it.
func TestPartialStageIsInvisibleAndConflictingReplayIsRejected(t *testing.T) {
	store := newMemoryStore()
	store.failTransaction = 3
	repository := mustRepository(t, store)
	snapshot := fixtureSnapshot(1, fixtureRecords()[:2])
	canonical, err := canonicalizeSnapshot(snapshot)
	if err != nil {
		t.Fatalf("canonical fixture: %v", err)
	}
	if _, err = repository.Stage(context.Background(), snapshot); !hasKind(err, errs.KindStorageUnavailable) {
		t.Fatalf("partial Stage error = %v, want storage unavailable", err)
	}
	if _, err = repository.Load(context.Background(), canonical.reference, store.currentRevision()); err == nil {
		t.Fatal("Load observed a partially staged snapshot")
	}
	changed := fixtureSnapshot(1, fixtureRecords()[:2])
	changed.Files[0].SHA256 = strings.Repeat("e", 64)
	if _, err = repository.Stage(context.Background(), changed); !hasKind(err, errs.KindStateConflict) {
		t.Fatalf("changed replay error = %v, want state conflict", err)
	}
	reference, err := repository.Stage(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("exact replay Stage: %v", err)
	}
	if _, err = repository.Load(context.Background(), reference, store.currentRevision()); err != nil {
		t.Fatalf("Load sealed exact replay: %v", err)
	}
}

// Rationale: SVC-15 can retain the documented maximum without an unbounded
// request; the 513th file is rejected before any Store operation.
func TestMaximumFilesUsesBoundedStoreRequests(t *testing.T) {
	records := make([]taskmaterialization.Record, MaximumFiles)
	for index := range records {
		records[index] = removalRecord(fmt.Sprintf("config/file-%03d", index))
	}
	store := newMemoryStore()
	repository := mustRepository(t, store)
	reference, err := repository.Stage(context.Background(), fixtureSnapshot(1, records))
	if err != nil {
		t.Fatalf("Stage maximum snapshot: %v", err)
	}
	loaded, err := repository.Load(context.Background(), reference, store.currentRevision())
	if err != nil || len(loaded.Files) != MaximumFiles {
		t.Fatalf("Load maximum snapshot = %d files, %v", len(loaded.Files), err)
	}
	maximumKeys, maximumOperations, maximumValue, calls := store.audit()
	if maximumKeys > readBatchSize || maximumOperations > 4 || maximumValue > maximumEncodedRecordBytes ||
		calls > MaximumFiles+2 {
		t.Fatalf(
			"Store bounds = keys:%d operations:%d value:%d txns:%d",
			maximumKeys,
			maximumOperations,
			maximumValue,
			calls,
		)
	}
	store = newMemoryStore()
	repository = mustRepository(t, store)
	tooMany := append(records, removalRecord("config/overflow"))
	if _, err = repository.Stage(context.Background(), fixtureSnapshot(2, tooMany)); err == nil {
		t.Fatal("Stage accepted 513 files")
	}
	if _, _, _, calls = store.audit(); calls != 0 || store.getCalls() != 0 {
		t.Fatalf("oversized Stage touched Store: %d reads, %d transactions", store.getCalls(), calls)
	}
}

// Rationale: Entry-only success must replace exactly the changed destination,
// preserve other immutable sources, and retain explicit absence metadata.
func TestMergePreservesUnchangedSourcesAndExplicitRemoval(t *testing.T) {
	previous := fixtureSnapshot(1, fixtureRecords()[:2])
	removed := removalRecord(previous.Files[0].Destination)
	merged, err := Merge(previous, []taskmaterialization.Record{removed}, fixtureID(ids.KindConfig, 70), 2)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if len(merged.Files) != 2 || merged.Files[0].Destination >= merged.Files[1].Destination {
		t.Fatalf("merged files are not uniquely sorted: %#v", merged.Files)
	}
	byDestination := make(map[string]taskmaterialization.Record, len(merged.Files))
	for _, record := range merged.Files {
		byDestination[record.Destination] = record
	}
	if got := byDestination[removed.Destination]; got.Source.Kind != taskmaterialization.SourceRemoval {
		t.Fatalf("changed file = %#v, want explicit removal", got)
	}
	unchanged := previous.Files[1]
	if !reflect.DeepEqual(byDestination[unchanged.Destination], unchanged) {
		t.Fatalf("unchanged file = %#v, want %#v", byDestination[unchanged.Destination], unchanged)
	}
	if _, err := Merge(
		previous, []taskmaterialization.Record{removed, removed}, fixtureID(ids.KindConfig, 71), 2,
	); err == nil {
		t.Fatal("Merge accepted duplicate changed destinations")
	}
	foreign := removed
	foreign.EnvironmentID = fixtureID(ids.KindEnvironment, 99)
	if _, err := Merge(previous, []taskmaterialization.Record{foreign}, fixtureID(ids.KindConfig, 72), 2); err == nil {
		t.Fatal("Merge accepted a foreign Environment")
	}
}

// Rationale: runtime receipts and the current pointer must share one strict,
// canonical Reference representation so malformed durable authority fails closed.
func TestReferenceCodecIsCanonicalAndStrict(t *testing.T) {
	reference := Reference{
		ID: fixtureID(ids.KindConfig, 1), EnvironmentID: fixtureID(ids.KindEnvironment, 2),
		Generation: 3, SHA256: strings.Repeat("a", 64),
	}
	encoded, err := EncodeReference(reference)
	if err != nil {
		t.Fatalf("EncodeReference: %v", err)
	}
	decoded, err := DecodeReference(encoded)
	if err != nil || decoded != reference {
		t.Fatalf("DecodeReference = %#v, %v, want %#v", decoded, err, reference)
	}
	malformed := [][]byte{
		append(append([]byte(nil), encoded...), '\n'),
		bytes.Replace(encoded, []byte(`{"id"`), []byte(`{"unknown":1,"id"`), 1),
		bytes.Replace(encoded, []byte(`{"id"`), []byte(`{"id":"`+reference.ID+`","id"`), 1),
	}
	for _, value := range malformed {
		if _, err := DecodeReference(value); err == nil {
			t.Fatalf("DecodeReference accepted %q", value)
		}
	}
}

func fixtureSnapshot(seed int64, records []taskmaterialization.Record) Snapshot {
	return Snapshot{
		ID: fixtureID(ids.KindConfig, seed), EnvironmentID: fixtureID(ids.KindEnvironment, 10),
		Generation: 1, Files: taskmaterialization.Clone(records),
	}
}

func fixtureRecords() []taskmaterialization.Record {
	environmentID := fixtureID(ids.KindEnvironment, 10)
	blueprint := fileRecord("config/app.yaml", taskmaterialization.OutputPlainFile, 0o444)
	blueprint.Source = taskmaterialization.Source{
		Kind: taskmaterialization.SourceBlueprintFile,
		BlueprintFile: &taskmaterialization.BlueprintFileValueReference{
			RevisionID: fixtureID(ids.KindTask, 20), Path: "config/app.yaml",
		},
	}
	secret := fileRecord("secrets/token", taskmaterialization.OutputSecretFile, 0o600)
	secret.Source = taskmaterialization.Source{
		Kind: taskmaterialization.SourceEntryValue,
		EntryValue: &taskmaterialization.EntryValueReference{
			EntryID: fixtureID(ids.KindEnvEntry, 30), ValueGenerationID: fixtureID(ids.KindConfig, 31),
			Storage: taskmaterialization.EntryValueStorageSecret,
		},
	}
	generated := fileRecord(
		"secrets/.env."+environmentID, taskmaterialization.OutputGeneratedEnvironment, 0o600,
	)
	generated.UID, generated.GID = 0, 0
	generated.EnvironmentID = environmentID
	generated.Source = taskmaterialization.Source{
		Kind: taskmaterialization.SourceGeneratedEnvironment,
		GeneratedEnvironment: &taskmaterialization.GeneratedEnvironmentValueReference{
			FormatVersion: 1,
			Values: []taskmaterialization.GeneratedEnvironmentEntryReference{
				{
					Name: "API_TOKEN", Secret: &taskmaterialization.SecretValueReference{
						SecretID: fixtureID(ids.KindSecret, 40), Revision: 7,
						CiphertextSHA256: strings.Repeat("b", 64),
					},
				},
				{
					Name: "PORT", Value: taskmaterialization.EntryValueReference{
						EntryID:           fixtureID(ids.KindEnvEntry, 41),
						ValueGenerationID: fixtureID(ids.KindConfig, 42),
						Storage:           taskmaterialization.EntryValueStoragePlain,
					},
				},
			},
		},
	}
	return []taskmaterialization.Record{secret, generated, blueprint}
}

func fileRecord(destination string, output taskmaterialization.OutputKind, mode uint32) taskmaterialization.Record {
	return taskmaterialization.Record{
		StepID: fixtureID(ids.KindStep, 11), MaterializationID: fixtureID(ids.KindConfig, 12),
		EnvironmentID: fixtureID(ids.KindEnvironment, 10), Destination: destination,
		OutputKind: output, UID: 1000, GID: 1001, Mode: mode,
		Length: 14, SHA256: strings.Repeat("a", 64),
	}
}

func removalRecord(destination string) taskmaterialization.Record {
	record := fileRecord(
		destination,
		taskmaterialization.OutputRemovePlainFile,
		uint32(entrymaterialization.ModeReadOnly),
	)
	record.Length = 0
	record.SHA256 = digest(nil)
	record.Source = taskmaterialization.Source{Kind: taskmaterialization.SourceRemoval}
	return record
}

func fixtureID(kind ids.Kind, seed int64) string { return ids.NewAt(kind, fixtureTime, seed) }

func mustRepository(t *testing.T, store Store) *Repository {
	t.Helper()
	repository, err := NewRepository(store)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	return repository
}

func mustMutate(t *testing.T, store Store, mutation Mutation) {
	t.Helper()
	result, err := store.Transact(context.Background(), nil, []Mutation{mutation})
	if err != nil || !result.Succeeded {
		t.Fatalf("mutate Store = %#v, %v", result, err)
	}
}

func hasKind(err error, want errs.Kind) bool {
	got, ok := errs.KindOf(err)
	return ok && got == want
}

type memoryVersion struct {
	revision int64
	value    []byte
	deleted  bool
}

type memoryStore struct {
	mu                  sync.Mutex
	revision            int64
	history             map[string][]memoryVersion
	failTransaction     int
	transactionAttempts int
	transactionCalls    int
	readCalls           int
	maximumReadKeys     int
	maximumOperations   int
	maximumValue        int
}

func newMemoryStore() *memoryStore {
	return &memoryStore{revision: 1, history: make(map[string][]memoryVersion)}
}

func (store *memoryStore) GetMany(
	ctx context.Context,
	keys []string,
	revision int64,
) (*GetManyResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.readCalls++
	store.maximumReadKeys = max(store.maximumReadKeys, len(keys))
	readRevision := revision
	if readRevision == 0 {
		readRevision = store.revision
	}
	if readRevision < 0 || readRevision > store.revision {
		return nil, errors.New("invalid memory revision")
	}
	result := &GetManyResult{Values: make([]*KeyValue, len(keys)), ReadRevision: readRevision}
	for index, key := range keys {
		versions := store.history[key]
		for position := len(versions) - 1; position >= 0; position-- {
			version := versions[position]
			if version.revision > readRevision {
				continue
			}
			if !version.deleted {
				result.Values[index] = &KeyValue{
					Key: key, Value: append([]byte(nil), version.value...), ModRevision: version.revision,
				}
			}
			break
		}
	}
	return result, nil
}

func (store *memoryStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TxnResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return TxnResult{}, err
	}
	store.transactionAttempts++
	if store.failTransaction == store.transactionAttempts {
		return TxnResult{}, errors.New("injected transaction failure")
	}
	store.transactionCalls++
	store.maximumOperations = max(store.maximumOperations, len(conditions)+len(mutations))
	for _, mutation := range mutations {
		store.maximumValue = max(store.maximumValue, len(mutation.Value))
	}
	for _, condition := range conditions {
		current := store.current(condition.Key)
		if condition.ModRevision == 0 && current != nil || condition.ModRevision > 0 &&
			(current == nil || current.revision != condition.ModRevision) {
			return TxnResult{Succeeded: false, Revision: store.revision}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		switch mutation.Type {
		case MutationPut:
			store.history[mutation.Key] = append(store.history[mutation.Key], memoryVersion{
				revision: store.revision, value: append([]byte(nil), mutation.Value...),
			})
		case MutationDelete:
			store.history[mutation.Key] = append(store.history[mutation.Key], memoryVersion{
				revision: store.revision, deleted: true,
			})
		default:
			return TxnResult{}, errors.New("unknown mutation")
		}
	}
	return TxnResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *memoryStore) current(key string) *memoryVersion {
	versions := store.history[key]
	if len(versions) == 0 || versions[len(versions)-1].deleted {
		return nil
	}
	value := versions[len(versions)-1]
	return &value
}

func (store *memoryStore) currentRevision() int64 {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.revision
}

func (store *memoryStore) audit() (int, int, int, int) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.maximumReadKeys, store.maximumOperations, store.maximumValue, store.transactionCalls
}

func (store *memoryStore) getCalls() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.readCalls
}
