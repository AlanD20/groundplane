package materializationcontent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	contentEnvironmentID     = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	contentStepID            = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	contentMaterializationID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	contentRevisionID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	contentComponentID       = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	contentGeneration        = uint64(9)
)

// Rationale: ADR0079 SVC-15/JOURNEY-02 must load exact bytes independently
// of both the producing Task lifetime and any later Component renderer.
func TestRepositoryRetainsExactLargeContentAfterTaskRemoval(t *testing.T) {
	store := newMemoryStore()
	repository := mustRepository(t, store)
	content := bytes.Repeat([]byte("stable-render\n"), int(entrymaterialization.MaximumContentBytes)/14)
	content = append(content, bytes.Repeat([]byte{'x'}, int(entrymaterialization.MaximumContentBytes)-len(content))...)
	record := componentRecord(content)
	ctx := context.Background()
	taskKey := "/v1/records/tasks/" + contentRevisionID
	if result, err := store.Transact(ctx, []Condition{{Key: taskKey}}, []Mutation{
		{Type: MutationPut, Key: taskKey, Value: []byte("old Task")},
	}); err != nil || !result.Succeeded {
		t.Fatalf("seed Task = %#v, %v", result, err)
	}
	if err := repository.Stage(ctx, record, contentGeneration, content); err != nil {
		t.Fatalf("Stage = %v", err)
	}
	if result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationDelete, Key: taskKey}}); err != nil ||
		!result.Succeeded {
		t.Fatalf("remove Task = %#v, %v", result, err)
	}
	loaded, err := repository.Load(ctx, record, contentGeneration)
	if err != nil || !bytes.Equal(loaded, content) {
		t.Fatalf("Load after Task removal = %d bytes/%v, want %d exact bytes", len(loaded), err, len(content))
	}
	clear(loaded)
	if err := repository.Stage(ctx, record, contentGeneration, content); err != nil {
		t.Fatalf("exact replay Stage = %v", err)
	}
	maxKeys, maxOperations, maxValueBytes := store.audit()
	if maxKeys > maximumChunks(record.Length) || maxOperations > 4 || maxValueBytes > maximumEncodedRecordBytes {
		t.Fatalf("Store bounds = keys:%d operations:%d value:%d", maxKeys, maxOperations, maxValueBytes)
	}
}

// Rationale: the immutable MaterializationID reservation must reject changed
// bytes, while an unpublished partial upload must never become loadable.
func TestRepositoryRejectsChangedReplayAndPartialPublication(t *testing.T) {
	content := []byte("original rendered bytes\n")
	record := componentRecord(content)
	store := newMemoryStore()
	repository := mustRepository(t, store)
	store.failSeal = true
	if err := repository.Stage(context.Background(), record, contentGeneration, content); !hasKind(
		err,
		errs.KindStorageUnavailable,
	) {
		t.Fatalf("partial Stage error = %v, want storage unavailable", err)
	}
	if _, err := repository.Load(context.Background(), record, contentGeneration); !hasKind(err, errs.KindInternal) {
		t.Fatalf("Load partial content error = %v, want internal", err)
	}
	store.failSeal = false
	if err := repository.Stage(context.Background(), record, contentGeneration, content); err != nil {
		t.Fatalf("resume exact Stage = %v", err)
	}
	changed := []byte("new renderer output\n")
	changedRecord := record
	changedDigest := sha256.Sum256(changed)
	changedRecord.Length = uint64(len(changed))
	changedRecord.SHA256 = hex.EncodeToString(changedDigest[:])
	if err := repository.Stage(
		context.Background(), changedRecord, contentGeneration, changed,
	); !hasKind(err, errs.KindStateConflict) {
		t.Fatalf("changed replay Stage error = %v, want state conflict", err)
	}
}

// Rationale: retained content is usable only when the canonical root, every
// chunk digest, and the caller's complete typed identity agree.
func TestRepositoryRejectsMissingCorruptAndMismatchedContent(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*memoryStore, *taskmaterialization.Record)
	}{
		{
			name: "missing chunk",
			mutate: func(store *memoryStore, record *taskmaterialization.Record) {
				store.remove(chunkKey(record.EnvironmentID, record.MaterializationID, 0))
			},
		},
		{
			name: "corrupt chunk",
			mutate: func(store *memoryStore, record *taskmaterialization.Record) {
				store.corrupt(chunkKey(record.EnvironmentID, record.MaterializationID, 0), []byte(`{"schema":1}`))
			},
		},
		{
			name: "noncanonical root",
			mutate: func(store *memoryStore, record *taskmaterialization.Record) {
				key := publishedRootKey(record.EnvironmentID, record.MaterializationID)
				store.appendValue(key, '\n')
			},
		},
		{
			name: "mismatched Component identity",
			mutate: func(_ *memoryStore, record *taskmaterialization.Record) {
				record.Source.ComponentFile.ComponentID = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			content := []byte("retained bytes\n")
			record := componentRecord(content)
			store := newMemoryStore()
			repository := mustRepository(t, store)
			if err := repository.Stage(context.Background(), record, contentGeneration, content); err != nil {
				t.Fatalf("Stage = %v", err)
			}
			test.mutate(store, &record)
			if loaded, err := repository.Load(
				context.Background(), record, contentGeneration,
			); !hasKind(err, errs.KindInternal) {
				clear(loaded)
				t.Fatalf("Load error = %v, want internal", err)
			}
		})
	}
}

// Rationale: no Entry Secret or generated-environment plaintext may cross the
// non-secret Component/plain retention seam, even when its metadata is valid.
func TestRepositoryRejectsSecretLikeSourcesBeforeStoreAccess(t *testing.T) {
	content := []byte("must not persist")
	tests := []taskmaterialization.Record{
		secretEntryRecord(content),
		generatedEnvironmentRecord(content),
	}
	for _, record := range tests {
		store := newMemoryStore()
		repository := mustRepository(t, store)
		if err := repository.Stage(
			context.Background(), record, contentGeneration, content,
		); !hasKind(err, errs.KindValidationFailed) {
			t.Fatalf("Stage(%s) error = %v, want validation", record.Source.Kind, err)
		}
		if gets, transactions := store.calls(); gets != 0 || transactions != 0 {
			t.Fatalf("Stage(%s) touched Store: %d reads/%d transactions", record.Source.Kind, gets, transactions)
		}
	}
}

func componentRecord(content []byte) taskmaterialization.Record {
	digest := sha256.Sum256(content)
	return taskmaterialization.Record{
		StepID: contentStepID, MaterializationID: contentMaterializationID,
		EnvironmentID: contentEnvironmentID, Destination: "components/proxy/config.json",
		OutputKind: taskmaterialization.OutputPlainFile, Mode: uint32(entrymaterialization.ModeReadOnly),
		Length: uint64(len(content)), SHA256: hex.EncodeToString(digest[:]),
		Source: taskmaterialization.Source{
			Kind: taskmaterialization.SourceComponentFile,
			ComponentFile: &taskmaterialization.ComponentFileValueReference{
				RevisionID: contentRevisionID, ComponentID: contentComponentID, Path: "components/proxy/config.json",
			},
		},
	}
}

func secretEntryRecord(content []byte) taskmaterialization.Record {
	record := componentRecord(content)
	record.Destination = "secrets/token"
	record.OutputKind = taskmaterialization.OutputSecretFile
	record.Mode = uint32(entrymaterialization.ModePrivate)
	record.Source = taskmaterialization.Source{
		Kind: taskmaterialization.SourceEntryValue,
		EntryValue: &taskmaterialization.EntryValueReference{
			EntryID: "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV", ValueGenerationID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			Storage: taskmaterialization.EntryValueStorageSecret,
		},
	}
	return record
}

func generatedEnvironmentRecord(content []byte) taskmaterialization.Record {
	record := componentRecord(content)
	record.Destination = "secrets/.env." + contentEnvironmentID + ".api"
	record.ServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	record.ServiceName = "api"
	record.OutputKind = taskmaterialization.OutputGeneratedEnvironment
	record.Mode = uint32(entrymaterialization.ModePrivate)
	record.Source = taskmaterialization.Source{
		Kind: taskmaterialization.SourceGeneratedEnvironment,
		GeneratedEnvironment: &taskmaterialization.GeneratedEnvironmentValueReference{
			FormatVersion: 1,
			Values: []taskmaterialization.GeneratedEnvironmentEntryReference{{
				Name: "TOKEN", Secret: &taskmaterialization.SecretValueReference{
					SecretID: "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV", Revision: 1,
					CiphertextSHA256: strings.Repeat("a", sha256.Size*2),
				},
			}},
		},
	}
	return record
}

func mustRepository(t *testing.T, store Store) *Repository {
	t.Helper()
	repository, err := newRepository(store)
	if err != nil {
		t.Fatalf("NewRepository = %v", err)
	}
	return repository
}

func hasKind(err error, kind errs.Kind) bool { return errors.Is(err, errs.New(kind, "")) }

type memoryStore struct {
	mu            sync.Mutex
	values        map[string]*KeyValue
	revision      int64
	gets          int
	transactions  int
	maxKeys       int
	maxOperations int
	maxValueBytes int
	failSeal      bool
}

func newMemoryStore() *memoryStore { return &memoryStore{values: map[string]*KeyValue{}, revision: 1} }

func (store *memoryStore) GetMany(
	_ context.Context,
	keys []string,
	revision int64,
) (*GetManyResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.gets++
	store.maxKeys = max(store.maxKeys, len(keys))
	readRevision := store.revision
	if revision > 0 {
		readRevision = revision
	}
	result := &GetManyResult{Values: make([]*KeyValue, len(keys)), ReadRevision: readRevision}
	for index, key := range keys {
		if value := store.values[key]; value != nil && value.ModRevision <= readRevision {
			copyValue := *value
			copyValue.Value = append([]byte(nil), value.Value...)
			result.Values[index] = &copyValue
		}
	}
	return result, nil
}

func (store *memoryStore) Transact(
	_ context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.transactions++
	store.maxOperations = max(store.maxOperations, len(conditions)+len(mutations))
	valueBytes := 0
	seal := false
	for _, mutation := range mutations {
		valueBytes += len(mutation.Key) + len(mutation.Value)
		seal = seal || (mutation.Type == MutationPut && strings.HasSuffix(mutation.Key, "/root") &&
			strings.HasPrefix(mutation.Key, recordsPrefix))
	}
	store.maxValueBytes = max(store.maxValueBytes, valueBytes)
	if store.failSeal && seal {
		return TransactionResult{}, errors.New("injected seal failure")
	}
	for _, condition := range conditions {
		value := store.values[condition.Key]
		if (value == nil && condition.ModRevision != 0) ||
			(value != nil && value.ModRevision != condition.ModRevision) {
			return TransactionResult{Revision: store.revision}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		switch mutation.Type {
		case MutationPut:
			store.values[mutation.Key] = &KeyValue{
				Key: mutation.Key, Value: append([]byte(nil), mutation.Value...), ModRevision: store.revision,
			}
		case MutationDelete:
			delete(store.values, mutation.Key)
		}
	}
	return TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *memoryStore) remove(key string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.revision++
	delete(store.values, key)
}

func (store *memoryStore) corrupt(key string, value []byte) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.revision++
	store.values[key] = &KeyValue{Key: key, Value: append([]byte(nil), value...), ModRevision: store.revision}
}

func (store *memoryStore) appendValue(key string, suffix byte) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.revision++
	value := append([]byte(nil), store.values[key].Value...)
	value = append(value, suffix)
	store.values[key] = &KeyValue{Key: key, Value: value, ModRevision: store.revision}
}

func (store *memoryStore) audit() (int, int, int) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.maxKeys, store.maxOperations, store.maxValueBytes
}

func (store *memoryStore) calls() (int, int) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.gets, store.transactions
}
