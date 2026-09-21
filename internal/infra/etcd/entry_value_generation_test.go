package etcd

import (
	context "context"
	sha256 "crypto/sha256"
	hex "encoding/hex"
	errors "errors"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testentryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	testing "testing"
)

// Rationale: a Task retry must resolve the same immutable plain bytes even
// after caller buffers are reused or a duplicate create is replayed.
func TestPlainEntryValueGenerationIsImmutableAndOwned(t *testing.T) {
	repository, err := testentryvalues.New(&releaseLogMemoryStore{newMemoryHierarchyStore()})
	if err != nil {
		t.Fatalf("newEntryValueGenerationRepository() error = %v", err)
	}
	record := testPlainEntryValueGeneration()
	if err := repository.CreatePlain(context.Background(), record); err != nil {
		t.Fatalf("CreatePlain() error = %v", err)
	}
	record.Content[0] = 'X'
	stored, found, err := repository.GetPlain(context.Background(), record.EntryID, record.GenerationID)
	if err != nil || !found || string(stored.Content) != "plain-entry-value" {
		t.Fatalf("GetPlain() = %#v, %v, %v", stored, found, err)
	}
	replay := testPlainEntryValueGeneration()
	if err := repository.CreatePlain(context.Background(), replay); err != nil {
		t.Fatalf("CreatePlain(replay) error = %v", err)
	}
	replay.Content = []byte("different")
	digest := sha256.Sum256(replay.Content)
	replay.PlaintextSHA256 = hex.EncodeToString(digest[:])
	if err := repository.CreatePlain(context.Background(), replay); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("CreatePlain(conflict) error = %v, want state.conflict", err)
	}
	clear(stored.Content)
}

// Rationale: secret generations must persist only validated envelope bytes
// under the non-listable secret root and reject generation-id reuse.
func TestSecretEntryValueGenerationIsImmutableCiphertext(t *testing.T) {
	store := newMemoryHierarchyStore()
	repository, err := testentryvalues.New(&releaseLogMemoryStore{store})
	if err != nil {
		t.Fatalf("newEntryValueGenerationRepository() error = %v", err)
	}
	record := testSecretEntryValueGeneration()
	if err := repository.CreateSecret(context.Background(), record); err != nil {
		t.Fatalf("CreateSecret() error = %v", err)
	}
	stored, found, err := repository.GetSecret(context.Background(), record.EntryID, record.GenerationID)
	if err != nil || !found || string(stored.Ciphertext) != "age-ciphertext" {
		t.Fatalf("GetSecret() = %#v, %v, %v", stored, found, err)
	}
	if store.valueAt(testentryvalues.SecretKey(record.EntryID, record.GenerationID), store.revision) == nil ||
		store.valueAt(testentryvalues.PlainKey(record.EntryID, record.GenerationID), store.revision) != nil {
		t.Fatal("secret Entry generation used the wrong durable keyspace")
	}
	record.Ciphertext = []byte("other-ciphertext")
	digest := sha256.Sum256(record.Ciphertext)
	record.CiphertextSHA256 = hex.EncodeToString(digest[:])
	if err := repository.CreateSecret(context.Background(), record); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("CreateSecret(conflict) error = %v, want state.conflict", err)
	}
	clear(stored.Ciphertext)
}

func testPlainEntryValueGeneration() testentryvalues.PlainGeneration {
	content := []byte("plain-entry-value")
	digest := sha256.Sum256(content)
	now := taskJournalTime()
	return testentryvalues.PlainGeneration{
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 40),
		EntryID:       ids.NewAt(ids.KindEnvEntry, now, 41),
		GenerationID:  ids.NewAt(ids.KindConfig, now, 42),
		Content:       content, PlaintextSHA256: hex.EncodeToString(digest[:]), CreatedAt: now,
	}
}

func testSecretEntryValueGeneration() testentryvalues.SecretGeneration {
	ciphertext := []byte("age-ciphertext")
	digest := sha256.Sum256(ciphertext)
	now := taskJournalTime()
	return testentryvalues.SecretGeneration{
		EnvironmentID:   ids.NewAt(ids.KindEnvironment, now, 40),
		EntryID:         ids.NewAt(ids.KindEnvEntry, now, 41),
		GenerationID:    ids.NewAt(ids.KindConfig, now, 42),
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextSHA256: hex.EncodeToString(digest[:]), Ciphertext: ciphertext, CreatedAt: now,
	}
}
