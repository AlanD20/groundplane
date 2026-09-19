package entrygeneration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a non-secret literal must become the exact immutable plain generation accepted by Entry persistence.
func TestEntryGenerationServiceGeneratesPlainLiteral(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 17, 0, 0, 0, time.UTC)
	service := newEntryGenerationTestService(t, nil, &entryGenerationTestFactResolver{})
	entry := testEnvEntry(now, 1)
	entry.Source = core.EntrySource{Kind: core.SourceLiteral, Literal: "public-value"}
	generation, err := service.Generate(
		context.Background(),
		ids.NewAt(ids.KindProject, now, 2),
		ids.NewAt(ids.KindEnvironment, now, 3),
		entry,
		ids.NewAt(ids.KindConfig, now, 4),
		now,
	)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	defer ClearEntryValueGeneration(&generation)
	digest := sha256.Sum256([]byte("public-value"))
	if generation.Plain == nil || generation.Secret != nil || string(generation.Plain.Content) != "public-value" ||
		generation.Plain.PlaintextSHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("Generate() = %#v", generation)
	}
}

// Rationale: secret literal plaintext is a transient mutation input and must
// become only an encrypted generation even though durable metadata is redacted.
func TestEntryGenerationServiceGeneratesSecretLiteral(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 17, 30, 0, 0, time.UTC)
	service := newEntryGenerationTestService(t, nil, &entryGenerationTestFactResolver{})
	entry := testEnvEntry(now, 11)
	entry.Secret = true
	entry.Source = core.EntrySource{Kind: core.SourceLiteral, Literal: "private-value"}
	generation, err := service.Generate(
		context.Background(),
		ids.NewAt(ids.KindProject, now, 12),
		ids.NewAt(ids.KindEnvironment, now, 13),
		entry,
		ids.NewAt(ids.KindConfig, now, 14),
		now,
	)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	defer ClearEntryValueGeneration(&generation)
	if generation.Plain != nil || generation.Secret == nil {
		t.Fatalf("Generate() = %#v", generation)
	}
	if got := openEntryGenerationTestValue(t, service.protector, *generation.Secret); got != "private-value" {
		t.Fatalf("generated secret plaintext = %q", got)
	}
}

// Rationale: a project-first reusable Secret must be opened only in memory and resealed into an Entry-owned generation.
func TestEntryGenerationServiceGeneratesReusableSecret(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	crypt := entryGenerationTestCrypt{}
	protector, err := secretvalue.NewProtector(crypt, crypt)
	if err != nil {
		t.Fatalf("NewProtector() error = %v", err)
	}
	projectID := ids.NewAt(ids.KindProject, now, 1)
	secretID := ids.NewAt(ids.KindSecret, now, 2)
	stored := sealEntryGenerationTestSecret(t, protector, secretID, []byte("database-password"))
	secrets := &entryGenerationTestSecretRepository{
		record: etcd.Versioned[etcd.SecretRecord]{
			Record: etcd.SecretRecord{Secret: core.Secret{
				ID: secretID, Scope: core.SecretScopeProject, ProjectID: projectID,
				Key: "DB_PASSWORD", Kind: core.SecretKindEnvVar, Ref: "secrets/.env." + projectID, UpdatedAt: now,
			}},
			Revision:     1,
			ReadRevision: 1,
		},
		value: stored,
	}
	service, err := NewEntryGenerationService(secrets, &entryGenerationTestFactResolver{}, protector)
	if err != nil {
		t.Fatalf("NewEntryGenerationService() error = %v", err)
	}
	entry := testEnvEntry(now, 3)
	entry.Secret = true
	entry.Source = core.EntrySource{Kind: core.SourceSecretRef, SecretRef: "DB_PASSWORD"}
	generation, err := service.Generate(
		context.Background(),
		projectID,
		ids.NewAt(ids.KindEnvironment, now, 4),
		entry,
		ids.NewAt(ids.KindConfig, now, 5),
		now,
	)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	defer ClearEntryValueGeneration(&generation)
	if generation.Secret == nil || generation.Plain != nil || secrets.resolves != 1 || secrets.reads != 1 {
		t.Fatalf("Generate() = %#v, resolves = %d, reads = %d", generation, secrets.resolves, secrets.reads)
	}
	if got := openEntryGenerationTestValue(t, protector, *generation.Secret); got != "database-password" {
		t.Fatalf("generated secret plaintext = %q", got)
	}
}

// Rationale: a fact source must carry the Entry's secret-destination decision into live Attach resolution.
func TestEntryGenerationServiceEnforcesFactSecrecy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 19, 0, 0, 0, time.UTC)
	facts := &entryGenerationTestFactResolver{secret: true, value: []byte("secret-url")}
	service := newEntryGenerationTestService(t, nil, facts)
	entry := testEnvEntry(now, 1)
	entry.Source = core.EntrySource{
		Kind: core.SourceFact,
		Fact: &core.FactRef{Attach: "api-db", Key: "pg16_URL"},
	}
	_, err := service.Generate(
		context.Background(),
		ids.NewAt(ids.KindProject, now, 2),
		ids.NewAt(ids.KindEnvironment, now, 3),
		entry,
		ids.NewAt(ids.KindConfig, now, 4),
		now,
	)
	kind, _ := errs.KindOf(err)
	if kind != errs.KindValidationFailed || facts.calls != 1 || facts.lastDestinationSecret {
		t.Fatalf("Generate() error = %v, resolver = %#v", err, facts)
	}
}

type entryGenerationTestSecretRepository struct {
	record   etcd.Versioned[etcd.SecretRecord]
	value    etcd.SecretEncryptedValue
	resolves int
	reads    int
}

func (repository *entryGenerationTestSecretRepository) ResolveSecret(
	_ context.Context,
	projectID string,
	_ string,
) (etcd.Versioned[etcd.SecretRecord], error) {
	repository.resolves++
	if repository.record.Record.Secret.ProjectID != projectID {
		return etcd.Versioned[etcd.SecretRecord]{}, errs.New(errs.KindSecretNotFound, "Secret was not found")
	}
	return repository.record, nil
}

func (repository *entryGenerationTestSecretRepository) GetSecretValue(
	_ context.Context,
	_ etcd.Versioned[etcd.SecretRecord],
) (etcd.SecretEncryptedValue, error) {
	repository.reads++
	value := repository.value
	value.Ciphertext = append([]byte(nil), repository.value.Ciphertext...)
	return value, nil
}

type entryGenerationTestFactResolver struct {
	secret                bool
	value                 []byte
	calls                 int
	lastDestinationSecret bool
}

func (resolver *entryGenerationTestFactResolver) ResolveFact(
	_ context.Context,
	_ string,
	_ core.FactRef,
	destinationSecret bool,
	consume secretvalue.PlaintextConsumer,
) error {
	resolver.calls++
	resolver.lastDestinationSecret = destinationSecret
	if resolver.secret && !destinationSecret {
		return errs.New(errs.KindValidationFailed, "Secret fact requires a secret destination")
	}
	value := append([]byte(nil), resolver.value...)
	defer clear(value)
	return consume(value)
}

type entryGenerationTestCrypt struct{}

func (entryGenerationTestCrypt) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	return append([]byte(nil), plaintext...), nil
}

func (entryGenerationTestCrypt) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	return append([]byte(nil), ciphertext...), nil
}

func newEntryGenerationTestService(
	t *testing.T,
	secrets EntrySecretRepository,
	facts EntryFactResolver,
) *EntryGenerationService {
	t.Helper()
	protector, err := secretvalue.NewProtector(entryGenerationTestCrypt{}, entryGenerationTestCrypt{})
	if err != nil {
		t.Fatalf("NewProtector() error = %v", err)
	}
	if secrets == nil {
		secrets = &entryGenerationTestSecretRepository{}
	}
	service, err := NewEntryGenerationService(secrets, facts, protector)
	if err != nil {
		t.Fatalf("NewEntryGenerationService() error = %v", err)
	}
	return service
}

func testEnvEntry(now time.Time, seed int64) core.EnvEntry {
	return core.EnvEntry{
		ID:       ids.NewAt(ids.KindEnvEntry, now, seed),
		Kind:     core.EntryKindEnv,
		Key:      "VALUE",
		Exposure: []string{"all"},
	}
}

func sealEntryGenerationTestSecret(
	t *testing.T,
	protector *secretvalue.Protector,
	secretID string,
	plaintext []byte,
) etcd.SecretEncryptedValue {
	t.Helper()
	envelope, err := protector.Seal(context.Background(), plaintext)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	metadata := envelope.Metadata()
	return etcd.SecretEncryptedValue{
		SecretID:         secretID,
		EnvelopeVersion:  uint8(metadata.Version),
		Cipher:           string(metadata.Cipher),
		DigestAlgorithm:  string(metadata.Digest.Algorithm),
		CiphertextSHA256: metadata.Digest.Value,
		Ciphertext:       envelope.Ciphertext(),
	}
}

func openEntryGenerationTestValue(
	t *testing.T,
	protector *secretvalue.Protector,
	generation etcd.SecretEntryValueGeneration,
) string {
	t.Helper()
	envelope, err := secretvalue.Restore(secretvalue.Metadata{
		Version: secretvalue.EnvelopeVersion(generation.EnvelopeVersion),
		Cipher:  secretvalue.CipherSuite(generation.Cipher),
		Digest: secretvalue.Digest{
			Algorithm: secretvalue.DigestAlgorithm(generation.DigestAlgorithm),
			Value:     generation.CiphertextSHA256,
		},
	}, generation.Ciphertext)
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	var value string
	if err = protector.Open(context.Background(), envelope, func(plaintext []byte) error {
		value = string(plaintext)
		return nil
	}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return value
}
