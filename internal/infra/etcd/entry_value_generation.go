package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	entryPlainValueGenerationPrefix  = "/v1/entry-values/plain/"
	entrySecretValueGenerationPrefix = "/v1/secret-values/entries/"
	MaximumEntryValueBytes           = 256 << 10
)

type PlainEntryValueGeneration struct {
	EnvironmentID   string
	EntryID         string
	GenerationID    string
	Content         []byte
	PlaintextSHA256 string
	CreatedAt       time.Time
}

// SecretEntryValueGeneration contains only Controller-key envelope data.
// Plaintext length and digest remain in the authenticated execution plan.
type SecretEntryValueGeneration struct {
	EnvironmentID    string
	EntryID          string
	GenerationID     string
	EnvelopeVersion  uint8
	Cipher           string
	DigestAlgorithm  string
	CiphertextSHA256 string
	Ciphertext       []byte
	CreatedAt        time.Time
}

type plainEntryValueGenerationData struct {
	EnvironmentID   string `json:"environment_id"`
	EntryID         string `json:"entry_id"`
	GenerationID    string `json:"generation_id"`
	Content         []byte `json:"content"`
	PlaintextSHA256 string `json:"plaintext_sha256"`
	CreatedAt       string `json:"created_at"`
}

type secretEntryValueGenerationData struct {
	EnvironmentID    string `json:"environment_id"`
	EntryID          string `json:"entry_id"`
	GenerationID     string `json:"generation_id"`
	EnvelopeVersion  uint8  `json:"envelope_version"`
	Cipher           string `json:"cipher"`
	DigestAlgorithm  string `json:"digest_algorithm"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
	Ciphertext       []byte `json:"ciphertext"`
	CreatedAt        string `json:"created_at"`
}

type entryValueGenerationStore interface {
	Get(context.Context, string) (*GetResult, error)
	Transact(context.Context, []Condition, []Mutation) (TransactionResult, error)
}

type EntryValueGenerationRepository struct {
	store entryValueGenerationStore
}

func NewEntryValueGenerationRepository(store Store) (*EntryValueGenerationRepository, error) {
	return newEntryValueGenerationRepository(store)
}

func newEntryValueGenerationRepository(
	store entryValueGenerationStore,
) (*EntryValueGenerationRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Entry value generation store is required")
	}
	return &EntryValueGenerationRepository{store: store}, nil
}

func (repository *EntryValueGenerationRepository) CreatePlain(
	ctx context.Context,
	record PlainEntryValueGeneration,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if repository == nil || repository.store == nil {
		return errs.New(errs.KindInternal, "Entry value generation repository is not configured")
	}
	encoded, err := encodePlainEntryValueGeneration(record)
	if err != nil {
		return err
	}
	defer clear(encoded)
	key := plainEntryValueGenerationKey(record.EntryID, record.GenerationID)
	result, err := repository.store.Transact(
		ctx,
		[]Condition{{Key: key, ModRevision: 0}},
		[]Mutation{{Type: MutationPut, Key: key, Value: encoded}},
	)
	if err != nil {
		return err
	}
	if result.Succeeded {
		return nil
	}
	existing, found, err := repository.GetPlain(ctx, record.EntryID, record.GenerationID)
	if err != nil {
		return err
	}
	defer clear(existing.Content)
	if found && equalPlainEntryValueGeneration(existing, record) {
		return nil
	}
	return errs.New(errs.KindStateConflict, "Entry plain value generation id is already occupied")
}

func (repository *EntryValueGenerationRepository) CreateSecret(
	ctx context.Context,
	record SecretEntryValueGeneration,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if repository == nil || repository.store == nil {
		return errs.New(errs.KindInternal, "Entry value generation repository is not configured")
	}
	encoded, err := encodeSecretEntryValueGeneration(record)
	if err != nil {
		return err
	}
	defer clear(encoded)
	key := secretEntryValueGenerationKey(record.EntryID, record.GenerationID)
	result, err := repository.store.Transact(
		ctx,
		[]Condition{{Key: key, ModRevision: 0}},
		[]Mutation{{Type: MutationPut, Key: key, Value: encoded}},
	)
	if err != nil {
		return err
	}
	if result.Succeeded {
		return nil
	}
	existing, found, err := repository.GetSecret(ctx, record.EntryID, record.GenerationID)
	if err != nil {
		return err
	}
	defer clear(existing.Ciphertext)
	if found && equalSecretEntryValueGeneration(existing, record) {
		return nil
	}
	return errs.New(errs.KindStateConflict, "Entry secret value generation id is already occupied")
}

func (repository *EntryValueGenerationRepository) GetPlain(
	ctx context.Context,
	entryID string,
	generationID string,
) (PlainEntryValueGeneration, bool, error) {
	if err := validateContext(ctx); err != nil {
		return PlainEntryValueGeneration{}, false, err
	}
	if repository == nil || repository.store == nil {
		return PlainEntryValueGeneration{}, false, errs.New(
			errs.KindInternal,
			"Entry value generation repository is not configured",
		)
	}
	if validateStableID(ids.KindEnvEntry, entryID) != nil || validateStableID(ids.KindConfig, generationID) != nil {
		return PlainEntryValueGeneration{}, false, errs.New(
			errs.KindValidationFailed,
			"Entry plain value generation identity is invalid",
		)
	}
	result, err := repository.store.Get(ctx, plainEntryValueGenerationKey(entryID, generationID))
	if err != nil {
		return PlainEntryValueGeneration{}, false, err
	}
	if result == nil {
		return PlainEntryValueGeneration{}, false, errs.New(errs.KindInternal, "Entry value generation read is empty")
	}
	if result.Entry == nil {
		return PlainEntryValueGeneration{}, false, nil
	}
	record, err := decodePlainEntryValueGeneration(result.Entry.Value)
	if err != nil || record.EntryID != entryID || record.GenerationID != generationID {
		clear(record.Content)
		return PlainEntryValueGeneration{}, false, corruptEntryValueGeneration()
	}
	return record, true, nil
}

func (repository *EntryValueGenerationRepository) GetSecret(
	ctx context.Context,
	entryID string,
	generationID string,
) (SecretEntryValueGeneration, bool, error) {
	if err := validateContext(ctx); err != nil {
		return SecretEntryValueGeneration{}, false, err
	}
	if repository == nil || repository.store == nil {
		return SecretEntryValueGeneration{}, false, errs.New(
			errs.KindInternal,
			"Entry value generation repository is not configured",
		)
	}
	if validateStableID(ids.KindEnvEntry, entryID) != nil || validateStableID(ids.KindConfig, generationID) != nil {
		return SecretEntryValueGeneration{}, false, errs.New(
			errs.KindValidationFailed,
			"Entry secret value generation identity is invalid",
		)
	}
	result, err := repository.store.Get(ctx, secretEntryValueGenerationKey(entryID, generationID))
	if err != nil {
		return SecretEntryValueGeneration{}, false, err
	}
	if result == nil {
		return SecretEntryValueGeneration{}, false, errs.New(errs.KindInternal, "Entry value generation read is empty")
	}
	if result.Entry == nil {
		return SecretEntryValueGeneration{}, false, nil
	}
	record, err := decodeSecretEntryValueGeneration(result.Entry.Value)
	if err != nil || record.EntryID != entryID || record.GenerationID != generationID {
		clear(record.Ciphertext)
		return SecretEntryValueGeneration{}, false, corruptEntryValueGeneration()
	}
	return record, true, nil
}

func plainEntryValueGenerationKey(entryID string, generationID string) string {
	return entryPlainValueGenerationPrefix + entryID + "/" + generationID
}

func secretEntryValueGenerationKey(entryID string, generationID string) string {
	return entrySecretValueGenerationPrefix + entryID + "/" + generationID
}

func encodePlainEntryValueGeneration(record PlainEntryValueGeneration) ([]byte, error) {
	if err := validatePlainEntryValueGeneration(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("entry_plain_value_generation", plainEntryValueGenerationData{
		EnvironmentID: record.EnvironmentID, EntryID: record.EntryID, GenerationID: record.GenerationID,
		Content: append([]byte(nil), record.Content...), PlaintextSHA256: record.PlaintextSHA256,
		CreatedAt: record.CreatedAt.Format(time.RFC3339Nano),
	})
}

func decodePlainEntryValueGeneration(value []byte) (PlainEntryValueGeneration, error) {
	data, err := decodeEnvelope[plainEntryValueGenerationData](value, "entry_plain_value_generation")
	if err != nil {
		return PlainEntryValueGeneration{}, err
	}
	createdAt, err := parseCanonicalTimestamp(data.CreatedAt)
	if err != nil {
		clear(data.Content)
		return PlainEntryValueGeneration{}, corruptEntryValueGeneration()
	}
	record := PlainEntryValueGeneration{
		EnvironmentID: data.EnvironmentID, EntryID: data.EntryID, GenerationID: data.GenerationID,
		Content: data.Content, PlaintextSHA256: data.PlaintextSHA256, CreatedAt: createdAt,
	}
	if err := validatePlainEntryValueGeneration(record); err != nil {
		clear(record.Content)
		return PlainEntryValueGeneration{}, corruptEntryValueGeneration()
	}
	return record, nil
}

func encodeSecretEntryValueGeneration(record SecretEntryValueGeneration) ([]byte, error) {
	if err := validateSecretEntryValueGeneration(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("entry_secret_value_generation", secretEntryValueGenerationData{
		EnvironmentID: record.EnvironmentID, EntryID: record.EntryID, GenerationID: record.GenerationID,
		EnvelopeVersion: record.EnvelopeVersion, Cipher: record.Cipher,
		DigestAlgorithm: record.DigestAlgorithm, CiphertextSHA256: record.CiphertextSHA256,
		Ciphertext: append([]byte(nil), record.Ciphertext...), CreatedAt: record.CreatedAt.Format(time.RFC3339Nano),
	})
}

func decodeSecretEntryValueGeneration(value []byte) (SecretEntryValueGeneration, error) {
	data, err := decodeEnvelope[secretEntryValueGenerationData](value, "entry_secret_value_generation")
	if err != nil {
		return SecretEntryValueGeneration{}, err
	}
	createdAt, err := parseCanonicalTimestamp(data.CreatedAt)
	if err != nil {
		clear(data.Ciphertext)
		return SecretEntryValueGeneration{}, corruptEntryValueGeneration()
	}
	record := SecretEntryValueGeneration{
		EnvironmentID: data.EnvironmentID, EntryID: data.EntryID, GenerationID: data.GenerationID,
		EnvelopeVersion: data.EnvelopeVersion, Cipher: data.Cipher, DigestAlgorithm: data.DigestAlgorithm,
		CiphertextSHA256: data.CiphertextSHA256, Ciphertext: data.Ciphertext, CreatedAt: createdAt,
	}
	if err := validateSecretEntryValueGeneration(record); err != nil {
		clear(record.Ciphertext)
		return SecretEntryValueGeneration{}, corruptEntryValueGeneration()
	}
	return record, nil
}

func validatePlainEntryValueGeneration(record PlainEntryValueGeneration) error {
	if err := validateEntryValueGenerationIdentity(
		record.EnvironmentID,
		record.EntryID,
		record.GenerationID,
		record.CreatedAt,
	); err != nil {
		return err
	}
	if len(record.Content) > MaximumEntryValueBytes || !validSHA256(record.PlaintextSHA256) {
		return errs.New(errs.KindValidationFailed, "Entry plain value generation is invalid")
	}
	digest := sha256.Sum256(record.Content)
	want, _ := hex.DecodeString(record.PlaintextSHA256)
	if subtle.ConstantTimeCompare(digest[:], want) != 1 {
		return errs.New(errs.KindValidationFailed, "Entry plain value generation digest does not match")
	}
	return nil
}

func validateSecretEntryValueGeneration(record SecretEntryValueGeneration) error {
	if err := validateEntryValueGenerationIdentity(
		record.EnvironmentID,
		record.EntryID,
		record.GenerationID,
		record.CreatedAt,
	); err != nil {
		return err
	}
	if record.EnvelopeVersion != 1 || record.Cipher != "age-x25519" || record.DigestAlgorithm != "sha256" ||
		len(record.Ciphertext) == 0 || len(record.Ciphertext) > MaximumEntryValueBytes ||
		!validSHA256(record.CiphertextSHA256) {
		return errs.New(errs.KindValidationFailed, "Entry secret value generation envelope is invalid")
	}
	digest := sha256.Sum256(record.Ciphertext)
	want, _ := hex.DecodeString(record.CiphertextSHA256)
	if subtle.ConstantTimeCompare(digest[:], want) != 1 {
		return errs.New(errs.KindValidationFailed, "Entry secret value generation digest does not match")
	}
	return nil
}

func validateEntryValueGenerationIdentity(
	environmentID string,
	entryID string,
	generationID string,
	createdAt time.Time,
) error {
	if validateStableID(ids.KindEnvironment, environmentID) != nil ||
		validateStableID(ids.KindEnvEntry, entryID) != nil ||
		validateStableID(ids.KindConfig, generationID) != nil {
		return errs.New(errs.KindValidationFailed, "Entry value generation identity is invalid")
	}
	return validateTimestamp("Entry value generation created_at", createdAt)
}

func equalPlainEntryValueGeneration(left PlainEntryValueGeneration, right PlainEntryValueGeneration) bool {
	return left.EnvironmentID == right.EnvironmentID && left.EntryID == right.EntryID &&
		left.GenerationID == right.GenerationID && left.PlaintextSHA256 == right.PlaintextSHA256 &&
		left.CreatedAt.Equal(right.CreatedAt) && bytes.Equal(left.Content, right.Content)
}

func equalSecretEntryValueGeneration(left SecretEntryValueGeneration, right SecretEntryValueGeneration) bool {
	return left.EnvironmentID == right.EnvironmentID && left.EntryID == right.EntryID &&
		left.GenerationID == right.GenerationID && left.EnvelopeVersion == right.EnvelopeVersion &&
		left.Cipher == right.Cipher && left.DigestAlgorithm == right.DigestAlgorithm &&
		left.CiphertextSHA256 == right.CiphertextSHA256 && left.CreatedAt.Equal(right.CreatedAt) &&
		bytes.Equal(left.Ciphertext, right.Ciphertext)
}

func corruptEntryValueGeneration() error {
	return errs.New(errs.KindInternal, "Entry value generation record is corrupt")
}
