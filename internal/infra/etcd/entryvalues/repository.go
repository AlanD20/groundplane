package entryvalues

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	PlainPrefix  = "/v1/entry-values/plain/"
	SecretPrefix = "/v1/secret-values/entries/"
)

type PlainGeneration struct {
	EnvironmentID   string
	EntryID         string
	GenerationID    string
	Content         []byte
	PlaintextSHA256 string
	CreatedAt       time.Time
}

// SecretGeneration contains only Controller-key envelope data.
// Plaintext length and digest remain in the authenticated execution plan.
type SecretGeneration struct {
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
	Get(context.Context, string) (*etcdstore.GetResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

type Repository struct {
	store entryValueGenerationStore
}

func New(store etcdstore.Store) (*Repository, error) {
	return newEntryValueGenerationRepository(store)
}

func newEntryValueGenerationRepository(
	store entryValueGenerationStore,
) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Entry value generation store is required")
	}
	return &Repository{store: store}, nil
}

func (repository *Repository) CreatePlain(
	ctx context.Context,
	record PlainGeneration,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if repository == nil || repository.store == nil {
		return errs.New(errs.KindInternal, "Entry value generation repository is not configured")
	}
	encoded, err := EncodePlain(record)
	if err != nil {
		return err
	}
	defer clear(encoded)
	key := PlainKey(record.EntryID, record.GenerationID)
	result, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{{Key: key, ModRevision: 0}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: encoded}},
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

func (repository *Repository) CreateSecret(
	ctx context.Context,
	record SecretGeneration,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if repository == nil || repository.store == nil {
		return errs.New(errs.KindInternal, "Entry value generation repository is not configured")
	}
	encoded, err := EncodeSecret(record)
	if err != nil {
		return err
	}
	defer clear(encoded)
	key := SecretKey(record.EntryID, record.GenerationID)
	result, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{{Key: key, ModRevision: 0}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: encoded}},
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

func (repository *Repository) GetPlain(
	ctx context.Context,
	entryID string,
	generationID string,
) (PlainGeneration, bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return PlainGeneration{}, false, err
	}
	if repository == nil || repository.store == nil {
		return PlainGeneration{}, false, errs.New(
			errs.KindInternal,
			"Entry value generation repository is not configured",
		)
	}
	if recordcodec.ValidateID(ids.KindEnvEntry, entryID) != nil || recordcodec.ValidateID(ids.KindConfig, generationID) != nil {
		return PlainGeneration{}, false, errs.New(
			errs.KindValidationFailed,
			"Entry plain value generation identity is invalid",
		)
	}
	result, err := repository.store.Get(ctx, PlainKey(entryID, generationID))
	if err != nil {
		return PlainGeneration{}, false, err
	}
	if result == nil {
		return PlainGeneration{}, false, errs.New(errs.KindInternal, "Entry value generation read is empty")
	}
	if result.Entry == nil {
		return PlainGeneration{}, false, nil
	}
	record, err := DecodePlain(result.Entry.Value)
	if err != nil || record.EntryID != entryID || record.GenerationID != generationID {
		clear(record.Content)
		return PlainGeneration{}, false, corruptEntryValueGeneration()
	}
	return record, true, nil
}

func (repository *Repository) GetSecret(
	ctx context.Context,
	entryID string,
	generationID string,
) (SecretGeneration, bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return SecretGeneration{}, false, err
	}
	if repository == nil || repository.store == nil {
		return SecretGeneration{}, false, errs.New(
			errs.KindInternal,
			"Entry value generation repository is not configured",
		)
	}
	if recordcodec.ValidateID(ids.KindEnvEntry, entryID) != nil || recordcodec.ValidateID(ids.KindConfig, generationID) != nil {
		return SecretGeneration{}, false, errs.New(
			errs.KindValidationFailed,
			"Entry secret value generation identity is invalid",
		)
	}
	result, err := repository.store.Get(ctx, SecretKey(entryID, generationID))
	if err != nil {
		return SecretGeneration{}, false, err
	}
	if result == nil {
		return SecretGeneration{}, false, errs.New(errs.KindInternal, "Entry value generation read is empty")
	}
	if result.Entry == nil {
		return SecretGeneration{}, false, nil
	}
	record, err := DecodeSecret(result.Entry.Value)
	if err != nil || record.EntryID != entryID || record.GenerationID != generationID {
		clear(record.Ciphertext)
		return SecretGeneration{}, false, corruptEntryValueGeneration()
	}
	return record, true, nil
}

func PlainKey(entryID string, generationID string) string {
	return PlainPrefix + entryID + "/" + generationID
}

func SecretKey(entryID string, generationID string) string {
	return SecretPrefix + entryID + "/" + generationID
}

func EncodePlain(record PlainGeneration) ([]byte, error) {
	if err := validatePlainEntryValueGeneration(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("entry_plain_value_generation", plainEntryValueGenerationData{
		EnvironmentID: record.EnvironmentID, EntryID: record.EntryID, GenerationID: record.GenerationID,
		Content: append([]byte(nil), record.Content...), PlaintextSHA256: record.PlaintextSHA256,
		CreatedAt: record.CreatedAt.Format(time.RFC3339Nano),
	})
}

func DecodePlain(value []byte) (PlainGeneration, error) {
	data, err := recordcodec.Decode[plainEntryValueGenerationData](value, "entry_plain_value_generation")
	if err != nil {
		return PlainGeneration{}, err
	}
	createdAt, err := recordcodec.ParseCanonicalTimestamp(data.CreatedAt)
	if err != nil {
		clear(data.Content)
		return PlainGeneration{}, corruptEntryValueGeneration()
	}
	record := PlainGeneration{
		EnvironmentID: data.EnvironmentID, EntryID: data.EntryID, GenerationID: data.GenerationID,
		Content: data.Content, PlaintextSHA256: data.PlaintextSHA256, CreatedAt: createdAt,
	}
	if err := validatePlainEntryValueGeneration(record); err != nil {
		clear(record.Content)
		return PlainGeneration{}, corruptEntryValueGeneration()
	}
	return record, nil
}

func EncodeSecret(record SecretGeneration) ([]byte, error) {
	if err := validateSecretEntryValueGeneration(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("entry_secret_value_generation", secretEntryValueGenerationData{
		EnvironmentID: record.EnvironmentID, EntryID: record.EntryID, GenerationID: record.GenerationID,
		EnvelopeVersion: record.EnvelopeVersion, Cipher: record.Cipher,
		DigestAlgorithm: record.DigestAlgorithm, CiphertextSHA256: record.CiphertextSHA256,
		Ciphertext: append([]byte(nil), record.Ciphertext...), CreatedAt: record.CreatedAt.Format(time.RFC3339Nano),
	})
}

func DecodeSecret(value []byte) (SecretGeneration, error) {
	data, err := recordcodec.Decode[secretEntryValueGenerationData](value, "entry_secret_value_generation")
	if err != nil {
		return SecretGeneration{}, err
	}
	createdAt, err := recordcodec.ParseCanonicalTimestamp(data.CreatedAt)
	if err != nil {
		clear(data.Ciphertext)
		return SecretGeneration{}, corruptEntryValueGeneration()
	}
	record := SecretGeneration{
		EnvironmentID: data.EnvironmentID, EntryID: data.EntryID, GenerationID: data.GenerationID,
		EnvelopeVersion: data.EnvelopeVersion, Cipher: data.Cipher, DigestAlgorithm: data.DigestAlgorithm,
		CiphertextSHA256: data.CiphertextSHA256, Ciphertext: data.Ciphertext, CreatedAt: createdAt,
	}
	if err := validateSecretEntryValueGeneration(record); err != nil {
		clear(record.Ciphertext)
		return SecretGeneration{}, corruptEntryValueGeneration()
	}
	return record, nil
}

func validatePlainEntryValueGeneration(record PlainGeneration) error {
	if err := validateEntryValueGenerationIdentity(
		record.EnvironmentID,
		record.EntryID,
		record.GenerationID,
		record.CreatedAt,
	); err != nil {
		return err
	}
	if len(record.Content) > recordcodec.MaximumValueBytes || !recordcodec.ValidSHA256(record.PlaintextSHA256) {
		return errs.New(errs.KindValidationFailed, "Entry plain value generation is invalid")
	}
	digest := sha256.Sum256(record.Content)
	want, _ := hex.DecodeString(record.PlaintextSHA256)
	if subtle.ConstantTimeCompare(digest[:], want) != 1 {
		return errs.New(errs.KindValidationFailed, "Entry plain value generation digest does not match")
	}
	return nil
}

func validateSecretEntryValueGeneration(record SecretGeneration) error {
	if err := validateEntryValueGenerationIdentity(
		record.EnvironmentID,
		record.EntryID,
		record.GenerationID,
		record.CreatedAt,
	); err != nil {
		return err
	}
	if record.EnvelopeVersion != 1 || record.Cipher != "age-x25519" || record.DigestAlgorithm != "sha256" ||
		len(record.Ciphertext) == 0 || len(record.Ciphertext) > recordcodec.MaximumValueBytes ||
		!recordcodec.ValidSHA256(record.CiphertextSHA256) {
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
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil ||
		recordcodec.ValidateID(ids.KindEnvEntry, entryID) != nil ||
		recordcodec.ValidateID(ids.KindConfig, generationID) != nil {
		return errs.New(errs.KindValidationFailed, "Entry value generation identity is invalid")
	}
	return recordcodec.ValidateTimestamp("Entry value generation created_at", createdAt)
}

func equalPlainEntryValueGeneration(left PlainGeneration, right PlainGeneration) bool {
	return left.EnvironmentID == right.EnvironmentID && left.EntryID == right.EntryID &&
		left.GenerationID == right.GenerationID && left.PlaintextSHA256 == right.PlaintextSHA256 &&
		left.CreatedAt.Equal(right.CreatedAt) && bytes.Equal(left.Content, right.Content)
}

func equalSecretEntryValueGeneration(left SecretGeneration, right SecretGeneration) bool {
	return left.EnvironmentID == right.EnvironmentID && left.EntryID == right.EntryID &&
		left.GenerationID == right.GenerationID && left.EnvelopeVersion == right.EnvelopeVersion &&
		left.Cipher == right.Cipher && left.DigestAlgorithm == right.DigestAlgorithm &&
		left.CiphertextSHA256 == right.CiphertextSHA256 && left.CreatedAt.Equal(right.CreatedAt) &&
		bytes.Equal(left.Ciphertext, right.Ciphertext)
}

func corruptEntryValueGeneration() error {
	return errs.New(errs.KindInternal, "Entry value generation record is corrupt")
}
