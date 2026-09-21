package entrygeneration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	entries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type EntrySecretRepository interface {
	ResolveSecret(context.Context, string, string) (etcdstore.Versioned[secretrecord.Record], error)
	GetSecretValue(context.Context, etcdstore.Versioned[secretrecord.Record]) (secretrecord.EncryptedValue, error)
}

type EntryFactResolver interface {
	ResolveFact(
		context.Context,
		string,
		core.FactRef,
		bool,
		secretvalue.PlaintextConsumer,
	) error
}

type EntryGenerationService struct {
	secrets   EntrySecretRepository
	facts     EntryFactResolver
	protector *secretvalue.Protector
}

func NewEntryGenerationService(
	secrets EntrySecretRepository,
	facts EntryFactResolver,
	protector *secretvalue.Protector,
) (*EntryGenerationService, error) {
	if secrets == nil || facts == nil || protector == nil {
		return nil, errs.New(errs.KindValidationFailed, "Entry source repositories and protector are required")
	}
	return &EntryGenerationService{secrets: secrets, facts: facts, protector: protector}, nil
}

// WithFactResolver returns an otherwise identical generator that resolves
// facts through the supplied operation-local view.
func (service *EntryGenerationService) WithFactResolver(
	facts EntryFactResolver,
) *EntryGenerationService {
	configured := *service
	configured.facts = facts
	return &configured
}

// Generate resolves one live desired source and freezes its exact bytes into
// an immutable generation for an atomic Entry mutation.
func (service *EntryGenerationService) Generate(
	ctx context.Context,
	projectID string,
	environmentID string,
	entry core.EnvEntry,
	generationID string,
	createdAt time.Time,
) (entries.EntryValueGeneration, error) {
	if ctx == nil {
		return entries.EntryValueGeneration{}, errs.New(errs.KindValidationFailed, "Entry generation context is required")
	}
	for _, check := range []struct {
		kind  ids.Kind
		value string
		field string
	}{
		{ids.KindProject, projectID, "Entry Project"},
		{ids.KindEnvironment, environmentID, "Entry Environment"},
		{ids.KindEnvEntry, entry.ID, "Entry"},
		{ids.KindConfig, generationID, "Entry generation"},
	} {
		if ids.Validate(check.kind, check.value) != nil {
			return entries.EntryValueGeneration{}, errs.Newf(
				errs.KindValidationFailed,
				"%s stable id is invalid",
				check.field,
			)
		}
	}
	validationEntry := entry
	if validationEntry.Secret && validationEntry.Source.Kind == core.SourceLiteral {
		validationEntry.Source.Literal = ""
	}
	if err := validationEntry.Validate(); err != nil {
		return entries.EntryValueGeneration{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	if createdAt.IsZero() {
		return entries.EntryValueGeneration{}, errs.New(
			errs.KindValidationFailed,
			"Entry generation created_at is required",
		)
	}

	var generation entries.EntryValueGeneration
	err := service.withResolvedEntrySource(ctx, projectID, environmentID, entry, func(value []byte) error {
		if len(value) > recordcodec.MaximumValueBytes {
			return errs.New(errs.KindValidationFailed, "Resolved Entry value exceeds the maximum size")
		}
		if !entry.Secret {
			digest := sha256.Sum256(value)
			generation.Plain = &entryvalues.PlainGeneration{
				EnvironmentID:   environmentID,
				EntryID:         entry.ID,
				GenerationID:    generationID,
				Content:         append([]byte(nil), value...),
				PlaintextSHA256: hex.EncodeToString(digest[:]),
				CreatedAt:       createdAt.UTC(),
			}
			return nil
		}
		envelope, sealErr := service.protector.Seal(ctx, value)
		if sealErr != nil {
			return sealErr
		}
		ciphertext := envelope.Ciphertext()
		defer clear(ciphertext)
		if len(ciphertext) == 0 || len(ciphertext) > recordcodec.MaximumValueBytes {
			return errs.New(errs.KindValidationFailed, "Encrypted Entry generation exceeds the maximum size")
		}
		metadata := envelope.Metadata()
		generation.Secret = &entryvalues.SecretGeneration{
			EnvironmentID:    environmentID,
			EntryID:          entry.ID,
			GenerationID:     generationID,
			EnvelopeVersion:  uint8(metadata.Version),
			Cipher:           string(metadata.Cipher),
			DigestAlgorithm:  string(metadata.Digest.Algorithm),
			CiphertextSHA256: metadata.Digest.Value,
			Ciphertext:       append([]byte(nil), ciphertext...),
			CreatedAt:        createdAt.UTC(),
		}
		return nil
	})
	if err != nil {
		ClearEntryValueGeneration(&generation)
		return entries.EntryValueGeneration{}, err
	}
	return generation, nil
}

func (service *EntryGenerationService) withResolvedEntrySource(
	ctx context.Context,
	projectID string,
	environmentID string,
	entry core.EnvEntry,
	consume secretvalue.PlaintextConsumer,
) error {
	switch entry.Source.Kind {
	case core.SourceLiteral:
		value := []byte(entry.Source.Literal)
		defer clear(value)
		return consume(value)
	case core.SourceSecretRef:
		if !entry.Secret {
			return errs.New(errs.KindValidationFailed, "Reusable Secret requires a secret Entry destination")
		}
		current, err := service.secrets.ResolveSecret(ctx, projectID, entry.Source.SecretRef)
		if err != nil {
			return err
		}
		if !entrySecretKindMatches(entry.Kind, current.Record.Secret.Kind) {
			return errs.New(errs.KindValidationFailed, "Reusable Secret kind does not match the Entry destination")
		}
		stored, err := service.secrets.GetSecretValue(ctx, current)
		if err != nil {
			return err
		}
		defer clear(stored.Ciphertext)
		envelope, err := secretvalue.Restore(secretvalue.Metadata{
			Version: secretvalue.EnvelopeVersion(stored.EnvelopeVersion),
			Cipher:  secretvalue.CipherSuite(stored.Cipher),
			Digest: secretvalue.Digest{
				Algorithm: secretvalue.DigestAlgorithm(stored.DigestAlgorithm),
				Value:     stored.CiphertextSHA256,
			},
		}, stored.Ciphertext)
		if err != nil {
			return err
		}
		return service.protector.Open(ctx, envelope, consume)
	case core.SourceFact:
		return service.facts.ResolveFact(ctx, environmentID, *entry.Source.Fact, entry.Secret, consume)
	default:
		return errs.New(errs.KindValidationFailed, "Entry source kind is invalid")
	}
}

func entrySecretKindMatches(entryKind core.EntryKind, secretKind core.SecretKind) bool {
	return entryKind == core.EntryKindEnv && secretKind == core.SecretKindEnvVar ||
		entryKind == core.EntryKindFile && secretKind == core.SecretKindFile
}

func ClearEntryValueGeneration(generation *entries.EntryValueGeneration) {
	if generation == nil {
		return
	}
	if generation.Plain != nil {
		clear(generation.Plain.Content)
		generation.Plain.Content = nil
	}
	if generation.Secret != nil {
		clear(generation.Secret.Ciphertext)
		generation.Secret.Ciphertext = nil
	}
	generation.Plain = nil
	generation.Secret = nil
}
