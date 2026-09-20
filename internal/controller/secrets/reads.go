package secrets

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type secretReadRepository interface {
	GetProject(context.Context, string) (etcd.Versioned[hierarchyrecord.ProjectRecord], error)
	GetSecret(context.Context, string) (etcd.Versioned[secretrecord.Record], error)
	GetSecretValue(context.Context, etcd.Versioned[secretrecord.Record]) (secretrecord.EncryptedValue, error)
	ListSecrets(context.Context, core.SecretScope, string, etcd.PageRequest) (etcd.Page[secretrecord.Record], error)
}

type secretReadService struct {
	repository secretReadRepository
	protector  *secretvalue.Protector
}

func NewReadService(
	repository secretReadRepository,
	protector *secretvalue.Protector,
) (*secretReadService, error) {
	if repository == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "Secret read dependencies are not configured")
	}
	return &secretReadService{repository: repository, protector: protector}, nil
}

func (service *secretReadService) ListSecrets(
	ctx context.Context,
	scope core.SecretScope,
	projectID string,
	request etcd.PageRequest,
) (etcd.Page[secretrecord.Record], error) {
	if ctx == nil {
		return etcd.Page[secretrecord.Record]{}, errs.New(errs.KindInternal, "Secret list context is required")
	}
	if request.Limit < 0 {
		return etcd.Page[secretrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Secret list limit must be a positive integer",
		)
	}
	switch scope {
	case core.SecretScopeProject:
		if ids.Validate(ids.KindProject, projectID) != nil {
			return etcd.Page[secretrecord.Record]{}, errs.New(
				errs.KindValidationFailed,
				"Secret list requires a stable Project id",
			)
		}
		if _, err := service.repository.GetProject(ctx, projectID); err != nil {
			return etcd.Page[secretrecord.Record]{}, err
		}
	case core.SecretScopePlatform:
		if projectID != "" {
			return etcd.Page[secretrecord.Record]{}, errs.New(
				errs.KindValidationFailed,
				"Platform Secret list must not set a Project id",
			)
		}
	default:
		return etcd.Page[secretrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Secret list scope is invalid",
		)
	}
	return service.repository.ListSecrets(ctx, scope, projectID, request)
}

func (service *secretReadService) GetSecret(
	ctx context.Context,
	secretID string,
) (etcd.Versioned[secretrecord.Record], error) {
	if ctx == nil {
		return etcd.Versioned[secretrecord.Record]{}, errs.New(errs.KindInternal, "Secret read context is required")
	}
	if ids.Validate(ids.KindSecret, secretID) != nil {
		return etcd.Versioned[secretrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Secret read requires a stable Secret id",
		)
	}
	return service.repository.GetSecret(ctx, secretID)
}

func (service *secretReadService) RevealSecret(ctx context.Context, secretID string) (string, error) {
	current, err := service.GetSecret(ctx, secretID)
	if err != nil {
		return "", err
	}
	stored, err := service.repository.GetSecretValue(ctx, current)
	if err != nil {
		return "", err
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
		return "", err
	}
	var revealed string
	if err := service.protector.Open(ctx, envelope, func(plaintext []byte) error {
		if !utf8.Valid(plaintext) {
			return errs.New(errs.KindInternal, "Secret plaintext is not valid UTF-8")
		}
		revealed = string(plaintext)
		return nil
	}); err != nil {
		return "", err
	}
	return revealed, nil
}

type durableSecretReadRepository struct {
	hierarchy *etcd.HierarchyRepository
	secrets   *etcd.SecretRepository
}

func NewReadRepository(
	hierarchy *etcd.HierarchyRepository,
	secrets *etcd.SecretRepository,
) (*durableSecretReadRepository, error) {
	if hierarchy == nil || secrets == nil {
		return nil, errs.New(errs.KindInternal, "Secret read repositories are not configured")
	}
	return &durableSecretReadRepository{hierarchy: hierarchy, secrets: secrets}, nil
}

func (repository *durableSecretReadRepository) GetProject(
	ctx context.Context,
	id string,
) (etcd.Versioned[hierarchyrecord.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableSecretReadRepository) GetSecret(
	ctx context.Context,
	id string,
) (etcd.Versioned[secretrecord.Record], error) {
	return repository.secrets.GetSecret(ctx, id)
}

func (repository *durableSecretReadRepository) GetSecretValue(
	ctx context.Context,
	current etcd.Versioned[secretrecord.Record],
) (secretrecord.EncryptedValue, error) {
	return repository.secrets.GetSecretValue(ctx, current)
}

func (repository *durableSecretReadRepository) ListSecrets(
	ctx context.Context,
	scope core.SecretScope,
	projectID string,
	request etcd.PageRequest,
) (etcd.Page[secretrecord.Record], error) {
	return repository.secrets.ListSecrets(ctx, scope, projectID, request)
}

func (repository *durableSecretReadRepository) CreateSecretIdempotent(
	ctx context.Context,
	owner etcd.SecretOwner,
	record secretrecord.Record,
	value secretrecord.EncryptedValue,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.secrets.CreateSecretIdempotent(ctx, owner, record, value, marker)
}

func (repository *durableSecretReadRepository) BeginSecretDeletionWithTask(
	ctx context.Context,
	owner etcd.SecretOwner,
	current etcd.Versioned[secretrecord.Record],
	tombstone etcd.DeletionTombstoneRecord,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.secrets.BeginSecretDeletionWithTask(ctx, owner, current, tombstone, task, marker)
}
