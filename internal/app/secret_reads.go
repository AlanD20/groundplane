package app

import (
	"context"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type secretReadRepository interface {
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetSecret(context.Context, string) (etcd.Versioned[etcd.SecretRecord], error)
	GetSecretValue(context.Context, etcd.Versioned[etcd.SecretRecord]) (etcd.SecretEncryptedValue, error)
	ListSecrets(context.Context, core.SecretScope, string, etcd.PageRequest) (etcd.Page[etcd.SecretRecord], error)
}

type secretReadService struct {
	repository secretReadRepository
	protector  *secretvalue.Protector
}

func newSecretReadService(
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
) (etcd.Page[etcd.SecretRecord], error) {
	if ctx == nil {
		return etcd.Page[etcd.SecretRecord]{}, errs.New(errs.KindInternal, "Secret list context is required")
	}
	if request.Limit < 0 {
		return etcd.Page[etcd.SecretRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Secret list limit must be a positive integer",
		)
	}
	switch scope {
	case core.SecretScopeProject:
		if ids.Validate(ids.KindProject, projectID) != nil {
			return etcd.Page[etcd.SecretRecord]{}, errs.New(
				errs.KindValidationFailed,
				"Secret list requires a stable Project id",
			)
		}
		if _, err := service.repository.GetProject(ctx, projectID); err != nil {
			return etcd.Page[etcd.SecretRecord]{}, err
		}
	case core.SecretScopePlatform:
		if projectID != "" {
			return etcd.Page[etcd.SecretRecord]{}, errs.New(
				errs.KindValidationFailed,
				"Platform Secret list must not set a Project id",
			)
		}
	default:
		return etcd.Page[etcd.SecretRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Secret list scope is invalid",
		)
	}
	return service.repository.ListSecrets(ctx, scope, projectID, request)
}

func (service *secretReadService) GetSecret(
	ctx context.Context,
	secretID string,
) (etcd.Versioned[etcd.SecretRecord], error) {
	if ctx == nil {
		return etcd.Versioned[etcd.SecretRecord]{}, errs.New(errs.KindInternal, "Secret read context is required")
	}
	if ids.Validate(ids.KindSecret, secretID) != nil {
		return etcd.Versioned[etcd.SecretRecord]{}, errs.New(
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

func newDurableSecretReadRepository(
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
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableSecretReadRepository) GetSecret(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.SecretRecord], error) {
	return repository.secrets.GetSecret(ctx, id)
}

func (repository *durableSecretReadRepository) GetSecretValue(
	ctx context.Context,
	current etcd.Versioned[etcd.SecretRecord],
) (etcd.SecretEncryptedValue, error) {
	return repository.secrets.GetSecretValue(ctx, current)
}

func (repository *durableSecretReadRepository) ListSecrets(
	ctx context.Context,
	scope core.SecretScope,
	projectID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.SecretRecord], error) {
	return repository.secrets.ListSecrets(ctx, scope, projectID, request)
}
