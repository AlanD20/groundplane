package app

import (
	"context"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type entryReadRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetEntry(context.Context, string) (etcd.Versioned[etcd.EntryRecord], error)
	ListEntries(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.EntryRecord], error)
	GetEnvironmentComposeProjection(context.Context, string) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	GetEnvironmentComposeProjectionRevision(context.Context, string, string) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	GetSecretEntryValue(
		context.Context,
		string,
		string,
	) (etcd.SecretEntryValueGeneration, bool, error)
}

type entryReadService struct {
	repository entryReadRepository
	protector  *secretvalue.Protector
}

func newEntryReadService(
	repository entryReadRepository,
	protector *secretvalue.Protector,
) (*entryReadService, error) {
	if repository == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "Entry read dependencies are not configured")
	}
	return &entryReadService{repository: repository, protector: protector}, nil
}

func (service *entryReadService) ListEntries(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.EntryRecord], error) {
	if ctx == nil {
		return etcd.Page[etcd.EntryRecord]{}, errs.New(errs.KindInternal, "Entry list context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.Page[etcd.EntryRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Entry list requires a stable Environment id",
		)
	}
	if request.Limit < 0 {
		return etcd.Page[etcd.EntryRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Entry list limit must be a positive integer",
		)
	}
	if _, err := service.repository.GetEnvironment(ctx, environmentID); err != nil {
		return etcd.Page[etcd.EntryRecord]{}, err
	}
	if page, found, err := service.listProjectedEntries(ctx, environmentID, request); err != nil || found {
		return page, err
	}
	return service.repository.ListEntries(ctx, environmentID, request)
}

func (service *entryReadService) GetEntry(
	ctx context.Context,
	entryID string,
) (etcd.Versioned[etcd.EntryRecord], error) {
	if ctx == nil {
		return etcd.Versioned[etcd.EntryRecord]{}, errs.New(errs.KindInternal, "Entry read context is required")
	}
	if ids.Validate(ids.KindEnvEntry, entryID) != nil {
		return etcd.Versioned[etcd.EntryRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Entry read requires a stable Entry id",
		)
	}
	return service.repository.GetEntry(ctx, entryID)
}

func (service *entryReadService) RevealEntry(ctx context.Context, entryID string) (string, error) {
	current, err := service.GetEntry(ctx, entryID)
	if err != nil {
		return "", err
	}
	if !current.Record.Entry.Secret {
		return "", errs.New(errs.KindValidationFailed, "Entry value reveal requires a secret Entry")
	}
	stored, found, err := service.repository.GetSecretEntryValue(
		ctx,
		current.Record.Entry.ID,
		current.Record.CurrentValueGenerationID,
	)
	if err != nil {
		return "", err
	}
	defer clear(stored.Ciphertext)
	if !found || stored.EnvironmentID != current.Record.EnvironmentID ||
		stored.EntryID != current.Record.Entry.ID ||
		stored.GenerationID != current.Record.CurrentValueGenerationID {
		return "", errs.New(errs.KindInternal, "Entry selected secret value generation is missing or mismatched")
	}
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
			return errs.New(errs.KindInternal, "Entry plaintext is not valid UTF-8")
		}
		revealed = string(plaintext)
		return nil
	}); err != nil {
		return "", err
	}
	return revealed, nil
}

type durableEntryReadRepository struct {
	hierarchy *etcd.HierarchyRepository
	entries   *etcd.EntryRepository
	values    *etcd.EntryValueGenerationRepository
}

func newDurableEntryReadRepository(
	hierarchy *etcd.HierarchyRepository,
	entries *etcd.EntryRepository,
	values *etcd.EntryValueGenerationRepository,
) (*durableEntryReadRepository, error) {
	if hierarchy == nil || entries == nil || values == nil {
		return nil, errs.New(errs.KindInternal, "Entry read repositories are not configured")
	}
	return &durableEntryReadRepository{hierarchy: hierarchy, entries: entries, values: values}, nil
}

func (repository *durableEntryReadRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableEntryReadRepository) GetEntry(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.EntryRecord], error) {
	current, err := repository.entries.GetEntry(ctx, id)
	if err == nil {
		return current, nil
	}
	kind, ok := errs.KindOf(err)
	if !ok || kind != errs.KindEntryNotFound {
		return etcd.Versioned[etcd.EntryRecord]{}, err
	}
	environmentID, found, err := repository.entries.ResolveBlueprintEntryEnvironment(ctx, id)
	if err != nil || !found {
		return etcd.Versioned[etcd.EntryRecord]{}, err
	}
	projection, found, err := repository.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return etcd.Versioned[etcd.EntryRecord]{}, err
	}
	if found {
		for _, record := range projection.Record.Entries {
			if record.Entry.ID == id {
				return etcd.Versioned[etcd.EntryRecord]{
					Record: record, Revision: projection.Revision, ReadRevision: projection.ReadRevision,
				}, nil
			}
		}
	}
	return etcd.Versioned[etcd.EntryRecord]{}, errs.New(errs.KindEntryNotFound, "Entry was not found")
}

func (repository *durableEntryReadRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
}

func (repository *durableEntryReadRepository) GetEnvironmentComposeProjectionRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjectionRevision(ctx, environmentID, revisionID)
}

func (repository *durableEntryReadRepository) ListEntries(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.EntryRecord], error) {
	return repository.entries.ListEntries(ctx, environmentID, request)
}

func (repository *durableEntryReadRepository) GetSecretEntryValue(
	ctx context.Context,
	entryID string,
	generationID string,
) (etcd.SecretEntryValueGeneration, bool, error) {
	return repository.values.GetSecret(ctx, entryID, generationID)
}
