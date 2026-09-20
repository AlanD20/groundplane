package operations

import (
	"context"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type entryReadRepository interface {
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetEntry(context.Context, string) (etcdstore.Versioned[entryrecord.Record], error)
	ListEntries(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[entryrecord.Record], error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	GetEnvironmentComposeProjectionRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	GetSecretEntryValue(
		context.Context,
		string,
		string,
	) (entryvalues.SecretGeneration, bool, error)
}

type entryReadService struct {
	repository entryReadRepository
	protector  *secretvalue.Protector
}

func NewReadService(
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
	request etcdstore.PageRequest,
) (etcdstore.Page[entryrecord.Record], error) {
	if ctx == nil {
		return etcdstore.Page[entryrecord.Record]{}, errs.New(errs.KindInternal, "Entry list context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcdstore.Page[entryrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Entry list requires a stable Environment id",
		)
	}
	if request.Limit < 0 {
		return etcdstore.Page[entryrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Entry list limit must be a positive integer",
		)
	}
	if _, err := service.repository.GetEnvironment(ctx, environmentID); err != nil {
		return etcdstore.Page[entryrecord.Record]{}, err
	}
	if page, found, err := service.listProjectedEntries(ctx, environmentID, request); err != nil || found {
		return page, err
	}
	return service.repository.ListEntries(ctx, environmentID, request)
}

func (service *entryReadService) GetEntry(
	ctx context.Context,
	entryID string,
) (etcdstore.Versioned[entryrecord.Record], error) {
	if ctx == nil {
		return etcdstore.Versioned[entryrecord.Record]{}, errs.New(errs.KindInternal, "Entry read context is required")
	}
	if ids.Validate(ids.KindEnvEntry, entryID) != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, errs.New(
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
	values    *entryvalues.Repository
}

func NewReadRepository(
	hierarchy *etcd.HierarchyRepository,
	entries *etcd.EntryRepository,
	values *entryvalues.Repository,
) (*durableEntryReadRepository, error) {
	if hierarchy == nil || entries == nil || values == nil {
		return nil, errs.New(errs.KindInternal, "Entry read repositories are not configured")
	}
	return &durableEntryReadRepository{hierarchy: hierarchy, entries: entries, values: values}, nil
}

func (repository *durableEntryReadRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableEntryReadRepository) GetEntry(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[entryrecord.Record], error) {
	current, err := repository.entries.GetEntry(ctx, id)
	if err == nil {
		return current, nil
	}
	kind, ok := errs.KindOf(err)
	if !ok || kind != errs.KindEntryNotFound {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	environmentID, found, err := repository.entries.ResolveBlueprintEntryEnvironment(ctx, id)
	if err != nil || !found {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	projection, found, err := repository.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	if found {
		for _, record := range projection.Record.Entries {
			if record.Entry.ID == id {
				return etcdstore.Versioned[entryrecord.Record]{
					Record: record, Revision: projection.Revision, ReadRevision: projection.ReadRevision,
				}, nil
			}
		}
	}
	return etcdstore.Versioned[entryrecord.Record]{}, errs.New(errs.KindEntryNotFound, "Entry was not found")
}

func (repository *durableEntryReadRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
}

func (repository *durableEntryReadRepository) GetEnvironmentComposeProjectionRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjectionRevision(ctx, environmentID, revisionID)
}

func (repository *durableEntryReadRepository) ListEntries(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[entryrecord.Record], error) {
	return repository.entries.ListEntries(ctx, environmentID, request)
}

func (repository *durableEntryReadRepository) GetSecretEntryValue(
	ctx context.Context,
	entryID string,
	generationID string,
) (entryvalues.SecretGeneration, bool, error) {
	return repository.values.GetSecret(ctx, entryID, generationID)
}
