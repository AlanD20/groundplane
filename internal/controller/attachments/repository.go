package attachments

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type attachMutationRepository interface {
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	GetEnvironmentBlueprintHead(
		context.Context,
		string,
	) (etcdstore.Versioned[blueprints.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentBlueprintRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	GetEnvironmentComposeProjectionRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	GetAttach(context.Context, string) (etcdstore.Versioned[attachrecord.Record], error)
	GetAttachFactsVersioned(context.Context, etcdstore.Versioned[attachrecord.Record]) (etcdstore.Versioned[attachrecord.EncryptedFacts], bool, error)
	ListAttaches(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[attachrecord.Record], error)
	RenameAttachIdempotent(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[attachrecord.Record],
		string,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	PublishAttachWork(context.Context, etcd.AttachWorkPublication) (etcd.IdempotencyTransactionResult, error)
	CaptureDetachSnapshot(context.Context, etcdstore.Versioned[attachrecord.Record]) (attachremoval.DetachSnapshot, error)
	ResolveDetachHost(context.Context) (workexecution.Placement, error)
	PublishDetachWork(context.Context, etcd.DetachWorkPublication) (etcd.IdempotencyTransactionResult, error)
}

type durableAttachMutationRepository struct {
	hierarchy *etcd.HierarchyRepository
	services  *etcd.ServiceRepository
	attaches  *etcd.AttachRepository
}

func NewRepository(
	hierarchy *etcd.HierarchyRepository,
	services *etcd.ServiceRepository,
	attaches *etcd.AttachRepository,
) (*durableAttachMutationRepository, error) {
	if hierarchy == nil || services == nil || attaches == nil {
		return nil, errs.New(errs.KindInternal, "Attach mutation repositories are not configured")
	}
	return &durableAttachMutationRepository{hierarchy: hierarchy, services: services, attaches: attaches}, nil
}

func (repository *durableAttachMutationRepository) GetTenant(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error) {
	return repository.hierarchy.GetTenant(ctx, id)
}

func (repository *durableAttachMutationRepository) GetProject(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableAttachMutationRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableAttachMutationRepository) GetEnvironmentBlueprintRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision], bool, error) {
	return repository.hierarchy.GetEnvironmentBlueprintRevision(ctx, environmentID, revisionID)
}

func (repository *durableAttachMutationRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
}

func (repository *durableAttachMutationRepository) GetService(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	return repository.services.GetService(ctx, id)
}

func (repository *durableAttachMutationRepository) GetEnvironmentBlueprintHead(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[blueprints.EnvironmentBlueprintHead], bool, error) {
	return repository.hierarchy.GetEnvironmentBlueprintHead(ctx, environmentID)
}

func (repository *durableAttachMutationRepository) GetEnvironmentComposeProjectionRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjectionRevision(ctx, environmentID, revisionID)
}

func (repository *durableAttachMutationRepository) GetAttach(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[attachrecord.Record], error) {
	return repository.attaches.GetAttach(ctx, id)
}

func (repository *durableAttachMutationRepository) GetAttachFactsVersioned(ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
) (etcdstore.Versioned[attachrecord.EncryptedFacts], bool, error) {
	return repository.attaches.GetAttachFactsVersioned(ctx, current)
}

func (repository *durableAttachMutationRepository) ListAttaches(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[attachrecord.Record], error) {
	return repository.attaches.ListAttaches(ctx, environmentID, request)
}

func (repository *durableAttachMutationRepository) RenameAttachIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[attachrecord.Record],
	name string,
	marker idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.attaches.RenameAttachIdempotent(ctx, environment, project, current, name, marker)
}

func (repository *durableAttachMutationRepository) PublishAttachWork(ctx context.Context, request etcd.AttachWorkPublication) (etcd.IdempotencyTransactionResult, error) {
	return repository.attaches.PublishAttachWork(ctx, request)
}

func (repository *durableAttachMutationRepository) CaptureDetachSnapshot(ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
) (attachremoval.DetachSnapshot, error) {
	return repository.attaches.CaptureDetachSnapshot(ctx, current)
}

func (repository *durableAttachMutationRepository) ResolveDetachHost(ctx context.Context) (workexecution.Placement, error) {
	return repository.attaches.ResolveDetachHost(ctx)
}

func (repository *durableAttachMutationRepository) PublishDetachWork(ctx context.Context,
	request etcd.DetachWorkPublication,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.attaches.PublishDetachWork(ctx, request)
}
