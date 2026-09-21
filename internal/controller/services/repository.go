package services

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type serviceMutationRepository interface {
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironmentBlueprintHead(context.Context, string) (etcdstore.Versioned[blueprints.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	ListServices(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[servicerecord.ServiceRecord], error)
	ListZones(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[zonerecord.Record], error)
	ClaimEnvironmentBlueprintStage(
		context.Context,
		etcd.EnvironmentBlueprintStageClaimRequest,
	) (etcd.EnvironmentBlueprintStageClaim, error)
	StageEnvironmentBlueprintRevision(
		context.Context,
		etcd.EnvironmentBlueprintStageRequest,
	) (etcd.EnvironmentBlueprintSeal, error)
	PublishEnvironmentServiceDesiredRevisionDirect(
		context.Context,
		etcd.EnvironmentServiceDesiredPublication,
	) (etcd.IdempotencyTransactionResult, error)
	ValidateServiceRemovalReferences(
		context.Context,
		etcdstore.Versioned[servicerecord.ServiceRecord],
		etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	) error
	BeginServiceRemovalWithTask(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.TenantRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[servicerecord.ServiceRecord],
		etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
		deletionrecord.DeletionTombstoneRecord,
		etcd.ServiceRemovalIntent,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type MutationRepository struct {
	hierarchy *etcd.HierarchyRepository
	services  *etcd.ServiceRepository
	zones     *etcd.ZoneRepository
	desired   *desiredrevisionstore.Repository
	releases  *etcd.ReleaseLedger
}

func NewMutationRepository(
	hierarchy *etcd.HierarchyRepository,
	services *etcd.ServiceRepository,
	zones *etcd.ZoneRepository,
	desired *desiredrevisionstore.Repository,
	releases *etcd.ReleaseLedger,
) (*MutationRepository, error) {
	if hierarchy == nil || services == nil || zones == nil || desired == nil || releases == nil {
		return nil, errs.New(errs.KindInternal, "Service mutation repositories are not configured")
	}
	return &MutationRepository{
		hierarchy: hierarchy,
		services:  services,
		zones:     zones,
		desired:   desired,
		releases:  releases,
	}, nil
}

func (repository *MutationRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *MutationRepository) GetProject(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *MutationRepository) GetEnvironmentBlueprintHead(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[blueprints.EnvironmentBlueprintHead], bool, error) {
	return repository.hierarchy.GetEnvironmentBlueprintHead(ctx, environmentID)
}

func (repository *MutationRepository) GetService(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	return repository.services.GetService(ctx, id)
}

func (repository *MutationRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[servicerecord.ServiceRecord], error) {
	return repository.services.ListServices(ctx, environmentID, request)
}

func (repository *MutationRepository) ListZones(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[zonerecord.Record], error) {
	return repository.zones.ListZones(ctx, environmentID, request)
}

func (repository *MutationRepository) ClaimEnvironmentBlueprintStage(
	ctx context.Context,
	request etcd.EnvironmentBlueprintStageClaimRequest,
) (etcd.EnvironmentBlueprintStageClaim, error) {
	return repository.desired.ClaimEnvironmentBlueprintStage(ctx, request)
}

func (repository *MutationRepository) StageEnvironmentBlueprintRevision(
	ctx context.Context,
	request etcd.EnvironmentBlueprintStageRequest,
) (etcd.EnvironmentBlueprintSeal, error) {
	return repository.desired.StageEnvironmentBlueprintRevision(ctx, request)
}

func (repository *MutationRepository) PublishEnvironmentServiceDesiredRevisionDirect(
	ctx context.Context,
	input etcd.EnvironmentServiceDesiredPublication,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.hierarchy.PublishEnvironmentServiceDesiredRevisionDirect(ctx, input)
}

func (repository *MutationRepository) ValidateServiceRemovalReferences(
	ctx context.Context,
	current etcdstore.Versioned[servicerecord.ServiceRecord],
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
) error {
	return repository.services.ValidateServiceRemovalReferences(ctx, current, projection)
}

func (repository *MutationRepository) BeginServiceRemovalWithTask(
	ctx context.Context,
	tenant etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	current etcdstore.Versioned[servicerecord.ServiceRecord],
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	tombstone deletionrecord.DeletionTombstoneRecord,
	intent etcd.ServiceRemovalIntent,
	task etcd.TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.services.BeginServiceRemovalWithTask(
		ctx, tenant, project, environment, current, projection, tombstone, intent, task, marker,
	)
}
