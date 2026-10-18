package services

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"time"
)

type serviceLifecycleRepository interface {
	ResolveLifecycleHost(context.Context) (workexecution.Placement, error)
	GetServiceRemovalRuntime(
		context.Context,
		string,
		string,
		int64,
	) (etcdstore.Versioned[serviceruntimerecord.Record], error)
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	GetEnvironmentAppliedComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	ResolveServing(context.Context, string, string, int64) (releasequeries.ServingRelease, error)
	GetReleaseRenderInputAt(
		context.Context,
		string,
		int64,
	) (etcdstore.Versioned[releaserender.ReleaseRenderInput], error)
	PublishServiceLifecycle(
		context.Context,
		*etcdstore.Versioned[hierarchyrecord.TenantRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[servicerecord.ServiceRecord],
		servicelifecyclework.Input,
		*taskconfiguration.BackingHookEncryptedInputs,
		time.Time,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

func (repository *MutationRepository) GetTenant(
	ctx context.Context,
	tenantID string,
) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error) {
	return repository.hierarchy.GetTenant(ctx, tenantID)
}

func (repository *MutationRepository) GetEnvironmentAppliedComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentAppliedComposeProjection(ctx, environmentID)
}

func (repository *MutationRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
}

func (repository *MutationRepository) ResolveServing(
	ctx context.Context,
	environmentID string,
	serviceID string,
	revision int64,
) (releasequeries.ServingRelease, error) {
	return repository.releases.ResolveServing(ctx, environmentID, serviceID, revision)
}

func (repository *MutationRepository) GetReleaseRenderInputAt(
	ctx context.Context,
	releaseID string,
	revision int64,
) (etcdstore.Versioned[releaserender.ReleaseRenderInput], error) {
	return repository.releases.GetReleaseRenderInputAt(ctx, releaseID, revision)
}

func (repository *MutationRepository) ResolveLifecycleHost(ctx context.Context) (workexecution.Placement, error) {
	return repository.hierarchy.ResolveResourceHost(ctx)
}

func (repository *MutationRepository) PublishServiceLifecycle(
	ctx context.Context,
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	current etcdstore.Versioned[servicerecord.ServiceRecord],
	input servicelifecyclework.Input,
	hookInputs *taskconfiguration.BackingHookEncryptedInputs,
	deadline time.Time,
	marker idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.services.PublishServiceLifecycle(
		ctx, tenant, project, environment, current, input, hookInputs, deadline, marker,
	)
}
