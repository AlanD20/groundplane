package services

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
)

type serviceLifecycleRepository interface {
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
	BeginServiceLifecycleWithTaskHookInputs(
		context.Context,
		*etcdstore.Versioned[hierarchyrecord.TenantRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[servicerecord.ServiceRecord],
		servicerecord.ServiceRecord,
		*etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
		*releaserender.ServiceLifecycleRenderInput,
		*taskconfiguration.BackingHookEncryptedInputs,
		etcd.TaskRecord,
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

func (repository *MutationRepository) BeginServiceLifecycleWithTaskHookInputs(
	ctx context.Context,
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	current etcdstore.Versioned[servicerecord.ServiceRecord],
	replacement servicerecord.ServiceRecord,
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	renderInput *releaserender.ServiceLifecycleRenderInput,
	hookInputs *taskconfiguration.BackingHookEncryptedInputs,
	task etcd.TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.services.BeginServiceLifecycleWithTaskHookInputs(
		ctx, tenant, project, environment, current, replacement, projection, renderInput, hookInputs, task, marker,
	)
}
