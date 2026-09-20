package services

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type serviceLifecycleRepository interface {
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetService(context.Context, string) (etcdstore.Versioned[etcd.ServiceRecord], error)
	GetEnvironmentAppliedComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	ResolveServing(context.Context, string, string, int64) (etcd.ServingRelease, error)
	GetReleaseRenderInputAt(context.Context, string, int64) (etcdstore.Versioned[etcd.ReleaseRenderInput], error)
	BeginServiceLifecycleWithTaskHookInputs(
		context.Context,
		*etcdstore.Versioned[hierarchyrecord.TenantRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[etcd.ServiceRecord],
		etcd.ServiceRecord,
		*etcdstore.Versioned[etcd.EnvironmentComposeProjection],
		*etcd.ServiceLifecycleRenderInput,
		*etcd.BackingHookEncryptedInputs,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

func (repository *durableServiceMutationRepository) GetTenant(
	ctx context.Context,
	tenantID string,
) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error) {
	return repository.hierarchy.GetTenant(ctx, tenantID)
}

func (repository *durableServiceMutationRepository) GetEnvironmentAppliedComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentAppliedComposeProjection(ctx, environmentID)
}

func (repository *durableServiceMutationRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
}

func (repository *durableServiceMutationRepository) ResolveServing(
	ctx context.Context,
	environmentID string,
	serviceID string,
	revision int64,
) (etcd.ServingRelease, error) {
	return repository.releases.ResolveServing(ctx, environmentID, serviceID, revision)
}

func (repository *durableServiceMutationRepository) GetReleaseRenderInputAt(
	ctx context.Context,
	releaseID string,
	revision int64,
) (etcdstore.Versioned[etcd.ReleaseRenderInput], error) {
	return repository.releases.GetReleaseRenderInputAt(ctx, releaseID, revision)
}

func (repository *durableServiceMutationRepository) BeginServiceLifecycleWithTaskHookInputs(
	ctx context.Context,
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	current etcdstore.Versioned[etcd.ServiceRecord],
	replacement etcd.ServiceRecord,
	projection *etcdstore.Versioned[etcd.EnvironmentComposeProjection],
	renderInput *etcd.ServiceLifecycleRenderInput,
	hookInputs *etcd.BackingHookEncryptedInputs,
	task etcd.TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.services.BeginServiceLifecycleWithTaskHookInputs(
		ctx, tenant, project, environment, current, replacement, projection, renderInput, hookInputs, task, marker,
	)
}
