package services

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type serviceLifecycleRepository interface {
	GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	GetEnvironmentAppliedComposeProjection(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	ResolveServing(context.Context, string, string, int64) (etcd.ServingRelease, error)
	GetReleaseRenderInputAt(context.Context, string, int64) (etcd.Versioned[etcd.ReleaseRenderInput], error)
	BeginServiceLifecycleWithTaskHookInputs(
		context.Context,
		*etcd.Versioned[etcd.TenantRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ServiceRecord],
		etcd.ServiceRecord,
		*etcd.Versioned[etcd.EnvironmentComposeProjection],
		*etcd.ServiceLifecycleRenderInput,
		*etcd.BackingHookEncryptedInputs,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

func (repository *durableServiceMutationRepository) GetTenant(
	ctx context.Context,
	tenantID string,
) (etcd.Versioned[etcd.TenantRecord], error) {
	return repository.hierarchy.GetTenant(ctx, tenantID)
}

func (repository *durableServiceMutationRepository) GetEnvironmentAppliedComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentAppliedComposeProjection(ctx, environmentID)
}

func (repository *durableServiceMutationRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
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
) (etcd.Versioned[etcd.ReleaseRenderInput], error) {
	return repository.releases.GetReleaseRenderInputAt(ctx, releaseID, revision)
}

func (repository *durableServiceMutationRepository) BeginServiceLifecycleWithTaskHookInputs(
	ctx context.Context,
	tenant *etcd.Versioned[etcd.TenantRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	environment etcd.Versioned[etcd.EnvironmentRecord],
	current etcd.Versioned[etcd.ServiceRecord],
	replacement etcd.ServiceRecord,
	projection *etcd.Versioned[etcd.EnvironmentComposeProjection],
	renderInput *etcd.ServiceLifecycleRenderInput,
	hookInputs *etcd.BackingHookEncryptedInputs,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.services.BeginServiceLifecycleWithTaskHookInputs(
		ctx, tenant, project, environment, current, replacement, projection, renderInput, hookInputs, task, marker,
	)
}
