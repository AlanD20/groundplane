package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumServiceLifecycleRenderInputBytes = 256 * 1024
	serviceLifecycleRenderInputPrefix       = "/v1/records/service-lifecycle-render-inputs/"
)

// ServiceLifecycleRenderInput is the immutable render snapshot owned by one
// applied Service lifecycle Task attempt. It prevents queued and retried work
// from observing a newer Blueprint, hierarchy label, or applied projection.
type ServiceLifecycleRenderInput struct {
	PlanID              string                       `json:"plan_id"`
	ServiceID           string                       `json:"service_id"`
	TenantID            string                       `json:"tenant_id"`
	TenantSlug          string                       `json:"tenant_slug"`
	ProjectID           string                       `json:"project_id"`
	ProjectSlug         string                       `json:"project_slug"`
	EnvironmentID       string                       `json:"environment_id"`
	EnvironmentName     string                       `json:"environment_name"`
	AuthorizedVolumeDir string                       `json:"authorized_volume_dir"`
	ArtifactID          string                       `json:"artifact_id"`
	Projection          EnvironmentComposeProjection `json:"projection"`
}

func serviceLifecycleRenderInputKey(taskID string) string {
	return serviceLifecycleRenderInputPrefix + taskID
}

func (repository *ServiceRepository) GetServiceLifecycleRenderInput(
	ctx context.Context,
	taskID string,
) (Versioned[ServiceLifecycleRenderInput], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ServiceLifecycleRenderInput]{}, false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return Versioned[ServiceLifecycleRenderInput]{}, false, errs.New(
			errs.KindValidationFailed,
			"Service lifecycle render input Task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, serviceLifecycleRenderInputKey(taskID))
	if err != nil {
		return Versioned[ServiceLifecycleRenderInput]{}, false, err
	}
	if result == nil {
		return Versioned[ServiceLifecycleRenderInput]{}, false, errs.New(
			errs.KindInternal,
			"Service lifecycle render input read is empty",
		)
	}
	if result.Entry == nil {
		return Versioned[ServiceLifecycleRenderInput]{ReadRevision: result.ReadRevision}, false, nil
	}
	input, err := decodeServiceLifecycleRenderInput(result.Entry.Value)
	if err != nil {
		return Versioned[ServiceLifecycleRenderInput]{}, false, err
	}
	return Versioned[ServiceLifecycleRenderInput]{
		Record: input, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func encodeServiceLifecycleRenderInput(input ServiceLifecycleRenderInput) ([]byte, error) {
	if err := validateServiceLifecycleRenderInput(input); err != nil {
		return nil, err
	}
	value, err := encodeEnvelope("service_lifecycle_render_input", input)
	if err != nil {
		return nil, err
	}
	if len(value) > MaximumServiceLifecycleRenderInputBytes {
		clear(value)
		return nil, errs.New(errs.KindValidationFailed, "Service lifecycle render input exceeds size limit")
	}
	return value, nil
}

func decodeServiceLifecycleRenderInput(value []byte) (ServiceLifecycleRenderInput, error) {
	input, err := decodeEnvelope[ServiceLifecycleRenderInput](value, "service_lifecycle_render_input")
	if err != nil || validateServiceLifecycleRenderInput(input) != nil {
		return ServiceLifecycleRenderInput{}, corruptServiceLifecycleRenderInput()
	}
	return cloneServiceLifecycleRenderInput(input), nil
}

func validateServiceLifecycleRenderInput(input ServiceLifecycleRenderInput) error {
	if ids.Validate(ids.KindPlan, input.PlanID) != nil ||
		ids.Validate(ids.KindService, input.ServiceID) != nil ||
		ids.Validate(ids.KindTenant, input.TenantID) != nil ||
		ids.Validate(ids.KindProject, input.ProjectID) != nil ||
		ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(ids.KindConfig, input.ArtifactID) != nil || input.TenantSlug == "" ||
		input.ProjectSlug == "" || input.EnvironmentName == "" || input.AuthorizedVolumeDir == "" {
		return errs.New(errs.KindValidationFailed, "Service lifecycle render input identity is invalid")
	}
	if validateEnvironmentComposeProjection(input.Projection) != nil ||
		input.Projection.EnvironmentID != input.EnvironmentID {
		return errs.New(errs.KindValidationFailed, "Service lifecycle render projection is invalid")
	}
	found := false
	for _, service := range input.Projection.Services {
		if service.ID == input.ServiceID {
			found = true
			break
		}
	}
	if !found {
		return errs.New(errs.KindValidationFailed, "Service lifecycle render projection does not contain Service")
	}
	return nil
}

func cloneServiceLifecycleRenderInput(source ServiceLifecycleRenderInput) ServiceLifecycleRenderInput {
	clone := source
	clone.Projection = cloneEnvironmentComposeProjection(source.Projection)
	return clone
}

func corruptServiceLifecycleRenderInput() error {
	return errs.New(errs.KindInternal, "Service lifecycle render input is corrupt")
}
