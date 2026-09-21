package etcd

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
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
	PlanID                    string                                        `json:"plan_id"`
	ServiceID                 string                                        `json:"service_id"`
	TenantID                  string                                        `json:"tenant_id"`
	TenantSlug                string                                        `json:"tenant_slug"`
	ProjectID                 string                                        `json:"project_id"`
	ProjectSlug               string                                        `json:"project_slug"`
	EnvironmentID             string                                        `json:"environment_id"`
	EnvironmentName           string                                        `json:"environment_name"`
	AuthorizedVolumeDir       string                                        `json:"authorized_volume_dir"`
	ArtifactID                string                                        `json:"artifact_id"`
	AdapterKey                string                                        `json:"adapter_key,omitempty"`
	HookConfiguration         *backinghook.Configuration                    `json:"hook_configuration,omitempty"`
	Projection                projectionrecord.EnvironmentComposeProjection `json:"projection"`
	AppliedProjectionRevision int64                                         `json:"applied_projection_revision"`
	Release                   ServiceLifecycleRelease                       `json:"release"`
}

// ServiceLifecycleRelease freezes the exact serving runtime sources used by a
// lifecycle Task. Rebuild and retry never consult a later release projection.
type ServiceLifecycleRelease struct {
	ServingReleaseID            string                            `json:"serving_release_id"`
	PriorServingReleaseID       string                            `json:"prior_serving_release_id,omitempty"`
	ProjectionRevision          int64                             `json:"projection_revision"`
	IntentRevision              int64                             `json:"intent_revision"`
	RenderRevision              int64                             `json:"render_revision"`
	Current                     releaserender.ReleaseRenderInput  `json:"current"`
	RetainedPrior               *releaserender.ReleaseRenderInput `json:"retained_prior,omitempty"`
	RetainedPriorRenderRevision int64                             `json:"retained_prior_render_revision,omitempty"`
}

func serviceLifecycleRenderInputKey(taskID string) string {
	return serviceLifecycleRenderInputPrefix + taskID
}

func (repository *ServiceRepository) GetServiceLifecycleRenderInput(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[ServiceLifecycleRenderInput], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[ServiceLifecycleRenderInput]{}, false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[ServiceLifecycleRenderInput]{}, false, errs.New(
			errs.KindValidationFailed,
			"Service lifecycle render input Task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, serviceLifecycleRenderInputKey(taskID))
	if err != nil {
		return etcdstore.Versioned[ServiceLifecycleRenderInput]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[ServiceLifecycleRenderInput]{}, false, errs.New(
			errs.KindInternal,
			"Service lifecycle render input read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[ServiceLifecycleRenderInput]{ReadRevision: result.ReadRevision}, false, nil
	}
	input, err := decodeServiceLifecycleRenderInput(result.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[ServiceLifecycleRenderInput]{}, false, err
	}
	return etcdstore.Versioned[ServiceLifecycleRenderInput]{
		Record: input, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func encodeServiceLifecycleRenderInput(input ServiceLifecycleRenderInput) ([]byte, error) {
	if err := validateServiceLifecycleRenderInput(input); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("service_lifecycle_render_input", input)
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
	input, err := recordcodec.Decode[ServiceLifecycleRenderInput](value, "service_lifecycle_render_input")
	if err != nil || validateServiceLifecycleRenderInput(input) != nil {
		return ServiceLifecycleRenderInput{}, corruptServiceLifecycleRenderInput()
	}
	return cloneServiceLifecycleRenderInput(input), nil
}

func validateServiceLifecycleRenderInput(input ServiceLifecycleRenderInput) error {
	if ids.Validate(ids.KindPlan, input.PlanID) != nil ||
		ids.Validate(ids.KindService, input.ServiceID) != nil ||
		ids.Validate(ids.KindProject, input.ProjectID) != nil ||
		ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(ids.KindConfig, input.ArtifactID) != nil ||
		input.ProjectSlug == "" || input.EnvironmentName == "" || input.AuthorizedVolumeDir == "" {
		return errs.New(errs.KindValidationFailed, "Service lifecycle render input identity is invalid")
	}
	if (input.TenantID == "") != (input.TenantSlug == "") ||
		input.TenantID != "" && ids.Validate(ids.KindTenant, input.TenantID) != nil {
		return errs.New(errs.KindValidationFailed, "Service lifecycle render input Tenant identity is invalid")
	}
	if input.HookConfiguration != nil {
		if input.AdapterKey != "custom" || backinghook.ValidateConfiguration(*input.HookConfiguration) != nil {
			return errs.New(errs.KindValidationFailed, "Service lifecycle hook configuration is invalid")
		}
	}
	if projectionrecord.ValidateEnvironmentComposeProjection(input.Projection) != nil ||
		input.Projection.EnvironmentID != input.EnvironmentID || input.AppliedProjectionRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "Service lifecycle render projection is invalid")
	}
	found := false
	for _, service := range input.Projection.DesiredServices {
		if service.Desired.ID == input.ServiceID {
			found = true
			break
		}
	}
	if !found {
		return errs.New(errs.KindValidationFailed, "Service lifecycle render projection does not contain Service")
	}
	if err := validateServiceLifecycleRelease(input.Release, input); err != nil {
		return err
	}
	return nil
}

func validateServiceLifecycleRelease(authority ServiceLifecycleRelease, input ServiceLifecycleRenderInput) error {
	if ids.Validate(ids.KindDeployment, authority.ServingReleaseID) != nil ||
		authority.ProjectionRevision <= 0 || authority.IntentRevision <= 0 || authority.RenderRevision <= 0 ||
		releaserender.ValidateReleaseRenderInput(authority.Current) != nil ||
		authority.Current.ReleaseID != authority.ServingReleaseID || authority.Current.ServiceID != input.ServiceID ||
		authority.Current.EnvironmentID != input.EnvironmentID {
		return errs.New(errs.KindValidationFailed, "Service lifecycle serving Release authority is invalid")
	}
	retained := authority.RetainedPrior != nil
	if retained != (authority.RetainedPriorRenderRevision > 0) || retained != (authority.PriorServingReleaseID != "") {
		return errs.New(errs.KindValidationFailed, "Service lifecycle retained Release authority is partial")
	}
	if !retained {
		return nil
	}
	prior := authority.RetainedPrior
	if ids.Validate(ids.KindDeployment, authority.PriorServingReleaseID) != nil ||
		releaserender.ValidateReleaseRenderInput(*prior) != nil || prior.ReleaseID != authority.PriorServingReleaseID ||
		prior.ServiceID != input.ServiceID || prior.EnvironmentID != input.EnvironmentID ||
		authority.Current.Strategy != domain.StrategyBlueGreen ||
		authority.Current.PriorStrategy != domain.StrategyBlueGreen ||
		authority.Current.PriorArtifactID != "" || prior.CandidateTarget != authority.Current.PriorTarget {
		return errs.New(errs.KindValidationFailed, "Service lifecycle retained Release authority is invalid")
	}
	return nil
}

func cloneServiceLifecycleRenderInput(source ServiceLifecycleRenderInput) ServiceLifecycleRenderInput {
	clone := source
	clone.HookConfiguration = backinghook.CloneConfiguration(source.HookConfiguration)
	clone.Projection = projectionrecord.CloneEnvironmentComposeProjection(source.Projection)
	clone.Release.Current = releaserender.CloneReleaseRenderInput(source.Release.Current)
	if source.Release.RetainedPrior != nil {
		prior := releaserender.CloneReleaseRenderInput(*source.Release.RetainedPrior)
		clone.Release.RetainedPrior = &prior
	}
	return clone
}

func corruptServiceLifecycleRenderInput() error {
	return errs.New(errs.KindInternal, "Service lifecycle render input is corrupt")
}
