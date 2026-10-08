package releaserender

import (
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumServiceLifecycleRenderInputBytes = 640 * 1024
	serviceLifecycleRenderInputPrefix       = "/v1/records/service-lifecycle-render-inputs/"
)

// ServiceLifecycleRenderInput is the immutable render snapshot owned by one
// applied Service lifecycle Task attempt. It prevents queued and retried work
// from observing a newer Blueprint, hierarchy label, or applied projection.
type ServiceLifecycleRenderInput struct {
	PlanID                      string                                         `json:"plan_id"`
	ServiceID                   string                                         `json:"service_id"`
	TenantID                    string                                         `json:"tenant_id"`
	TenantSlug                  string                                         `json:"tenant_slug"`
	ProjectID                   string                                         `json:"project_id"`
	ProjectSlug                 string                                         `json:"project_slug"`
	EnvironmentID               string                                         `json:"environment_id"`
	EnvironmentName             string                                         `json:"environment_name"`
	AuthorizedVolumeDir         string                                         `json:"authorized_volume_dir"`
	ArtifactID                  string                                         `json:"artifact_id"`
	AdapterKey                  string                                         `json:"adapter_key,omitempty"`
	HookConfiguration           *backinghook.Configuration                     `json:"hook_configuration,omitempty"`
	NativeBacking               *projectionrecord.EnvironmentComposeProjection `json:"native_backing,omitempty"`
	AppliedRenderGeneration     uint64                                         `json:"applied_render_generation,omitempty"`
	AppliedProjectionRevision   int64                                          `json:"applied_projection_revision"`
	Release                     *ServiceLifecycleRelease                       `json:"release,omitempty"`
	AcknowledgedRuntime         *serviceruntimerecord.Record                   `json:"acknowledged_runtime"`
	AcknowledgedRuntimeRevision int64                                          `json:"acknowledged_runtime_revision"`
}

// ServiceLifecycleRelease freezes the exact serving runtime sources used by a
// lifecycle Task. Rebuild and retry never consult a later release projection.
type ServiceLifecycleRelease struct {
	ServingReleaseID            string              `json:"serving_release_id"`
	PriorServingReleaseID       string              `json:"prior_serving_release_id,omitempty"`
	ProjectionRevision          int64               `json:"projection_revision"`
	IntentRevision              int64               `json:"intent_revision"`
	RenderRevision              int64               `json:"render_revision"`
	Current                     ReleaseRenderInput  `json:"current"`
	RetainedPrior               *ReleaseRenderInput `json:"retained_prior,omitempty"`
	RetainedPriorRenderRevision int64               `json:"retained_prior_render_revision,omitempty"`
}

func ServiceLifecycleRenderInputKey(taskID string) string {
	return serviceLifecycleRenderInputPrefix + taskID
}

func EncodeServiceLifecycleRenderInput(input ServiceLifecycleRenderInput) ([]byte, error) {
	if err := ValidateServiceLifecycleRenderInput(input); err != nil {
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

func DecodeServiceLifecycleRenderInput(value []byte) (ServiceLifecycleRenderInput, error) {
	input, err := recordcodec.Decode[ServiceLifecycleRenderInput](value, "service_lifecycle_render_input")
	if err != nil || ValidateServiceLifecycleRenderInput(input) != nil {
		return ServiceLifecycleRenderInput{}, corruptServiceLifecycleRenderInput()
	}
	return cloneServiceLifecycleRenderInput(input), nil
}

func ValidateServiceLifecycleRenderInput(input ServiceLifecycleRenderInput) error {
	if input.NativeBacking == nil &&
		(input.Release == nil || input.AcknowledgedRuntime == nil || input.AcknowledgedRuntimeRevision <= 0 ||
			serviceruntimerecord.Validate(*input.AcknowledgedRuntime) != nil ||
			input.AcknowledgedRuntime.EnvironmentID != input.EnvironmentID ||
			input.AcknowledgedRuntime.Runtime.ServiceID != input.ServiceID ||
			input.AcknowledgedRuntime.Runtime.ReleaseID != input.Release.ServingReleaseID ||
			input.AcknowledgedRuntime.Runtime.Target != string(input.Release.Current.CandidateTarget) ||
			(len(input.AcknowledgedRuntime.Runtime.RetainedPriorArtifact) != 0) != (input.Release.RetainedPrior != nil)) {
		return errs.New(errs.KindValidationFailed, "Service lifecycle acknowledged runtime is invalid")
	}
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
	if input.AppliedProjectionRevision <= 0 || input.RenderGeneration() == 0 {
		return errs.New(errs.KindValidationFailed, "Service lifecycle render projection is invalid")
	}
	if input.NativeBacking != nil {
		if input.AcknowledgedRuntime != nil || input.AcknowledgedRuntimeRevision != 0 ||
			input.Release != nil || input.TenantID != "" || input.HookConfiguration != nil || input.AdapterKey != "" ||
			input.NativeBacking.EnvironmentID != input.EnvironmentID ||
			input.NativeBacking.RenderGeneration != input.AppliedRenderGeneration ||
			projectionrecord.ValidateEnvironmentComposeProjection(*input.NativeBacking) != nil {
			return errs.New(errs.KindValidationFailed, "Service lifecycle native Backing authority is invalid")
		}
		artifact, _, err := projectionrecord.SelectBackingRuntime(*input.NativeBacking, input.ServiceID)
		if err != nil {
			return err
		}
		if artifact.ArtifactId != input.ArtifactID {
			return errs.New(errs.KindValidationFailed, "Service lifecycle native Backing artifact changed")
		}
		return nil
	}
	if err := ValidateServiceLifecycleRelease(*input.Release, input); err != nil {
		return err
	}
	return nil
}

func (input ServiceLifecycleRenderInput) RenderGeneration() uint64 {
	return input.AppliedRenderGeneration
}

func (input ServiceLifecycleRenderInput) RuntimeMemberCount() int {
	count := 1
	if input.Release != nil && len(input.Release.Current.ProxyPorts) != 0 {
		count++
	}
	if input.Release != nil && input.Release.RetainedPrior != nil {
		count++
	}
	return count
}

func ValidateServiceLifecycleRelease(authority ServiceLifecycleRelease, input ServiceLifecycleRenderInput) error {
	if ids.Validate(ids.KindDeployment, authority.ServingReleaseID) != nil ||
		authority.ProjectionRevision <= 0 || authority.IntentRevision <= 0 || authority.RenderRevision <= 0 ||
		ValidateReleaseRenderInput(authority.Current) != nil ||
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
		ValidateReleaseRenderInput(*prior) != nil || prior.ReleaseID != authority.PriorServingReleaseID ||
		prior.ServiceID != input.ServiceID || prior.EnvironmentID != input.EnvironmentID ||
		authority.Current.Strategy != domain.StrategyBlueGreen ||
		authority.Current.PriorStrategy != domain.StrategyBlueGreen ||
		prior.CandidateTarget != authority.Current.PriorTarget {
		return errs.New(errs.KindValidationFailed, "Service lifecycle retained Release authority is invalid")
	}
	return nil
}

func cloneServiceLifecycleRenderInput(source ServiceLifecycleRenderInput) ServiceLifecycleRenderInput {
	clone := source
	clone.HookConfiguration = backinghook.CloneConfiguration(source.HookConfiguration)
	if source.NativeBacking != nil {
		projection := projectionrecord.CloneEnvironmentComposeProjection(*source.NativeBacking)
		clone.NativeBacking = &projection
	}
	if source.Release != nil {
		release := *source.Release
		release.Current = CloneReleaseRenderInput(source.Release.Current)
		if source.Release.RetainedPrior != nil {
			prior := CloneReleaseRenderInput(*source.Release.RetainedPrior)
			release.RetainedPrior = &prior
		}
		clone.Release = &release
	}
	if source.AcknowledgedRuntime != nil {
		runtime := *source.AcknowledgedRuntime
		runtime.Runtime.CurrentArtifact = append([]byte(nil), runtime.Runtime.CurrentArtifact...)
		runtime.Runtime.RetainedPriorArtifact = append([]byte(nil), runtime.Runtime.RetainedPriorArtifact...)
		runtime.Runtime.ProxyConfigSHA256 = append([]byte(nil), runtime.Runtime.ProxyConfigSHA256...)
		clone.AcknowledgedRuntime = &runtime
	}
	return clone
}

func corruptServiceLifecycleRenderInput() error {
	return errs.New(errs.KindInternal, "Service lifecycle render input is corrupt")
}
