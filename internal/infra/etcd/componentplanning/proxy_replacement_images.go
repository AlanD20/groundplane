package componentplanning

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProxyReplacementImage is immutable image authority for a later Service
// release, not permission for the Component child to start that workload.
type ProxyReplacementImage struct {
	ServiceID string              `json:"service_id"`
	Workload  domain.WorkloadSeal `json:"workload"`
	Proxy     domain.ProxyImage   `json:"proxy"`
}

func (authority FrozenProjectionAuthority) ProxyReplacementServices() []string {
	var result []string
	if authority.proxyServicePreflight != nil {
		for _, source := range authority.proxyServicePreflight.Services {
			if source.ReplacementRequired {
				result = append(result, source.ServiceID)
			}
		}
	}
	return result
}

// ProxyServiceCapture returns an owned snapshot for pre-switch native planning.
// Publication still uses the authority's private source comparisons.
func (authority FrozenProjectionAuthority) ProxyServiceCapture() (ProxyServicePreflight, bool) {
	if authority.proxyServicePreflight == nil {
		return ProxyServicePreflight{}, false
	}
	return *cloneProxyServicePreflight(authority.proxyServicePreflight), true
}

func (authority FrozenProjectionAuthority) WithProxyReplacementImages(
	desired []core.Service, images []ProxyReplacementImage,
) (FrozenProjectionAuthority, error) {
	if authority.proxyServicePreflight == nil {
		return FrozenProjectionAuthority{}, errs.New(
			errs.KindValidationFailed,
			"proxy replacement has no frozen Service capture",
		)
	}
	requested := make(map[string]core.Service, len(desired))
	for _, service := range desired {
		requested[service.ID] = service
	}
	if len(images) != len(authority.ProxyReplacementServices()) {
		return FrozenProjectionAuthority{}, errs.New(
			errs.KindValidationFailed,
			"proxy replacement image membership is incomplete",
		)
	}
	preflight := cloneProxyServicePreflight(authority.proxyServicePreflight)
	for index, image := range images {
		service, found := requested[image.ServiceID]
		if !found || ids.Validate(ids.KindService, image.ServiceID) != nil ||
			index > 0 &&
				images[index-1].ServiceID >= image.ServiceID || domain.ValidateWorkloadSeal(image.Workload) != nil ||
			image.Proxy.Validate() != nil || image.Workload.RequestedReference != service.Image ||
			uint64(image.Workload.ReplicaCount) != uint64(service.Replicas) {
			return FrozenProjectionAuthority{}, errs.New(
				errs.KindValidationFailed,
				"proxy replacement image does not match its authored Service",
			)
		}
		ordinal := slices.IndexFunc(
			preflight.Services,
			func(source ProxyServiceSource) bool { return source.ServiceID == image.ServiceID },
		)
		if ordinal < 0 || !preflight.Services[ordinal].ReplacementRequired {
			return FrozenProjectionAuthority{}, errs.New(
				errs.KindValidationFailed,
				"proxy replacement image belongs to an unselected Service",
			)
		}
		preflight.Services[ordinal].Replacement = cloneProxyReplacementImage(&image)
	}
	if err := preflight.validate(); err != nil {
		return FrozenProjectionAuthority{}, err
	}
	authority.proxyServicePreflight = preflight
	return authority, nil
}

func (preflight ProxyServicePreflight) validate() error {
	if err := preflight.validateSources(); err != nil {
		return err
	}
	if input := preflight.CandidateInput; input != nil {
		if input.Validate() != nil || input.EnvironmentID != preflight.EnvironmentID ||
			input.ParentTaskID != preflight.ParentTaskID {
			return errs.New(errs.KindStateConflict, "prepared router Environment input belongs to another Apply")
		}
	}
	if hold := preflight.SourceHold; hold != nil && (hold.Validate() != nil || preflight.CandidateInput == nil) {
		return errs.New(errs.KindStateConflict, "pending proxy source hold has no captured Environment input")
	}
	for _, source := range preflight.Services {
		if source.Rollout != nil &&
			(!source.ReplacementRequired || source.Rollout.ServiceID != source.ServiceID || source.Rollout.Validate() != nil || preflight.CandidateInput == nil) {
			return errs.New(errs.KindStateConflict, "pre-switch Service rollout differs from its captured selection")
		}
		if source.ReplacementRequired != (source.Replacement != nil) {
			return errs.New(errs.KindStateConflict, "router Service replacement image capture is incomplete")
		}
		if image := source.Replacement; image != nil && (image.ServiceID != source.ServiceID ||
			domain.ValidateWorkloadSeal(image.Workload) != nil || image.Proxy.Validate() != nil) {
			return errs.New(errs.KindStateConflict, "router Service replacement image authority is invalid")
		}
	}
	return nil
}

func cloneProxyReplacementImage(image *ProxyReplacementImage) *ProxyReplacementImage {
	if image == nil {
		return nil
	}
	clone := *image
	return &clone
}
