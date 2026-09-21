package taskplanning

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	releasedomain "github.com/AlanD20/groundplane/internal/core/release"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"

	"github.com/AlanD20/groundplane/pkg/errs"
	"runtime"
)

// EnableServiceProxyImage supplies the composition root's compiled image asset.
// Controller and its singleton local Agent run on the same host in the MVP;
// execution independently checks this selected platform before starting it.
func (resolver *TaskPlanResolver) EnableServiceProxyImage(image componentsdk.OCIImage) error {
	platform, _, found := image.Select(runtime.GOOS, runtime.GOARCH)
	if resolver == nil || !found {
		return errs.New(errs.KindInternal, "compiled Service proxy image is unavailable for this host")
	}
	resolver.serviceProxyImage = &releasedomain.ProxyImage{
		Repository: image.Repository, IndexDigest: image.IndexDigest, Platform: platform,
	}
	return nil
}

// PrepareReleaseProxyImage freezes image selection before ledger staging.
// Existing proxies retain their exact historical image, never today's catalog.
func (resolver *TaskPlanResolver) PrepareReleaseProxyImage(
	render *releaserender.ReleaseRenderInput,
	prior *releaserender.ReleaseRenderInput,
) error {
	if render == nil || resolver == nil {
		return errs.New(errs.KindInternal, "release proxy preparation requires render inputs")
	}
	if len(render.ProxyPorts) == 0 {
		if render.ProxyImage != nil {
			return errs.New(errs.KindInternal, "portless release carries proxy image authority")
		}
		return nil
	}
	image := render.ProxyImage
	if prior != nil {
		image = prior.ProxyImage
		if prior.ServiceID != render.ServiceID || prior.EnvironmentID != render.EnvironmentID || image == nil {
			return errs.New(errs.KindStateConflict, "historical Service proxy image authority is absent")
		}
	} else if image == nil {
		if render.PriorArtifactID != "" {
			return errs.New(errs.KindStateConflict, "historical Service proxy image authority is absent")
		}
		image = resolver.serviceProxyImage
	}
	if image == nil || image.Validate() != nil {
		return errs.New(errs.KindInternal, "Service proxy image authority is unavailable")
	}
	frozen := *image
	render.ProxyImage = &frozen
	return nil
}
