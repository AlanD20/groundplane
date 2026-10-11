package taskplanning

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	releasedomain "github.com/AlanD20/groundplane/internal/core/release"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"

	"github.com/AlanD20/groundplane/pkg/errs"
	"runtime"
)

// EnableServiceProxyImages supplies the two closed compiled image assets.
// Controller and its singleton local Agent run on the same host in the MVP;
// execution independently checks this selected platform before starting it.
func (resolver *TaskPlanResolver) EnableServiceProxyImages(caddy, envoy componentsdk.OCIImage) error {
	if resolver == nil || caddy.Repository == envoy.Repository {
		return errs.New(errs.KindInternal, "compiled Service proxy image selection is invalid")
	}
	images := make(map[serviceproxy.Implementation]*releasedomain.ProxyImage, 2)
	for implementation, image := range map[serviceproxy.Implementation]componentsdk.OCIImage{
		serviceproxy.Caddy: caddy, serviceproxy.Envoy: envoy,
	} {
		platform, _, found := image.Select(runtime.GOOS, runtime.GOARCH)
		if !found {
			return errs.New(errs.KindInternal, "compiled Service proxy image is unavailable for this host")
		}
		images[implementation] = &releasedomain.ProxyImage{
			Repository: image.Repository, IndexDigest: image.IndexDigest, Platform: platform, SelectedTag: image.Tag,
		}
	}
	resolver.serviceProxyImages = images
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
	var previous *releasedomain.ProxySelection
	if prior != nil {
		if prior.ServiceID != render.ServiceID || prior.EnvironmentID != render.EnvironmentID {
			return errs.New(errs.KindStateConflict, "historical Service proxy belongs to another Service")
		}
		previous = &releasedomain.ProxySelection{Implementation: prior.ProxyImplementation, Ports: prior.ProxyPorts}
	} else if render.PriorArtifactID != "" {
		return errs.New(errs.KindStateConflict, "historical Service proxy selection is absent")
	}
	if err := releasedomain.ValidateProxyTransition(render.ServiceName, render.Strategy,
		releasedomain.ProxySelection{Implementation: render.ProxyImplementation, Ports: render.ProxyPorts}, previous); err != nil {
		return err
	}
	if len(render.ProxyPorts) == 0 {
		if render.ProxyImage != nil {
			return errs.New(errs.KindInternal, "portless release carries proxy image authority")
		}
		return nil
	}
	image := render.ProxyImage
	if prior != nil {
		if prior.ProxyImage == nil || prior.ProxyImage.Validate() != nil {
			return errs.New(errs.KindStateConflict, "historical Service proxy image authority is absent")
		}
		if prior.ProxyImplementation != render.ProxyImplementation {
			// Recreate removes the exact captured predecessor, applies the new
			// proxy/workload and proves completion. Recovery retains the old
			// artifact and image; it never rebuilds them from this new catalog.
			if image == nil {
				image = resolver.serviceProxyImages[render.ProxyImplementation]
			}
		} else {
			image = prior.ProxyImage
		}
	} else if image == nil {
		image = resolver.serviceProxyImages[render.ProxyImplementation]
	}
	if image == nil || image.Validate() != nil {
		return errs.New(errs.KindInternal, "Service proxy image authority is unavailable")
	}
	frozen := *image
	render.ProxyImage = &frozen
	return nil
}
