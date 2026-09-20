package taskplanning

import (
	"runtime"
	"sort"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// EnableServiceProxyImage supplies the composition root's compiled image asset.
// Controller and its singleton local Agent run on the same host in the MVP;
// execution independently checks this selected platform before starting it.
func (resolver *TaskPlanResolver) EnableServiceProxyImage(image componentsdk.OCIImage) error {
	platform, _, found := image.Select(runtime.GOOS, runtime.GOARCH)
	if resolver == nil || !found {
		return errs.New(errs.KindInternal, "compiled Service proxy image is unavailable for this host")
	}
	resolver.serviceProxyImage = &etcd.ReleaseProxyImage{
		Repository: image.Repository, IndexDigest: image.IndexDigest, Platform: platform,
	}
	return nil
}

// PrepareReleaseProxyImage freezes image selection before ledger staging.
// Existing proxies retain their exact historical image, never today's catalog.
func (resolver *TaskPlanResolver) PrepareReleaseProxyImage(
	render *etcd.ReleaseRenderInput,
	prior *etcd.ReleaseRenderInput,
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

func bindServiceProxyImage(
	image *etcd.ReleaseProxyImage,
	native *composetypes.ServiceConfig,
	service *agentpb.ComposeService,
) error {
	if image == nil || image.Validate() != nil {
		return errs.New(errs.KindInternal, "Service proxy image authority is absent or invalid")
	}
	p := image.Platform
	native.Image = image.Reference()
	service.ImageReference, service.ImageRepository = native.Image, image.Repository
	service.ImageIndexDigest = mustDecodePlatformDigest(image.IndexDigest)
	service.ImageChildDigest = mustDecodePlatformDigest(p.ChildDigest)
	service.ImageConfigDigest = mustDecodePlatformDigest(p.ConfigDigest)
	service.ImageOs, service.ImageArchitecture, service.ImageVariant = p.OS, p.Architecture, p.Variant
	for key, value := range map[string]string{
		"com.groundplane.image-index-digest":  "sha256:" + image.IndexDigest,
		"com.groundplane.image-child-digest":  "sha256:" + p.ChildDigest,
		"com.groundplane.image-config-digest": "sha256:" + p.ConfigDigest,
		"com.groundplane.image-platform":      p.OS + "/" + p.Architecture + platformVariantSuffix(p.Variant),
	} {
		native.Labels[key] = value
		service.ExpectedLabels = append(service.ExpectedLabels, &agentpb.LabelPair{Key: key, Value: value})
	}
	sort.Slice(
		service.ExpectedLabels,
		func(i, j int) bool { return service.ExpectedLabels[i].Key < service.ExpectedLabels[j].Key },
	)
	return nil
}
