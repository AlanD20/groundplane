package composerender

import (
	releasedomain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"sort"
)

func bindServiceProxyImage(
	image *releasedomain.ProxyImage,
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
