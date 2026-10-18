package dnsresolverobserver

import (
	"crypto/sha256"
	"encoding/hex"
	"runtime"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CapturedRequest binds compiled observation parameters to one immutable
// platform runtime. It has no Task/plan reader or mutable intent lookup.
func CapturedRequest(artifact *agentpb.ComposeArtifact, action *agentpb.ComponentApply,
	recipe registeredcatalog.DNSResolverObservationRecipe,
) (Request, error) {
	if artifact == nil || action == nil || artifact.ProjectName == "" || len(artifact.Services) != 1 || artifact.Services[0] == nil {
		return Request{}, errs.New(errs.KindValidationFailed, "DNS resolver observation requires one captured runtime")
	}
	service := artifact.Services[0]
	image := recipe.Image()
	platform, reference, selected := image.Select(runtime.GOOS, runtime.GOARCH)
	if !selected || service.ComposeName != recipe.ServiceName() || service.ServiceId == "" ||
		service.ImageReference != reference || service.ImageRepository != image.Repository ||
		hex.EncodeToString(service.ImageIndexDigest) != image.IndexDigest ||
		hex.EncodeToString(service.ImageChildDigest) != platform.ChildDigest ||
		hex.EncodeToString(service.ImageConfigDigest) != platform.ConfigDigest ||
		service.ImageOs != platform.OS || service.ImageArchitecture != platform.Architecture || service.ImageVariant != platform.Variant ||
		len(action.ArtifactDigest) != sha256.Size {
		return Request{}, errs.New(errs.KindValidationFailed, "DNS resolver observation image or Service differs from the compiled recipe")
	}
	labels := make(map[string]string, len(service.ExpectedLabels))
	for _, label := range service.ExpectedLabels {
		if label == nil || label.Key == "" {
			return Request{}, errs.New(errs.KindValidationFailed, "DNS resolver ownership label is missing")
		}
		if _, exists := labels[label.Key]; exists {
			return Request{}, errs.New(errs.KindValidationFailed, "DNS resolver ownership label is repeated")
		}
		labels[label.Key] = label.Value
	}
	var digest, imageIndex, imageConfig [sha256.Size]byte
	copy(digest[:], action.ArtifactDigest)
	copy(imageIndex[:], service.ImageIndexDigest)
	copy(imageConfig[:], service.ImageConfigDigest)
	request := Request{ComponentID: action.ComponentId, ServiceID: service.ServiceId, ArtifactID: action.ArtifactId,
		ArtifactSHA256: digest, RenderGeneration: action.Generation, ProjectName: artifact.ProjectName, ServiceName: recipe.ServiceName(),
		ArtifactTarget: recipe.ArtifactTarget(), ImageReference: reference, ImageRepository: service.ImageRepository,
		ImageIndexDigest: imageIndex, ImageConfigDigest: imageConfig, ImageOS: platform.OS, ImageArchitecture: platform.Architecture, ImageVariant: platform.Variant,
		ListenEndpoint: recipe.ListenEndpoint(), MetricsURL: recipe.MetricsURL(), ReloadMetric: recipe.ReloadMetric(), ExpectedLabels: labels}
	return request, validateRequest(request)
}
