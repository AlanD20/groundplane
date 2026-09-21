package componentaction

import (
	"crypto/sha256"
	"encoding/hex"
	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/internal/infra/docker/dnsresolverobserver"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"runtime"
)

func dnsResolverObservationRequest(
	assignment taskassignment.Assignment,
	action *agentpb.ComponentApply,
	recipe registeredcatalog.DNSResolverObservationRecipe,
) (dnsresolverobserver.Request, error) {
	artifact := componentObservationComposeArtifact(assignment.Plan)
	if artifact == nil {
		return dnsresolverobserver.Request{}, errs.New(
			errs.KindValidationFailed,
			"agent: DNS resolver observation artifact is invalid",
		)
	}
	if artifact.GetProjectName() == "" || len(artifact.GetServices()) != 1 {
		return dnsresolverobserver.Request{}, errs.New(
			errs.KindValidationFailed,
			"agent: DNS resolver observation target is invalid",
		)
	}
	service := artifact.GetServices()[0]
	image := recipe.Image()
	selectedPlatform, imageReference, selected := image.Select(runtime.GOOS, runtime.GOARCH)
	if service.GetComposeName() != recipe.ServiceName() || service.GetServiceId() == "" || !selected ||
		len(service.GetImageIndexDigest()) != sha256.Size || len(service.GetImageChildDigest()) != sha256.Size ||
		len(service.GetImageConfigDigest()) != sha256.Size || service.GetImageReference() != imageReference ||
		service.GetImageRepository() != image.Repository ||
		hex.EncodeToString(service.GetImageIndexDigest()) != image.IndexDigest ||
		hex.EncodeToString(service.GetImageChildDigest()) != selectedPlatform.ChildDigest ||
		hex.EncodeToString(service.GetImageConfigDigest()) != selectedPlatform.ConfigDigest ||
		service.GetImageOs() != selectedPlatform.OS ||
		service.GetImageArchitecture() != selectedPlatform.Architecture || service.GetImageVariant() != selectedPlatform.Variant {
		return dnsresolverobserver.Request{}, errs.New(
			errs.KindValidationFailed,
			"agent: DNS resolver observation Service is invalid",
		)
	}
	labels := make(map[string]string, len(service.GetExpectedLabels()))
	for _, label := range service.GetExpectedLabels() {
		if label == nil || label.GetKey() == "" {
			return dnsresolverobserver.Request{}, errs.New(
				errs.KindValidationFailed,
				"agent: DNS resolver ownership label is invalid",
			)
		}
		labels[label.GetKey()] = label.GetValue()
	}
	var digest [sha256.Size]byte
	copy(digest[:], action.GetArtifactDigest())
	var imageIndexDigest [sha256.Size]byte
	copy(imageIndexDigest[:], service.GetImageIndexDigest())
	var imageConfigDigest [sha256.Size]byte
	copy(imageConfigDigest[:], service.GetImageConfigDigest())
	return dnsresolverobserver.Request{
		ComponentID: action.GetComponentId(), ServiceID: service.GetServiceId(), ArtifactID: action.GetArtifactId(),
		ArtifactSHA256: digest, RenderGeneration: action.GetGeneration(), ProjectName: artifact.GetProjectName(),
		ServiceName: recipe.ServiceName(), ArtifactTarget: recipe.ArtifactTarget(),
		ImageReference: imageReference, ListenEndpoint: recipe.ListenEndpoint(),
		ImageRepository: service.GetImageRepository(), ImageIndexDigest: imageIndexDigest,
		ImageConfigDigest: imageConfigDigest,
		ImageOS:           service.GetImageOs(), ImageArchitecture: service.GetImageArchitecture(),
		ImageVariant: service.GetImageVariant(),
		MetricsURL:   recipe.MetricsURL(), ReloadMetric: recipe.ReloadMetric(), ExpectedLabels: labels,
	}, nil
}

func componentObservationComposeArtifact(plan *agentpb.ExecutionPlan) *agentpb.ComposeArtifact {
	if plan == nil {
		return nil
	}
	if len(plan.GetArtifacts()) == 1 {
		return plan.GetArtifacts()[0]
	}
	if len(plan.GetArtifacts()) != 2 {
		return nil
	}
	for _, step := range plan.GetSteps() {
		if apply := step.GetComposeApply(); apply != nil {
			for _, artifact := range plan.GetArtifacts() {
				if artifact.GetArtifactId() == apply.GetArtifactId() {
					return artifact
				}
			}
		}
	}
	return nil
}
