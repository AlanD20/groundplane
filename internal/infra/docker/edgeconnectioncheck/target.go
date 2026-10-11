package edgeconnectioncheck

import (
	"crypto/sha256"
	"encoding/hex"
	"runtime"
	"strconv"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane-component-sdk/component"
	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Target is constructed from the acknowledged Compose artifact and the
// Agent's compiled catalog recipe, never operator-selected labels or images.
type Target struct {
	request        contract.Request
	project        string
	service        string
	imageReference string
	platform       component.OCIPlatform
	labels         map[string]string
}

func NewTarget(
	request contract.Request,
	artifact *agentpb.ComposeArtifact,
	recipe registeredcatalog.EdgeTunnelObservationRecipe,
	expectedGeneration uint64,
) (Target, error) {
	if request.Validate() != nil || request.ReadyURL != recipe.ReadyURL() || artifact == nil ||
		artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT || artifact.OwnerId != request.EnvironmentID ||
		artifact.ProjectName == "" || len(artifact.CanonicalYaml) == 0 || len(artifact.CanonicalYaml) > executionplan.MaximumArtifactYAMLBytes ||
		len(artifact.YamlSha256) != sha256.Size || hex.EncodeToString(artifact.YamlSha256) != request.RuntimeSHA256 {
		return Target{}, errs.New(
			errs.KindValidationFailed,
			"Tunnel check requires its exact acknowledged runtime and compiled recipe",
		)
	}
	digest := sha256.Sum256(artifact.CanonicalYaml)
	if hex.EncodeToString(digest[:]) != request.RuntimeSHA256 {
		return Target{}, errs.New(
			errs.KindValidationFailed,
			"Tunnel check runtime content differs from its captured digest",
		)
	}
	var selected *agentpb.ComposeService
	for _, service := range artifact.Services {
		if service.GetOwnerComponentId() != request.ComponentID {
			continue
		}
		if selected != nil || service.GetServiceId() != request.ServiceID {
			return Target{}, errs.New(errs.KindValidationFailed, "Tunnel runtime ownership is not unique")
		}
		selected = service
	}
	image := recipe.Image()
	platform, reference, found := image.Select(runtime.GOOS, runtime.GOARCH)
	if !found || selected == nil || selected.ComposeName != recipe.ServiceName() || selected.ExpectedReplicas != 1 ||
		selected.ImageReference != reference || selected.ImageRepository != image.Repository ||
		hex.EncodeToString(
			selected.ImageIndexDigest,
		) != image.IndexDigest || hex.EncodeToString(selected.ImageChildDigest) != platform.ChildDigest ||
		hex.EncodeToString(selected.ImageConfigDigest) != platform.ConfigDigest || selected.ImageOs != platform.OS ||
		selected.ImageArchitecture != platform.Architecture || selected.ImageVariant != platform.Variant {
		return Target{}, errs.New(errs.KindValidationFailed, "Tunnel runtime differs from the compiled connector image")
	}
	labels := make(map[string]string, len(selected.ExpectedLabels)+4)
	for _, pair := range selected.ExpectedLabels {
		if pair == nil || pair.Key == "" {
			return Target{}, errs.New(errs.KindValidationFailed, "Tunnel ownership label is invalid")
		}
		if _, duplicate := labels[pair.Key]; duplicate {
			return Target{}, errs.New(errs.KindValidationFailed, "Tunnel ownership label is repeated")
		}
		labels[pair.Key] = pair.Value
	}
	if labels["com.groundplane.managed"] != "true" || labels["com.groundplane.kind"] != "service" ||
		labels["com.groundplane.environment-id"] != request.EnvironmentID || labels["com.groundplane.component-id"] != request.ComponentID ||
		labels["com.groundplane.service-id"] != request.ServiceID || ids.Validate(ids.KindPlan, labels["com.groundplane.plan-id"]) != nil {
		return Target{}, errs.New(errs.KindValidationFailed, "Tunnel runtime has incomplete native ownership")
	}
	generation, err := strconv.ParseUint(labels["com.groundplane.render-generation"], 10, 64)
	if err != nil || generation == 0 || generation != expectedGeneration ||
		strconv.FormatUint(generation, 10) != labels["com.groundplane.render-generation"] {
		return Target{}, errs.New(errs.KindValidationFailed, "Tunnel runtime generation is invalid")
	}
	for name, expected := range map[string]string{
		"com.docker.compose.project":          artifact.ProjectName,
		"com.docker.compose.service":          selected.ComposeName,
		"com.docker.compose.container-number": "1",
		"com.docker.compose.oneoff":           "False",
	} {
		if value, present := labels[name]; present && value != expected {
			return Target{}, errs.New(
				errs.KindValidationFailed,
				"Tunnel Compose ownership contradicts its captured artifact",
			)
		}
	}
	labels["com.docker.compose.project"] = artifact.ProjectName
	labels["com.docker.compose.service"] = selected.ComposeName
	labels["com.docker.compose.container-number"] = "1"
	labels["com.docker.compose.oneoff"] = "False"
	return Target{
		request:        request,
		project:        artifact.ProjectName,
		service:        selected.ComposeName,
		imageReference: reference,
		platform:       platform,
		labels:         labels,
	}, nil
}
