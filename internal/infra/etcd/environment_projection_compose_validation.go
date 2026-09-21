package etcd

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

func validateEnvironmentNormalizedCompose(value []byte) error {
	if len(value) == 0 || len(value) > EnvironmentBlueprintProjectionMaxBytes {
		return errs.New(errs.KindValidationFailed, "Environment normalized authored Compose is missing or oversized")
	}
	var document yaml.Node
	if yaml.Unmarshal(value, &document) != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return errs.New(errs.KindValidationFailed, "Environment normalized authored Compose is invalid")
	}
	return nil
}

func validateEnvironmentServiceExtensions(
	serviceNames []string,
	extensions map[string]core.ServiceExtensionSpec,
) error {
	services := make(map[string]struct{}, len(serviceNames))
	for _, name := range serviceNames {
		services[name] = struct{}{}
	}
	for name, extension := range extensions {
		if _, exists := services[name]; !exists {
			return errs.New(errs.KindValidationFailed, "Environment Service extension target is absent")
		}
		if extension.Release != nil {
			switch extension.Release.DefaultStrategy {
			case "", core.StrategyBlueGreen, core.StrategyRecreate:
			default:
				return errs.New(errs.KindValidationFailed, "Environment Service release strategy is invalid")
			}
			switch extension.Release.OnFailure {
			case "", core.OnFailureSwitchBack, core.OnFailureLeaveActive:
			default:
				return errs.New(errs.KindValidationFailed, "Environment Service release failure policy is invalid")
			}
		}
	}
	for _, phase := range []core.ServiceLifecyclePhase{
		core.ServiceLifecycleStart,
		core.ServiceLifecycleDeploy,
		core.ServiceLifecycleRollback,
	} {
		if _, err := core.BuildServiceDependencyPhasePlan(serviceNames, extensions, phase); err != nil {
			return errs.Wrap(errs.KindValidationFailed, err)
		}
	}
	return nil
}

func validateEnvironmentProjectionArtifact(
	projection EnvironmentComposeProjection,
	kind environmentArtifactKind,
) error {
	if len(projection.ComposeArtifact) == 0 ||
		len(projection.ComposeArtifact) > EnvironmentBlueprintProjectionMaxBytes {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact is missing or oversized")
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		projection.ComposeArtifact,
		artifact,
	); err != nil {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact is invalid")
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || !bytes.Equal(canonical, projection.ComposeArtifact) {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact is not canonical")
	}
	if recordcodec.ValidateID(ids.KindConfig, artifact.GetArtifactId()) != nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != projection.EnvironmentID || artifact.GetAuthorizedVolumeDir() == "" ||
		len(artifact.GetCanonicalYaml()) == 0 || len(artifact.GetYamlSha256()) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact identity is invalid")
	}
	digest := sha256.Sum256(artifact.GetCanonicalYaml())
	if subtle.ConstantTimeCompare(digest[:], artifact.GetYamlSha256()) != 1 {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact digest is invalid")
	}
	expectedServices := make(map[string]environmentArtifactServiceIdentity, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		expectedServices[service.Desired.ID] = environmentArtifactServiceIdentity{
			name: service.Desired.Name, renderGeneration: projection.RenderGeneration,
			capturedRuntime: kind == environmentArtifactCapturedRuntime,
		}
	}
	for _, component := range projection.Components {
		if !component.Desired.Enabled {
			continue
		}
		for _, serviceID := range component.Runtime.GeneratedServices {
			expected := expectedServices[serviceID]
			if expected.componentID != "" {
				return errs.New(
					errs.KindValidationFailed,
					"Environment normalized Compose Service identity is duplicated",
				)
			}
			expected.componentID = component.Desired.ID
			expectedServices[serviceID] = expected
		}
	}
	if len(artifact.GetVolumes()) != len(projection.Volumes) {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact coverage is incomplete")
	}
	if err := validateEnvironmentArtifactServices(artifact, expectedServices); err != nil {
		return err
	}
	volumes := make(map[string]string, len(artifact.GetVolumes()))
	for _, volume := range artifact.GetVolumes() {
		if volume == nil {
			return errs.New(errs.KindValidationFailed, "Environment normalized Compose Volume is invalid")
		}
		if _, duplicate := volumes[volume.GetVolumeId()]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment normalized Compose Volume is duplicated")
		}
		volumes[volume.GetVolumeId()] = volume.GetComposeName()
	}
	for _, identity := range projection.Volumes {
		if volumes[identity.ID] != identity.Key {
			return errs.New(errs.KindValidationFailed, "Environment normalized Compose Volume identity changed")
		}
	}
	return nil
}
