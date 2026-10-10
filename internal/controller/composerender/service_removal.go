package composerender

import (
	"crypto/sha256"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// RemoveEnvironmentServiceArtifact removes every physical member of one
// logical Service while preserving the exact ownership labels of all other
// runtime members. It never rewrites unrelated applied state as newly rendered.
func RemoveEnvironmentServiceArtifact(
	current *agentpb.ComposeArtifact,
	serviceID string,
	artifactID string,
) (*agentpb.ComposeArtifact, error) {
	if current == nil || current.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(ids.KindEnvironment, current.GetOwnerId()) != nil ||
		ids.Validate(ids.KindService, serviceID) != nil || ids.Validate(ids.KindConfig, artifactID) != nil {
		return nil, errs.New(errs.KindInternal, "Service removal artifact input is invalid")
	}
	owned := proto.CloneOf(current)
	var document yaml.Node
	if err := yaml.Unmarshal(owned.GetCanonicalYaml(), &document); err != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindInternal, "Service removal artifact YAML is corrupt")
	}
	services, err := ServiceArtifactMapping(document.Content[0])
	if err != nil {
		return nil, err
	}
	composeNames := make(map[string]struct{})
	retained := make([]*agentpb.ComposeService, 0, len(owned.GetServices()))
	for _, service := range owned.GetServices() {
		if service.GetServiceId() != serviceID {
			retained = append(retained, service)
			continue
		}
		componentID, err := serviceArtifactComponentOwner(service)
		if err != nil {
			return nil, err
		}
		if componentID != "" {
			return nil, errs.New(errs.KindResourceInUse, "Service is managed by its owning Component")
		}
		if service.GetComposeName() == "" {
			return nil, errs.New(errs.KindInternal, "Service removal artifact member is corrupt")
		}
		composeNames[service.GetComposeName()] = struct{}{}
	}
	if len(composeNames) == 0 {
		return nil, errs.New(errs.KindStateConflict, "Service removal artifact target is absent")
	}
	for composeName := range composeNames {
		index := MappingIndex(services, composeName)
		if index < 0 {
			return nil, errs.New(errs.KindInternal, "Service removal artifact member is absent from YAML")
		}
		services.Content = append(services.Content[:index], services.Content[index+2:]...)
	}
	sort.Slice(retained, func(left, right int) bool {
		return retained[left].GetComposeName() < retained[right].GetComposeName()
	})
	encoded, err := yaml.Marshal(&document)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	owned.ArtifactId = artifactID
	owned.Services = retained
	owned.CanonicalYaml = encoded
	owned.YamlSha256 = digest[:]
	return owned, nil
}
