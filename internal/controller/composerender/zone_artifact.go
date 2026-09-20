package composerender

import (
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
	"sort"
)

// ZoneArtifactMutation identifies one authored Environment network to remove
// from a new immutable Compose artifact.
type ZoneArtifactMutation struct {
	ZoneID           string
	ZoneName         string
	ArtifactID       string
	PlanID           string
	RenderGeneration uint64
}

// ZoneArtifactAddition identifies one authored Environment network to add to
// a new immutable Compose artifact.
type ZoneArtifactAddition struct {
	Zone             core.Zone
	ProjectID        string
	TenantID         string
	ArtifactID       string
	PlanID           string
	RenderGeneration uint64
}

// AddEnvironmentZoneArtifact adds exactly one managed network without
// changing any Service membership or other authored Compose resource.
func AddEnvironmentZoneArtifact(
	current *agentpb.ComposeArtifact,
	addition ZoneArtifactAddition,
) (*agentpb.ComposeArtifact, error) {
	subnet, subnetErr := ipam.ParseIPv4Prefix(addition.Zone.Subnet)
	dockerName, networkNameErr := networkname.New(addition.Zone.ID)
	if current == nil || current.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(ids.KindEnvironment, current.GetOwnerId()) != nil ||
		networkNameErr != nil || addition.Zone.Name == "" ||
		subnetErr != nil || subnet.String() != addition.Zone.Subnet ||
		ids.Validate(ids.KindProject, addition.ProjectID) != nil ||
		(addition.TenantID != "" && ids.Validate(ids.KindTenant, addition.TenantID) != nil) ||
		ids.Validate(ids.KindConfig, addition.ArtifactID) != nil ||
		ids.Validate(ids.KindPlan, addition.PlanID) != nil || addition.RenderGeneration == 0 {
		return nil, errs.New(errs.KindInternal, "Zone artifact addition input is invalid")
	}

	owned := proto.Clone(current).(*agentpb.ComposeArtifact)
	var document yaml.Node
	if err := yaml.Unmarshal(owned.GetCanonicalYaml(), &document); err != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindInternal, "normalized Compose artifact YAML is corrupt")
	}
	root := document.Content[0]
	networks := EnsureMappingValue(root, "networks")
	if networks == nil {
		return nil, errs.New(errs.KindInternal, "normalized Compose network mapping is corrupt")
	}
	if MappingIndex(networks, addition.Zone.Name) >= 0 {
		return nil, errs.New(errs.KindStateConflict, "Zone network already exists in the current desired revision")
	}
	for _, network := range owned.GetNetworks() {
		if network != nil &&
			(network.GetNetworkId() == addition.Zone.ID || network.GetComposeName() == addition.Zone.Name) {
			return nil, errs.New(
				errs.KindStateConflict,
				"Zone network identity already exists in the current desired revision",
			)
		}
	}

	network := EnsureMappingValue(networks, addition.Zone.Name)
	if network == nil {
		return nil, errs.New(errs.KindInternal, "normalized Compose Zone network is corrupt")
	}
	labels := map[string]string{
		composeLabelEnvironmentID: current.GetOwnerId(), composeLabelKind: "network",
		composeLabelManaged: "true", composeLabelProjectID: addition.ProjectID,
	}
	if addition.TenantID != "" {
		labels[composeLabelTenantID] = addition.TenantID
	}
	setMappingScalar(network, "name", dockerName)
	setMappingScalar(network, "driver", "bridge")
	if addition.Zone.Internal {
		setMappingTypedScalar(network, "internal", "!!bool", "true")
	}
	config := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	AppendMappingValue(config, "subnet", scalarNode(addition.Zone.Subnet))
	setMappingNode(network, "ipam", &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{
		scalarNode("config"), {Kind: yaml.SequenceNode, Tag: "!!seq", Content: []*yaml.Node{config}},
	}})
	labelNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for _, key := range sortedStringKeys(labels) {
		AppendMappingValue(labelNode, key, scalarNode(labels[key]))
	}
	setMappingNode(network, "labels", labelNode)
	resource := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	AppendMappingValue(resource, "kind", scalarNode("network"))
	AppendMappingValue(resource, "id", scalarNode(addition.Zone.ID))
	parent := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	AppendMappingValue(parent, "environment_id", scalarNode(current.GetOwnerId()))
	AppendMappingValue(resource, "parent", parent)
	setMappingNode(network, composeResourceExtension, resource)
	sortMapping(network)
	sortMapping(networks)
	owned.Networks = append(owned.Networks, &agentpb.ComposeNetwork{
		NetworkId: addition.Zone.ID, ComposeName: addition.Zone.Name, DockerName: dockerName,
		ExpectedLabels: labelPairs(labels),
	})
	if err := rewriteArtifactOwnership(root, addition.PlanID, addition.RenderGeneration); err != nil {
		return nil, err
	}
	for _, service := range owned.GetServices() {
		rewriteServiceArtifactLabelPairs(service.GetExpectedLabels(), addition.PlanID, addition.RenderGeneration)
	}
	for _, existing := range owned.GetNetworks() {
		rewriteServiceArtifactLabelPairs(existing.GetExpectedLabels(), addition.PlanID, addition.RenderGeneration)
	}
	for _, volume := range owned.GetVolumes() {
		rewriteServiceArtifactLabelPairs(volume.GetExpectedLabels(), addition.PlanID, addition.RenderGeneration)
	}
	sort.Slice(owned.Networks, func(left, right int) bool {
		return owned.Networks[left].GetNetworkId() < owned.Networks[right].GetNetworkId()
	})
	canonical, err := yaml.Marshal(&document)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(canonical)
	owned.ArtifactId = addition.ArtifactID
	owned.CanonicalYaml = canonical
	owned.YamlSha256 = digest[:]
	return owned, nil
}

// MutateEnvironmentZoneArtifact removes a Zone network and detaches its
// authored Compose name from every Service while preserving all Services.
func MutateEnvironmentZoneArtifact(
	current *agentpb.ComposeArtifact,
	mutation ZoneArtifactMutation,
) (*agentpb.ComposeArtifact, error) {
	if current == nil || current.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(ids.KindEnvironment, current.GetOwnerId()) != nil ||
		ids.Validate(ids.KindNetwork, mutation.ZoneID) != nil || mutation.ZoneName == "" ||
		ids.Validate(ids.KindConfig, mutation.ArtifactID) != nil ||
		ids.Validate(ids.KindPlan, mutation.PlanID) != nil || mutation.RenderGeneration == 0 {
		return nil, errs.New(errs.KindInternal, "Zone artifact mutation input is invalid")
	}

	owned := proto.Clone(current).(*agentpb.ComposeArtifact)
	var document yaml.Node
	if err := yaml.Unmarshal(owned.GetCanonicalYaml(), &document); err != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindInternal, "normalized Compose artifact YAML is corrupt")
	}
	root := document.Content[0]
	networksIndex := MappingIndex(root, "networks")
	if networksIndex < 0 || root.Content[networksIndex+1].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindStateConflict, "Zone network is absent from the current desired revision")
	}
	networks := root.Content[networksIndex+1]
	zoneIndex := MappingIndex(networks, mutation.ZoneName)
	metadataIndex := -1
	for index, network := range owned.GetNetworks() {
		if network != nil && network.GetNetworkId() == mutation.ZoneID &&
			network.GetComposeName() == mutation.ZoneName {
			if metadataIndex >= 0 {
				return nil, errs.New(errs.KindInternal, "Zone artifact network identity is duplicated")
			}
			metadataIndex = index
		}
	}
	if zoneIndex < 0 || metadataIndex < 0 {
		return nil, errs.New(errs.KindStateConflict, "Zone network is absent from the current desired revision")
	}
	networks.Content = append(networks.Content[:zoneIndex], networks.Content[zoneIndex+2:]...)
	owned.Networks = append(owned.Networks[:metadataIndex], owned.Networks[metadataIndex+1:]...)

	servicesIndex := MappingIndex(root, "services")
	if servicesIndex >= 0 {
		services := root.Content[servicesIndex+1]
		if services.Kind != yaml.MappingNode {
			return nil, errs.New(errs.KindInternal, "normalized Compose Service mapping is corrupt")
		}
		for index := 1; index < len(services.Content); index += 2 {
			service := services.Content[index]
			if service.Kind != yaml.MappingNode {
				return nil, errs.New(errs.KindInternal, "normalized Compose Service is corrupt")
			}
			if err := removeZoneFromServiceNetworks(service, mutation.ZoneName); err != nil {
				return nil, err
			}
		}
	}
	if err := rewriteArtifactOwnership(root, mutation.PlanID, mutation.RenderGeneration); err != nil {
		return nil, err
	}
	for _, service := range owned.GetServices() {
		rewriteServiceArtifactLabelPairs(service.GetExpectedLabels(), mutation.PlanID, mutation.RenderGeneration)
	}
	for _, network := range owned.GetNetworks() {
		rewriteServiceArtifactLabelPairs(network.GetExpectedLabels(), mutation.PlanID, mutation.RenderGeneration)
	}
	for _, volume := range owned.GetVolumes() {
		rewriteServiceArtifactLabelPairs(volume.GetExpectedLabels(), mutation.PlanID, mutation.RenderGeneration)
	}
	sort.Slice(owned.Networks, func(left, right int) bool {
		return owned.Networks[left].GetNetworkId() < owned.Networks[right].GetNetworkId()
	})
	canonical, err := yaml.Marshal(&document)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(canonical)
	owned.ArtifactId = mutation.ArtifactID
	owned.CanonicalYaml = canonical
	owned.YamlSha256 = digest[:]
	return owned, nil
}

func removeZoneFromServiceNetworks(service *yaml.Node, zoneName string) error {
	index := MappingIndex(service, "networks")
	if index < 0 {
		return nil
	}
	networks := service.Content[index+1]
	switch networks.Kind {
	case yaml.MappingNode:
		RemoveMappingValue(networks, zoneName)
	case yaml.SequenceNode:
		for item := 0; item < len(networks.Content); item++ {
			if networks.Content[item].Kind == yaml.ScalarNode && networks.Content[item].Value == zoneName {
				networks.Content = append(networks.Content[:item], networks.Content[item+1:]...)
				item--
			}
		}
	default:
		return errs.New(errs.KindInternal, "normalized Compose Service networks are corrupt")
	}
	if len(networks.Content) == 0 {
		RemoveMappingValue(service, "networks")
	}
	return nil
}
