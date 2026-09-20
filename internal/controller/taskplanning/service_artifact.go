package taskplanning

import (
	"crypto/sha256"
	"sort"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type ServiceArtifactAction string

const (
	ServiceArtifactCreate ServiceArtifactAction = "create"
	ServiceArtifactEdit   ServiceArtifactAction = "edit"
	ServiceArtifactRemove ServiceArtifactAction = "remove"
)

type ServiceArtifactMutation struct {
	Action           ServiceArtifactAction
	Desired          core.Service
	Zones            []ServiceArtifactZone
	ArtifactID       string
	PlanID           string
	TenantID         string
	ProjectID        string
	RenderGeneration uint64
}

type ServiceArtifactZone struct {
	ID       string
	Name     string
	Subnet   string
	Internal bool
}

func MutateEnvironmentServiceArtifact(
	current *agentpb.ComposeArtifact,
	mutation ServiceArtifactMutation,
) (*agentpb.ComposeArtifact, error) {
	if current == nil || current.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(ids.KindEnvironment, current.GetOwnerId()) != nil ||
		ids.Validate(
			ids.KindConfig,
			mutation.ArtifactID,
		) != nil || ids.Validate(ids.KindPlan, mutation.PlanID) != nil ||
		ids.Validate(
			ids.KindTenant,
			mutation.TenantID,
		) != nil || ids.Validate(ids.KindProject, mutation.ProjectID) != nil ||
		mutation.RenderGeneration == 0 ||
		(mutation.Action != ServiceArtifactCreate && mutation.Action != ServiceArtifactEdit &&
			mutation.Action != ServiceArtifactRemove) ||
		mutation.Desired.Validate() != nil ||
		(mutation.Action != ServiceArtifactRemove && validateServiceArtifactZones(mutation.Desired.Zones, mutation.Zones) != nil) {
		return nil, errs.New(errs.KindInternal, "Service artifact mutation input is invalid")
	}
	owned := proto.Clone(current).(*agentpb.ComposeArtifact)
	var document yaml.Node
	if err := yaml.Unmarshal(owned.GetCanonicalYaml(), &document); err != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindInternal, "normalized Compose artifact YAML is corrupt")
	}
	root := document.Content[0]
	services, err := serviceArtifactMapping(root)
	if err != nil {
		return nil, err
	}
	found := mappingIndex(services, mutation.Desired.Name)
	metadataIndex := serviceArtifactMetadataIndex(owned, mutation.Desired.ID, mutation.Desired.Name)
	if mutation.Action != ServiceArtifactCreate && metadataIndex >= 0 {
		componentID, err := serviceArtifactComponentOwner(owned.Services[metadataIndex])
		if err != nil {
			return nil, err
		}
		if componentID != "" {
			return nil, errs.New(errs.KindResourceInUse, "Service is managed by its owning Component")
		}
	}
	if mutation.Action == ServiceArtifactCreate {
		if found >= 0 || metadataIndex != -1 {
			return nil, errs.New(errs.KindStateConflict, "Service artifact identity is already present")
		}
		labels := serviceArtifactOwnershipLabels(current.GetOwnerId(), mutation)
		node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		labelNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		for _, key := range sortedStringKeys(labels) {
			appendMappingValue(labelNode, key, scalarNode(labels[key]))
		}
		appendMappingValue(node, "labels", labelNode)
		resource := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		appendMappingValue(resource, "kind", scalarNode("service"))
		appendMappingValue(resource, "id", scalarNode(mutation.Desired.ID))
		parent := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		appendMappingValue(parent, "environment_id", scalarNode(current.GetOwnerId()))
		appendMappingValue(resource, "parent", parent)
		appendMappingValue(node, composeResourceExtension, resource)
		if err := applyDirectServiceDesired(node, mutation.Desired); err != nil {
			return nil, err
		}
		appendMappingValue(services, mutation.Desired.Name, node)
		sortMapping(services)
		owned.Services = append(owned.Services, &agentpb.ComposeService{
			ServiceId: mutation.Desired.ID, ComposeName: mutation.Desired.Name,
			ExpectedLabels: labelPairs(labels), ExpectedReplicas: uint32(mutation.Desired.Replicas),
			HasHealthcheck: serviceNodeHasHealthcheck(node),
		})
	} else if mutation.Action == ServiceArtifactEdit {
		if found < 0 || metadataIndex < 0 {
			return nil, errs.New(errs.KindStateConflict, "Service artifact identity is absent")
		}
		if err := applyDirectServiceDesired(services.Content[found+1], mutation.Desired); err != nil {
			return nil, err
		}
		owned.Services[metadataIndex].ExpectedReplicas = uint32(mutation.Desired.Replicas)
		owned.Services[metadataIndex].HasHealthcheck = serviceNodeHasHealthcheck(services.Content[found+1])
	} else {
		if found < 0 || metadataIndex < 0 {
			return nil, errs.New(errs.KindStateConflict, "Service artifact identity is absent")
		}
		services.Content = append(services.Content[:found], services.Content[found+2:]...)
		owned.Services = append(owned.Services[:metadataIndex], owned.Services[metadataIndex+1:]...)
	}
	if err := ensureServiceZoneNetworks(root, owned, mutation); err != nil {
		return nil, err
	}
	if err := rewriteArtifactOwnership(root, mutation.PlanID, mutation.RenderGeneration); err != nil {
		return nil, err
	}
	for _, resource := range owned.GetServices() {
		rewriteServiceArtifactLabelPairs(resource.GetExpectedLabels(), mutation.PlanID, mutation.RenderGeneration)
	}
	for _, resource := range owned.GetNetworks() {
		rewriteServiceArtifactLabelPairs(resource.GetExpectedLabels(), mutation.PlanID, mutation.RenderGeneration)
	}
	for _, resource := range owned.GetVolumes() {
		rewriteServiceArtifactLabelPairs(resource.GetExpectedLabels(), mutation.PlanID, mutation.RenderGeneration)
	}
	sort.Slice(owned.Services, func(left, right int) bool {
		return owned.Services[left].GetServiceId() < owned.Services[right].GetServiceId()
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

func ProjectEnvironmentServiceNativeCompose(
	artifact *agentpb.ComposeArtifact,
	serviceName string,
) (string, error) {
	if artifact == nil || artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(ids.KindEnvironment, artifact.GetOwnerId()) != nil ||
		!core.ValidEnvironmentComposeName(serviceName) {
		return "", errs.New(errs.KindInternal, "Service native Compose projection input is invalid")
	}
	var document yaml.Node
	if err := yaml.Unmarshal(artifact.GetCanonicalYaml(), &document); err != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return "", errs.New(errs.KindInternal, "normalized Compose artifact YAML is corrupt")
	}
	root := document.Content[0]
	servicesIndex := mappingIndex(root, "services")
	if servicesIndex < 0 || root.Content[servicesIndex+1].Kind != yaml.MappingNode {
		return "", errs.New(errs.KindInternal, "normalized Compose Service mapping is missing")
	}
	services := root.Content[servicesIndex+1]
	serviceIndex := mappingIndex(services, serviceName)
	if serviceIndex < 0 || services.Content[serviceIndex+1].Kind != yaml.MappingNode {
		return "", errs.New(errs.KindInternal, "Service is absent from its current desired revision")
	}
	for _, metadata := range artifact.GetServices() {
		if metadata.GetComposeName() != serviceName {
			continue
		}
		componentID, err := serviceArtifactComponentOwner(metadata)
		if err != nil {
			return "", err
		}
		if componentID != "" {
			return "", errs.New(errs.KindServiceNotFound, "Service was not found")
		}
	}
	service := services.Content[serviceIndex+1]
	removeMappingValue(service, composeResourceExtension)
	for _, key := range []string{"labels", "annotations"} {
		if err := removeGroundplaneServiceMetadata(service, key); err != nil {
			return "", err
		}
	}
	projectedServices := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	appendMappingValue(projectedServices, serviceName, service)
	projectedRoot := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	appendMappingValue(projectedRoot, "services", projectedServices)
	projected := &yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{projectedRoot}}
	encoded, err := yaml.Marshal(projected)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	result := string(encoded)
	clear(encoded)
	return result, nil
}

func serviceArtifactComponentOwner(service *agentpb.ComposeService) (string, error) {
	componentID := ""
	for _, label := range service.GetExpectedLabels() {
		if label.GetKey() != composeLabelComponentID {
			continue
		}
		if componentID != "" && componentID != label.GetValue() {
			return "", errs.New(errs.KindInternal, "Service artifact Component ownership is corrupt")
		}
		componentID = label.GetValue()
	}
	if componentID != "" && ids.Validate(ids.KindComponent, componentID) != nil {
		return "", errs.New(errs.KindInternal, "Service artifact Component ownership is corrupt")
	}
	return componentID, nil
}

func removeGroundplaneServiceMetadata(service *yaml.Node, key string) error {
	index := mappingIndex(service, key)
	if index < 0 {
		return nil
	}
	metadata := service.Content[index+1]
	if metadata.Kind != yaml.MappingNode {
		return errs.New(errs.KindInternal, "normalized Compose Service metadata is corrupt")
	}
	kept := metadata.Content[:0]
	for offset := 0; offset+1 < len(metadata.Content); offset += 2 {
		if strings.HasPrefix(metadata.Content[offset].Value, "com.groundplane.") {
			continue
		}
		kept = append(kept, metadata.Content[offset], metadata.Content[offset+1])
	}
	metadata.Content = kept
	if len(metadata.Content) == 0 {
		removeMappingValue(service, key)
	}
	return nil
}

func validateServiceArtifactZones(names []string, zones []ServiceArtifactZone) error {
	if len(names) != len(zones) {
		return errs.New(errs.KindInternal, "Service artifact Zone bindings are invalid")
	}
	seen := make(map[string]struct{}, len(zones))
	for index, zone := range zones {
		subnet, err := ipam.ParseIPv4Prefix(zone.Subnet)
		if _, nameErr := networkname.New(zone.ID); nameErr != nil || zone.Name != names[index] ||
			err != nil || subnet.String() != zone.Subnet {
			return errs.New(errs.KindInternal, "Service artifact Zone bindings are invalid")
		}
		if _, exists := seen[zone.Name]; exists {
			return errs.New(errs.KindInternal, "Service artifact Zone bindings are invalid")
		}
		seen[zone.Name] = struct{}{}
	}
	return nil
}

func ensureServiceZoneNetworks(
	root *yaml.Node,
	artifact *agentpb.ComposeArtifact,
	mutation ServiceArtifactMutation,
) error {
	networks := ensureMappingValue(root, "networks")
	if networks == nil {
		return errs.New(errs.KindInternal, "normalized Compose network mapping is corrupt")
	}
	for _, zone := range mutation.Zones {
		dockerName, err := networkname.New(zone.ID)
		if err != nil {
			return errs.New(errs.KindInternal, "Service artifact Zone identity is invalid")
		}
		owned := false
		for _, network := range artifact.GetNetworks() {
			if network.GetNetworkId() == zone.ID || network.GetComposeName() == zone.Name {
				if network.GetNetworkId() != zone.ID || network.GetComposeName() != zone.Name {
					return errs.New(errs.KindInternal, "Service artifact Zone identity conflicts with an owned network")
				}
				owned = true
				break
			}
		}
		if owned {
			if mappingIndex(networks, zone.Name) < 0 {
				return errs.New(errs.KindInternal, "Service artifact owned Zone network is absent")
			}
			continue
		}
		network := ensureMappingValue(networks, zone.Name)
		if network == nil {
			return errs.New(errs.KindInternal, "normalized Compose Zone network is corrupt")
		}
		labels := map[string]string{
			composeLabelEnvironmentID: artifact.GetOwnerId(), composeLabelKind: "network",
			composeLabelManaged: "true", composeLabelProjectID: mutation.ProjectID,
			composeLabelTenantID: mutation.TenantID,
		}
		setMappingScalar(network, "name", dockerName)
		setMappingScalar(network, "driver", "bridge")
		removeMappingValue(network, "external")
		if zone.Internal {
			setMappingTypedScalar(network, "internal", "!!bool", "true")
		} else {
			removeMappingValue(network, "internal")
		}
		config := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		appendMappingValue(config, "subnet", scalarNode(zone.Subnet))
		configs := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Content: []*yaml.Node{config}}
		ipamNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		appendMappingValue(ipamNode, "config", configs)
		setMappingNode(network, "ipam", ipamNode)
		labelNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		for _, key := range sortedStringKeys(labels) {
			appendMappingValue(labelNode, key, scalarNode(labels[key]))
		}
		setMappingNode(network, "labels", labelNode)
		resource := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		appendMappingValue(resource, "kind", scalarNode("network"))
		appendMappingValue(resource, "id", scalarNode(zone.ID))
		parent := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		appendMappingValue(parent, "environment_id", scalarNode(artifact.GetOwnerId()))
		appendMappingValue(resource, "parent", parent)
		setMappingNode(network, composeResourceExtension, resource)
		sortMapping(network)
		artifact.Networks = append(artifact.Networks, &agentpb.ComposeNetwork{
			NetworkId: zone.ID, ComposeName: zone.Name, DockerName: dockerName,
			ExpectedLabels: labelPairs(labels),
		})
	}
	sortMapping(networks)
	sort.Slice(artifact.Networks, func(left, right int) bool {
		return artifact.Networks[left].GetNetworkId() < artifact.Networks[right].GetNetworkId()
	})
	return nil
}

func serviceArtifactMapping(root *yaml.Node) (*yaml.Node, error) {
	index := mappingIndex(root, "services")
	if index < 0 {
		value := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		appendMappingValue(root, "services", value)
		return value, nil
	}
	value := root.Content[index+1]
	if value.Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindInternal, "normalized Compose Service mapping is corrupt")
	}
	return value, nil
}

func serviceArtifactMetadataIndex(artifact *agentpb.ComposeArtifact, serviceID, name string) int {
	for index, service := range artifact.GetServices() {
		if service.GetServiceId() == serviceID || service.GetComposeName() == name {
			if service.GetServiceId() == serviceID && service.GetComposeName() == name {
				return index
			}
			return -2
		}
	}
	return -1
}

func applyDirectServiceDesired(node *yaml.Node, desired core.Service) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return errs.New(errs.KindInternal, "normalized Compose Service is corrupt")
	}
	setMappingScalar(node, "image", desired.Image)
	setServiceZones(node, desired.Zones)
	setServiceStringSequence(node, "expose", desired.Expose)
	setOrRemoveMappingScalar(node, "restart", desired.Restart)
	setOrRemoveMappingScalar(node, "mem_limit", desired.Resources.Mem)
	if desired.Resources.CPUs == 0 {
		removeMappingValue(node, "cpus")
	} else {
		setMappingTypedScalar(node, "cpus", "!!float", strconv.FormatFloat(desired.Resources.CPUs, 'g', -1, 64))
	}
	deploy := ensureMappingValue(node, "deploy")
	if deploy == nil {
		return errs.New(errs.KindInternal, "normalized Compose Service deploy mapping is corrupt")
	}
	setMappingTypedScalar(deploy, "replicas", "!!int", strconv.Itoa(desired.Replicas))
	if desired.Healthcheck == (core.Healthcheck{}) {
		if index := mappingIndex(node, "healthcheck"); index >= 0 && typedServiceHealthcheck(node.Content[index+1]) {
			removeMappingValue(node, "healthcheck")
		}
	} else {
		setMappingNode(node, "healthcheck", serviceHealthcheckNode(desired.Healthcheck))
	}
	sortMapping(node)
	return nil
}

func setServiceZones(service *yaml.Node, zones []string) {
	current := map[string]*yaml.Node{}
	if index := mappingIndex(service, "networks"); index >= 0 && service.Content[index+1].Kind == yaml.MappingNode {
		mapping := service.Content[index+1]
		for offset := 0; offset+1 < len(mapping.Content); offset += 2 {
			current[mapping.Content[offset].Value] = mapping.Content[offset+1]
		}
	}
	networks := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for _, zone := range zones {
		value := current[zone]
		if value == nil {
			value = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		}
		appendMappingValue(networks, zone, value)
	}
	setMappingNode(service, "networks", networks)
}

func setServiceStringSequence(mapping *yaml.Node, key string, values []string) {
	if len(values) == 0 {
		removeMappingValue(mapping, key)
		return
	}
	sequence := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	for _, value := range values {
		sequence.Content = append(sequence.Content, scalarNode(value))
	}
	setMappingNode(mapping, key, sequence)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func serviceArtifactOwnershipLabels(environmentID string, mutation ServiceArtifactMutation) map[string]string {
	return map[string]string{
		composeLabelEnvironmentID: environmentID, composeLabelKind: "service", composeLabelManaged: "true",
		composeLabelPlanID: mutation.PlanID, composeLabelProjectID: mutation.ProjectID,
		composeLabelRenderGen: strconv.FormatUint(mutation.RenderGeneration, 10),
		composeLabelServiceID: mutation.Desired.ID, composeLabelTenantID: mutation.TenantID,
	}
}

func rewriteServiceArtifactLabelPairs(values []*agentpb.LabelPair, planID string, generation uint64) {
	for _, pair := range values {
		if pair == nil {
			continue
		}
		if pair.Key == composeLabelPlanID {
			pair.Value = planID
		} else if pair.Key == composeLabelRenderGen {
			pair.Value = strconv.FormatUint(generation, 10)
		}
	}
}

func sortedStringKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func setMappingNode(mapping *yaml.Node, key string, value *yaml.Node) {
	if index := mappingIndex(mapping, key); index >= 0 {
		mapping.Content[index+1] = value
		return
	}
	appendMappingValue(mapping, key, value)
}

func setMappingTypedScalar(mapping *yaml.Node, key, tag, value string) {
	setMappingNode(mapping, key, &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: value})
}

func setOrRemoveMappingScalar(mapping *yaml.Node, key, value string) {
	if value == "" {
		removeMappingValue(mapping, key)
		return
	}
	setMappingScalar(mapping, key, value)
}

func removeMappingValue(mapping *yaml.Node, key string) {
	if index := mappingIndex(mapping, key); index >= 0 {
		mapping.Content = append(mapping.Content[:index], mapping.Content[index+2:]...)
	}
}

func ensureMappingValue(mapping *yaml.Node, key string) *yaml.Node {
	if index := mappingIndex(mapping, key); index >= 0 {
		if mapping.Content[index+1].Kind != yaml.MappingNode {
			return nil
		}
		return mapping.Content[index+1]
	}
	value := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	appendMappingValue(mapping, key, value)
	return value
}
