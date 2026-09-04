package controller

import (
	"crypto/sha256"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

const (
	composeLabelEnvironmentID    = "com.groundplane.environment-id"
	composeLabelComponentID      = "com.groundplane.component-id"
	composeLabelKind             = "com.groundplane.kind"
	composeLabelManaged          = "com.groundplane.managed"
	composeLabelPlanID           = "com.groundplane.plan-id"
	composeLabelProjectID        = "com.groundplane.project-id"
	composeLabelRenderGen        = "com.groundplane.render-generation"
	composeLabelServiceID        = "com.groundplane.service-id"
	composeLabelTenantID         = "com.groundplane.tenant-id"
	composeLabelReleaseID        = "com.groundplane.release-id"
	composeLabelSlot             = "com.groundplane.slot"
	composeLabelRuntimeRole      = "com.groundplane.runtime-role"
	composeResourceExtension     = "x-gp-resource"
	composeNetworkExtension      = "x-gp-network"
	composeVolumeBackupExtension = "x-gp-backup"
	composeVolumeSlugExtension   = "x-gp-slug"
)

// ComposeRenderInput is the complete Controller-owned input to one environment artifact render.
type ComposeRenderInput struct {
	Project             *composetypes.Project
	ArtifactID          string
	ProjectOwnerKind    ComposeProjectOwnerKind
	TenantID            string
	ProjectID           string
	EnvironmentID       string
	PlanID              string
	RenderGeneration    uint64
	AuthorizedVolumeDir string
	Identities          ComposeIdentitySnapshot
	ExternalNetworks    []ComposeResourceIdentity
	Releases            map[string]ComposeReleaseIdentity
}

type ComposeProjectOwnerKind string

const (
	ComposeProjectOwnerTenant  ComposeProjectOwnerKind = "tenant"
	ComposeProjectOwnerBacking ComposeProjectOwnerKind = "backing"
)

type ComposeReleaseIdentity struct {
	ReleaseID              string
	Target                 domain.WorkloadTarget
	Image                  string
	ServingReleaseID       string
	ServingTarget          domain.WorkloadTarget
	ServingProxyGeneration uint64
	Strategy               domain.Strategy
}

// RenderCompose converts one parsed, normalized owned-resource project into the Agent's immutable artifact.
func RenderCompose(input ComposeRenderInput) (*agentpb.ComposeArtifact, error) {
	if err := validateComposeRenderInput(input); err != nil {
		return nil, err
	}
	serviceNames, err := ownedServiceNames(input.Project)
	if err != nil {
		return nil, err
	}
	ownedNetworkNames, externalNetworkNames, err := renderableNetworkNames(input.Project)
	if err != nil {
		return nil, err
	}
	serviceIDs, err := indexComposeIdentities(ids.KindService, serviceNames, input.Identities.Services)
	if err != nil {
		return nil, err
	}
	networkIDs, err := indexComposeIdentities(ids.KindNetwork, ownedNetworkNames, input.Identities.Networks)
	if err != nil {
		return nil, err
	}
	externalNetworkIDs, err := indexComposeIdentities(ids.KindNetwork, externalNetworkNames, input.ExternalNetworks)
	if err != nil {
		return nil, err
	}
	volumeNames, err := renderableManagedVolumeNames(input.Project)
	if err != nil {
		return nil, err
	}
	volumeIDs, err := indexComposeIdentities(ids.KindVolume, volumeNames, input.Identities.Volumes)
	if err != nil {
		return nil, err
	}

	projectName := "gp-" + strings.ToLower(input.EnvironmentID)
	rendered := *input.Project
	rendered.Name = projectName
	rendered.Services = make(composetypes.Services, len(serviceNames)*3)
	rendered.DisabledServices = nil
	rendered.Networks = make(composetypes.Networks, len(ownedNetworkNames)+len(externalNetworkNames))
	rendered.Volumes = make(composetypes.Volumes, len(volumeNames))

	services, err := renderServiceProxyTopology(&rendered, input, serviceNames, serviceIDs)
	if err != nil {
		return nil, err
	}

	networks := make([]*agentpb.ComposeNetwork, 0, len(ownedNetworkNames))
	for _, name := range ownedNetworkNames {
		network := input.Project.Networks[name]
		networkID := networkIDs[name]
		labels, expectedLabels, err := composeOwnershipLabels(network.Labels, "network", "", input)
		if err != nil {
			return nil, err
		}
		extensions, err := composeResourceExtensions(network.Extensions, "network", networkID, input.EnvironmentID)
		if err != nil {
			return nil, err
		}
		dockerName, err := networkname.New(networkID)
		if err != nil {
			return nil, err
		}
		network.Name = dockerName
		network.Labels = labels
		network.Extensions = extensions
		rendered.Networks[name] = network
		networks = append(networks, &agentpb.ComposeNetwork{
			NetworkId:      networkID,
			ComposeName:    name,
			DockerName:     dockerName,
			ExpectedLabels: expectedLabels,
		})
	}
	sort.Slice(networks, func(i, j int) bool { return networks[i].NetworkId < networks[j].NetworkId })
	for _, name := range externalNetworkNames {
		network := input.Project.Networks[name]
		if err := rejectComposeOwnershipLabels(network.Labels); err != nil {
			return nil, err
		}
		dockerName, err := networkname.New(externalNetworkIDs[name])
		if err != nil {
			return nil, err
		}
		network.Name = dockerName
		rendered.Networks[name] = network
	}

	volumes := make([]*agentpb.ComposeVolume, 0, len(volumeNames))
	for _, name := range volumeNames {
		volume := input.Project.Volumes[name]
		volumeID := volumeIDs[name]
		labels, expectedLabels, err := composeOwnershipLabels(volume.Labels, "volume", "", input)
		if err != nil {
			return nil, err
		}
		authoredExtensions := make(composetypes.Extensions, len(volume.Extensions))
		for key, value := range volume.Extensions {
			if key != composeVolumeBackupExtension && key != composeVolumeSlugExtension {
				authoredExtensions[key] = value
			}
		}
		extensions, err := composeResourceExtensions(authoredExtensions, "volume", volumeID, input.EnvironmentID)
		if err != nil {
			return nil, err
		}
		dockerName := "gp_vol_" + strings.ToLower(volumeID)
		volume.Name = dockerName
		volume.Driver = "local"
		volume.DriverOpts = composetypes.Options{
			"type": "none", "o": "bind", "device": filepath.Join(input.AuthorizedVolumeDir, name),
		}
		volume.Labels = labels
		volume.Extensions = extensions
		rendered.Volumes[name] = volume
		volumes = append(volumes, &agentpb.ComposeVolume{
			VolumeId: volumeID, ComposeName: name, DockerName: dockerName, ExpectedLabels: expectedLabels,
		})
	}
	sort.Slice(volumes, func(i, j int) bool { return volumes[i].VolumeId < volumes[j].VolumeId })

	canonicalYAML, err := rendered.MarshalYAML()
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(canonicalYAML)
	return &agentpb.ComposeArtifact{
		ArtifactId:          input.ArtifactID,
		OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:             input.EnvironmentID,
		ProjectName:         projectName,
		CanonicalYaml:       canonicalYAML,
		YamlSha256:          digest[:],
		AuthorizedVolumeDir: input.AuthorizedVolumeDir,
		Services:            services,
		Networks:            networks,
		Volumes:             volumes,
	}, nil
}

func validateComposeRenderInput(input ComposeRenderInput) error {
	if input.Project == nil || input.RenderGeneration == 0 {
		return errs.New(errs.KindInternal, "compose render input is incomplete")
	}
	identities := []struct {
		kind  ids.Kind
		value string
	}{
		{kind: ids.KindConfig, value: input.ArtifactID},
		{kind: ids.KindProject, value: input.ProjectID},
		{kind: ids.KindEnvironment, value: input.EnvironmentID},
		{kind: ids.KindPlan, value: input.PlanID},
	}
	for _, identity := range identities {
		if ids.Validate(identity.kind, identity.value) != nil {
			return errs.New(errs.KindInternal, "compose render identity is invalid")
		}
	}
	switch input.ProjectOwnerKind {
	case ComposeProjectOwnerTenant:
		if ids.Validate(ids.KindTenant, input.TenantID) != nil {
			return errs.New(errs.KindInternal, "tenant-owned Compose render requires a Tenant id")
		}
	case ComposeProjectOwnerBacking:
		if input.TenantID != "" {
			return errs.New(errs.KindInternal, "backing-owned Compose render must not carry a Tenant id")
		}
	default:
		return errs.New(errs.KindInternal, "Compose render Project owner kind is invalid")
	}
	if !filepath.IsAbs(input.AuthorizedVolumeDir) ||
		filepath.Clean(input.AuthorizedVolumeDir) != input.AuthorizedVolumeDir {
		return errs.New(errs.KindInternal, "compose authorized volume directory is invalid")
	}
	for serviceID, release := range input.Releases {
		if ids.Validate(ids.KindService, serviceID) != nil ||
			(release.ReleaseID != "" && ids.Validate(ids.KindDeployment, release.ReleaseID) != nil) ||
			(release.Strategy != domain.StrategyBlueGreen && release.Strategy != domain.StrategyRecreate) ||
			release.Target.Validate() != nil || release.ServingTarget.Validate() != nil {
			return errs.New(errs.KindInternal, "compose release identity is invalid")
		}
		expected, err := domain.TargetFor(release.Strategy, release.Target.Slot())
		if err != nil || expected != release.Target {
			return errs.New(errs.KindInternal, "compose release workload topology is invalid")
		}
	}
	return nil
}

func renderableNetworkNames(project *composetypes.Project) ([]string, []string, error) {
	owned := make([]string, 0, len(project.Networks))
	external := make([]string, 0, len(project.Networks))
	for name, network := range project.Networks {
		if _, unresolved := network.Extensions[composeNetworkExtension]; unresolved {
			return nil, nil, errs.New(errs.KindNotImplemented, "x-gp-network resolution is not implemented")
		}
		if network.External {
			external = append(external, name)
			continue
		}
		owned = append(owned, name)
	}
	sort.Strings(owned)
	sort.Strings(external)
	return owned, external, nil
}

func renderableManagedVolumeNames(project *composetypes.Project) ([]string, error) {
	names := make([]string, 0, len(project.Volumes))
	for name, volume := range project.Volumes {
		if volume.External || (volume.Driver != "" && volume.Driver != "local") || len(volume.DriverOpts) != 0 {
			return nil, errs.New(errs.KindNotImplemented, "compose volume runtime requires an unresolved contract")
		}
		for extension := range volume.Extensions {
			if strings.HasPrefix(extension, "x-gp-") &&
				extension != composeVolumeBackupExtension && extension != composeVolumeSlugExtension {
				return nil, errs.New(errs.KindNotImplemented, "compose volume extension is not implemented")
			}
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func indexComposeIdentities(
	kind ids.Kind,
	desiredNames []string,
	identities []ComposeResourceIdentity,
) (map[string]string, error) {
	if len(desiredNames) != len(identities) {
		return nil, errs.New(errs.KindInternal, "compose identity coverage is incomplete")
	}
	indexed := make(map[string]string, len(identities))
	previousName := ""
	usedIDs := make(map[string]struct{}, len(identities))
	for _, identity := range identities {
		if identity.Name == "" || identity.Name <= previousName || ids.Validate(kind, identity.ID) != nil ||
			(identity.ComponentID != "" && (kind != ids.KindService || ids.Validate(ids.KindComponent, identity.ComponentID) != nil)) {
			return nil, errs.New(errs.KindInternal, "compose identity snapshot is invalid or unsorted")
		}
		if _, duplicate := usedIDs[identity.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "compose identity snapshot contains a duplicate id")
		}
		previousName = identity.Name
		indexed[identity.Name] = identity.ID
		usedIDs[identity.ID] = struct{}{}
	}
	for _, name := range desiredNames {
		if _, exists := indexed[name]; !exists {
			return nil, errs.New(errs.KindInternal, "compose identity coverage is incomplete")
		}
	}
	return indexed, nil
}

func composeOwnershipLabels(
	authored composetypes.Labels,
	resourceKind string,
	serviceID string,
	input ComposeRenderInput,
) (composetypes.Labels, []*agentpb.LabelPair, error) {
	if err := rejectComposeOwnershipLabels(authored); err != nil {
		return nil, nil, err
	}
	labels := make(composetypes.Labels, len(authored)+8)
	for key, value := range authored {
		labels[key] = value
	}
	expected := map[string]string{
		composeLabelEnvironmentID: input.EnvironmentID,
		composeLabelKind:          resourceKind,
		composeLabelManaged:       "true",
		composeLabelProjectID:     input.ProjectID,
	}
	if resourceKind == "service" {
		for _, identity := range input.Identities.Services {
			if identity.ID == serviceID && identity.ComponentID != "" {
				expected[composeLabelComponentID] = identity.ComponentID
				break
			}
		}
	}
	if input.ProjectOwnerKind == ComposeProjectOwnerTenant {
		expected[composeLabelTenantID] = input.TenantID
	}
	if resourceKind == "service" {
		expected[composeLabelServiceID] = serviceID
		expected[composeLabelPlanID] = input.PlanID
		expected[composeLabelRenderGen] = strconv.FormatUint(input.RenderGeneration, 10)
	}
	keys := make([]string, 0, len(expected))
	for key, value := range expected {
		labels[key] = value
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]*agentpb.LabelPair, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, &agentpb.LabelPair{Key: key, Value: expected[key]})
	}
	return labels, pairs, nil
}

func rejectComposeOwnershipLabels(labels composetypes.Labels) error {
	for key := range labels {
		if strings.HasPrefix(key, "com.groundplane.") {
			return errs.New(errs.KindValidationFailed, "compose label uses the reserved ownership namespace")
		}
	}
	return nil
}

func composeResourceExtensions(
	authored composetypes.Extensions,
	resourceKind string,
	resourceID string,
	environmentID string,
) (composetypes.Extensions, error) {
	extensions := make(composetypes.Extensions, len(authored)+1)
	for key, value := range authored {
		if key == composeResourceExtension {
			return nil, errs.New(errs.KindValidationFailed, "x-gp-resource is Controller-generated metadata")
		}
		extensions[key] = value
	}
	extensions[composeResourceExtension] = renderedComposeResourceMetadata{
		Kind: resourceKind,
		ID:   resourceID,
		Parent: renderedComposeResourceParent{
			EnvironmentID: environmentID,
		},
	}
	return extensions, nil
}

type renderedComposeResourceMetadata struct {
	Kind   string                        `yaml:"kind"`
	ID     string                        `yaml:"id"`
	Parent renderedComposeResourceParent `yaml:"parent"`
}

type renderedComposeResourceParent struct {
	EnvironmentID string `yaml:"environment_id"`
}
