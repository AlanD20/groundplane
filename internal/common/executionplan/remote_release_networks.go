package executionplan

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/common/zonedns"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type releaseBridgeSelection struct {
	Name     string                `yaml:"name"`
	Networks composetypes.Networks `yaml:"networks"`
}

// ValidateRemoteReleaseNetworkWorkInput rechecks the remote isolation rules
// when the sealed single-bridge ENSURE input is reopened.
func ValidateRemoteReleaseNetworkWorkInput(input *agentpb.NetworkWorkInput) error {
	if err := ValidateNetworkWorkInput(input); err != nil {
		return err
	}
	if input.Action != agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE {
		return invalidRemoteBridge()
	}
	artifact := input.Candidate
	identity := artifact.Networks[0]
	var selected releaseBridgeSelection
	decoder := yaml.NewDecoder(bytes.NewReader(artifact.CanonicalYaml))
	decoder.KnownFields(true)
	if err := decoder.Decode(&selected); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if !errors.Is(decoder.Decode(&struct{}{}), io.EOF) || selected.Name != artifact.ProjectName ||
		len(selected.Networks) != 1 {
		return invalidRemoteBridge()
	}
	network, found := selected.Networks[identity.ComposeName]
	if !found {
		return invalidRemoteBridge()
	}
	return validateRemoteBridge(network, identity, input.EnvironmentId)
}

func captureRemoteReleaseNetworks(
	source *agentpb.ComposeArtifact,
	artifactID string,
) (ReleaseNetworkPreparation, error) {
	if err := validateArtifactContent(source); err != nil {
		return ReleaseNetworkPreparation{}, err
	}
	if source.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
		return ReleaseNetworkPreparation{}, invalidRemoteBridge()
	}
	if err := RejectUnknown(source); err != nil {
		return ReleaseNetworkPreparation{}, err
	}
	if err := validateNetworks(nil, source); err != nil {
		return ReleaseNetworkPreparation{}, err
	}
	var document struct {
		Name     string `yaml:"name"`
		Services map[string]struct {
			NetworkMode string                                        `yaml:"network_mode"`
			Networks    map[string]*composetypes.ServiceNetworkConfig `yaml:"networks"`
		} `yaml:"services"`
		Networks composetypes.Networks `yaml:"networks"`
	}
	if err := yaml.Unmarshal(source.CanonicalYaml, &document); err != nil {
		return ReleaseNetworkPreparation{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	if document.Name != source.ProjectName || len(document.Services) == 0 {
		return ReleaseNetworkPreparation{}, invalidRemoteBridge()
	}
	used := make(map[string]bool)
	for _, service := range document.Services {
		if service.NetworkMode != "" || len(service.Networks) == 0 {
			return ReleaseNetworkPreparation{}, invalidRemoteBridge()
		}
		for name := range service.Networks {
			used[name] = true
		}
	}
	if len(used) > 256 {
		return ReleaseNetworkPreparation{}, errs.New(
			errs.KindValidationFailed, "remote Release requires more than 256 private bridge Zones",
		)
	}
	selected := make(composetypes.Networks, len(used))
	identities := make([]*agentpb.ComposeNetwork, 0, len(used))
	for _, identity := range source.Networks {
		name := identity.ComposeName
		if !used[name] {
			continue
		}
		network, found := document.Networks[name]
		if _, duplicate := selected[name]; !found || duplicate {
			return ReleaseNetworkPreparation{}, invalidRemoteBridge()
		}
		if err := validateRemoteBridge(network, identity, source.OwnerId); err != nil {
			return ReleaseNetworkPreparation{}, err
		}
		selected[name] = network
		identities = append(identities, proto.CloneOf(identity))
	}
	if len(selected) != len(used) {
		return ReleaseNetworkPreparation{}, invalidRemoteBridge()
	}
	content, err := yaml.Marshal(releaseBridgeSelection{Name: document.Name, Networks: selected})
	if err != nil {
		return ReleaseNetworkPreparation{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(content)
	return ReleaseNetworkPreparation{Artifact: &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: source.OwnerKind, OwnerId: source.OwnerId,
		ProjectName: source.ProjectName, AuthorizedVolumeDir: source.AuthorizedVolumeDir,
		CanonicalYaml: content, YamlSha256: digest[:], Networks: identities,
	}}, nil
}

func validateRemoteBridge(
	network composetypes.NetworkConfig,
	identity *agentpb.ComposeNetwork,
	environmentID string,
) error {
	if network.Name != identity.DockerName || bool(network.External) || !network.Internal ||
		network.Driver != "" && network.Driver != "bridge" || len(network.DriverOpts) != 0 || network.Attachable ||
		network.EnableIPv4 != nil && !*network.EnableIPv4 || network.EnableIPv6 != nil && *network.EnableIPv6 ||
		network.Ipam.Driver != "" && network.Ipam.Driver != "default" || len(network.Ipam.Options) != 0 ||
		len(network.Ipam.Config) != 1 || network.Ipam.Config[0] == nil {
		return invalidRemoteBridge()
	}
	dns, err := zonedns.Enabled(network)
	if err != nil {
		return err
	}
	if dns {
		return errs.New(errs.KindStateConflict, "remote Release bridges cannot use shared GP DNS")
	}
	pool := network.Ipam.Config[0]
	prefix, err := ipam.ParseIPv4Prefix(pool.Subnet)
	if err != nil || prefix.String() != pool.Subnet || ipam.ValidateBridgeSubnet(prefix) != nil ||
		pool.Gateway != "" || pool.IPRange != "" || len(pool.AuxiliaryAddresses) != 0 {
		return invalidRemoteBridge()
	}
	return validateRemoteBridgeOwner(network, identity, environmentID)
}

func validateRemoteBridgeOwner(
	network composetypes.NetworkConfig,
	identity *agentpb.ComposeNetwork,
	environmentID string,
) error {
	expected := make(map[string]bool, len(identity.ExpectedLabels))
	for _, label := range identity.ExpectedLabels {
		if actual, exists := network.Labels[label.Key]; !exists || actual != label.Value {
			return invalidRemoteBridge()
		}
		expected[label.Key] = true
	}
	for key := range network.Labels {
		if strings.HasPrefix(key, "com.docker.compose.") ||
			strings.HasPrefix(key, "com.groundplane.") && !expected[key] {
			return invalidRemoteBridge()
		}
	}
	metadata, _ := network.Extensions["x-gp-resource"].(map[string]any)
	parent, _ := metadata["parent"].(map[string]any)
	if metadata["kind"] != "network" || metadata["id"] != identity.NetworkId ||
		parent["environment_id"] != environmentID {
		return invalidRemoteBridge()
	}
	return nil
}

func invalidRemoteBridge() error {
	return errs.New(errs.KindStateConflict, "remote Release requires explicit, owned, private IPv4 bridge Zones")
}
