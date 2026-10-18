package executionplan

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// ReleaseNetworkWorkInputs selects only internal-DNS bridges from the frozen
// release artifact. Each returned input owns one bridge and can use the native
// Network executor; no hidden Task or generic plan is manufactured.
func ReleaseNetworkWorkInputs(source *agentpb.ComposeArtifact, intentSHA []byte, seed string) ([]*agentpb.NetworkWorkInput, error) {
	if source == nil || len(intentSHA) != sha256.Size || ids.Validate(ids.KindConfig, seed) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Release network preparation requires its captured artifact and intent")
	}
	at, err := ids.Timestamp(ids.KindConfig, seed)
	if err != nil {
		return nil, err
	}
	preparation, err := CaptureReleaseNetworkPreparation(source,
		ids.DeriveAt(ids.KindConfig, at, seed, "dns-networks"))
	if err != nil || preparation.Artifact == nil {
		return nil, err
	}
	var captured struct {
		Name     string                `yaml:"name"`
		Networks composetypes.Networks `yaml:"networks"`
	}
	if err := yaml.Unmarshal(preparation.Artifact.CanonicalYaml, &captured); err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	inputs := make([]*agentpb.NetworkWorkInput, 0, len(preparation.Artifact.Networks))
	for _, identity := range preparation.Artifact.Networks {
		network, found := captured.Networks[identity.ComposeName]
		if !found {
			return nil, errs.New(errs.KindStateConflict, "Release network capture is missing a selected bridge")
		}
		selection := struct {
			Name     string                `yaml:"name"`
			Networks composetypes.Networks `yaml:"networks"`
		}{Name: captured.Name, Networks: composetypes.Networks{identity.ComposeName: network}}
		content, err := yaml.Marshal(selection)
		if err != nil {
			return nil, errs.Wrap(errs.KindValidationFailed, err)
		}
		digest := sha256.Sum256(content)
		artifact := &agentpb.ComposeArtifact{
			ArtifactId: ids.DeriveAt(ids.KindConfig, at, seed, "dns-network/"+identity.NetworkId),
			OwnerKind:  source.OwnerKind, OwnerId: source.OwnerId, ProjectName: source.ProjectName,
			AuthorizedVolumeDir: source.AuthorizedVolumeDir, CanonicalYaml: content,
			YamlSha256: digest[:], Networks: []*agentpb.ComposeNetwork{proto.CloneOf(identity)},
		}
		input := &agentpb.NetworkWorkInput{Action: agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE,
			EnvironmentId: source.OwnerId, NetworkId: identity.NetworkId, DockerName: identity.DockerName,
			IntentSha256: append([]byte(nil), intentSHA...), Candidate: artifact}
		if err := ValidateNetworkWorkInput(input); err != nil {
			return nil, err
		}
		inputs = append(inputs, input)
	}
	return inputs, nil
}
