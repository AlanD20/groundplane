package executionplan

import (
	"crypto/sha256"
	"crypto/subtle"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/zonedns"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// ReleaseNetworkPreparation contains no workload, configuration or image effect.
// It is derived from the release's already-captured artifact, never desired state.
type ReleaseNetworkPreparation struct {
	Artifact  *agentpb.ComposeArtifact
	Upstreams []string
}

func CaptureReleaseNetworkPreparation(
	source *agentpb.ComposeArtifact,
	artifactID string,
) (ReleaseNetworkPreparation, error) {
	if source == nil || source.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(
			ids.KindEnvironment,
			source.GetOwnerId(),
		) != nil || ids.Validate(ids.KindConfig, artifactID) != nil ||
		len(source.GetCanonicalYaml()) == 0 || len(source.GetCanonicalYaml()) > MaximumArtifactYAMLBytes {
		return ReleaseNetworkPreparation{}, errs.New(errs.KindValidationFailed, "release network source is invalid")
	}
	digest := sha256.Sum256(source.GetCanonicalYaml())
	if subtle.ConstantTimeCompare(digest[:], source.GetYamlSha256()) != 1 {
		return ReleaseNetworkPreparation{}, errs.New(errs.KindStateConflict, "release network source digest changed")
	}
	// This stage selects network memberships, not workload configuration. Native
	// Compose fields such as healthcheck durations require Compose's own loader.
	var document struct {
		Name     string `yaml:"name"`
		Services map[string]struct {
			Networks map[string]*composetypes.ServiceNetworkConfig `yaml:"networks"`
		} `yaml:"services"`
		Networks composetypes.Networks `yaml:"networks"`
	}
	if err := yaml.Unmarshal(source.GetCanonicalYaml(), &document); err != nil {
		return ReleaseNetworkPreparation{}, errs.Wrap(errs.KindInternal, err)
	}
	if document.Name != source.GetProjectName() {
		return ReleaseNetworkPreparation{}, errs.New(errs.KindStateConflict, "release network source project changed")
	}
	used := make(map[string]bool)
	for _, service := range document.Services {
		for name := range service.Networks {
			used[name] = true
		}
	}
	selection := struct {
		Name     string                `yaml:"name"`
		Networks composetypes.Networks `yaml:"networks"`
	}{Name: document.Name, Networks: make(composetypes.Networks)}
	var networks []*agentpb.ComposeNetwork
	var upstreams []string
	for _, identity := range source.GetNetworks() {
		if identity == nil {
			return ReleaseNetworkPreparation{}, errs.New(errs.KindInternal, "release network identity is missing")
		}
		if !used[identity.GetComposeName()] {
			continue
		}
		network, exists := document.Networks[identity.GetComposeName()]
		if !exists || bool(network.External) {
			return ReleaseNetworkPreparation{}, errs.New(errs.KindStateConflict, "release owned network source changed")
		}
		enabled, err := zonedns.Enabled(network)
		if err != nil {
			return ReleaseNetworkPreparation{}, err
		}
		if !enabled {
			continue
		}
		if len(network.Ipam.Config) != 1 || network.Ipam.Config[0] == nil {
			return ReleaseNetworkPreparation{}, errs.New(errs.KindInternal, "release DNS network subnet is missing")
		}
		gateway, err := zonedns.Gateway(network.Ipam.Config[0].Subnet)
		if err != nil {
			return ReleaseNetworkPreparation{}, err
		}
		selection.Networks[identity.GetComposeName()] = network
		networks = append(networks, proto.Clone(identity).(*agentpb.ComposeNetwork))
		upstreams = append(upstreams, gateway)
	}
	if len(networks) == 0 {
		return ReleaseNetworkPreparation{}, nil
	}
	if len(networks) > 256 {
		return ReleaseNetworkPreparation{}, errs.New(
			errs.KindValidationFailed,
			"release requires more than 256 internal DNS Zones",
		)
	}
	slices.SortFunc(networks, func(left, right *agentpb.ComposeNetwork) int {
		return strings.Compare(left.GetNetworkId(), right.GetNetworkId())
	})
	slices.Sort(upstreams)
	upstreams = slices.Compact(upstreams)
	content, err := yaml.Marshal(selection)
	if err != nil {
		return ReleaseNetworkPreparation{}, errs.Wrap(errs.KindInternal, err)
	}
	selectedDigest := sha256.Sum256(content)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: source.GetOwnerKind(), OwnerId: source.GetOwnerId(),
		ProjectName: source.GetProjectName(), AuthorizedVolumeDir: source.GetAuthorizedVolumeDir(),
		CanonicalYaml: content, YamlSha256: append([]byte(nil), selectedDigest[:]...), Networks: networks,
	}
	return ReleaseNetworkPreparation{Artifact: artifact, Upstreams: upstreams}, nil
}

func BuildReleaseNetworkPreparation(planID string, generation uint64, timeout uint32,
	preparation ReleaseNetworkPreparation,
) (*agentpb.ExecutionPlan, error) {
	if preparation.Artifact == nil || len(preparation.Artifact.GetNetworks()) == 0 || timeout == 0 {
		return nil, errs.New(errs.KindValidationFailed, "release network preparation is empty")
	}
	planTime, err := ids.Timestamp(ids.KindPlan, planID)
	if err != nil {
		return nil, err
	}
	count := len(preparation.Artifact.GetNetworks())
	steps := make([]*agentpb.ExecutionStep, count*2)
	members := make([]*agentpb.ManagedNetworkPreparationMember, count)
	previous := ""
	for index, network := range preparation.Artifact.GetNetworks() {
		stepID := ids.DeriveAt(ids.KindStep, planTime, planID, "release-network-ensure:"+network.GetNetworkId())
		probeID := ids.DeriveAt(ids.KindStep, planTime, planID, "release-network-probe:"+network.GetNetworkId())
		steps[index] = &agentpb.ExecutionStep{
			StepId: stepID, PrerequisiteStepId: previous, TimeoutSeconds: timeout,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			Payload: &agentpb.ExecutionStep_ManagedNetworkEnsure{ManagedNetworkEnsure: &agentpb.ManagedNetworkEnsure{
				ArtifactId: preparation.Artifact.GetArtifactId(), NetworkId: network.GetNetworkId(),
			}},
		}
		probePrevious := ""
		if index > 0 {
			probePrevious = members[index-1].GetProbeStepId()
		}
		steps[count+index] = &agentpb.ExecutionStep{
			StepId: probeID, PrerequisiteStepId: probePrevious, TimeoutSeconds: timeout,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
			Payload: &agentpb.ExecutionStep_ManagedNetworkPreparationProbe{
				ManagedNetworkPreparationProbe: &agentpb.ManagedNetworkPreparationProbe{
					ArtifactId: preparation.Artifact.GetArtifactId(), NetworkId: network.GetNetworkId(),
				},
			},
		}
		members[index] = &agentpb.ManagedNetworkPreparationMember{
			NetworkId:    network.GetNetworkId(),
			EnsureStepId: stepID,
			ProbeStepId:  probeID,
		}
		previous = stepID
	}
	return Seal(&agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: planID, RenderGeneration: generation,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, TargetId: preparation.Artifact.GetOwnerId(),
		Artifacts: []*agentpb.ComposeArtifact{preparation.Artifact}, Steps: steps,
		ManagedNetworkRecoveryProcedure: &agentpb.ManagedNetworkRecoveryProcedure{
			EnvironmentId: preparation.Artifact.GetOwnerId(),
			Operation: &agentpb.ManagedNetworkRecoveryProcedure_Preparation{
				Preparation: &agentpb.ManagedNetworkPreparationRecovery{
					ArtifactId: preparation.Artifact.GetArtifactId(), Members: members,
				},
			},
		},
	})
}
