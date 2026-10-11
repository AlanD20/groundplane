package composerender

import (
	"sort"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func validateRenderedRollingReplicas(input ComposeRenderInput, artifact *agentpb.ComposeArtifact) error {
	for serviceID, identity := range input.Releases {
		if identity.Rolling == nil {
			continue
		}
		for _, replica := range identity.Rolling.Candidate {
			if _, err := executionplan.RollingReplicaService(artifact, serviceID, replica); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRollingRenderIdentity(input ComposeRenderInput, serviceID string, identity ComposeReleaseIdentity) error {
	if identity.Rolling == nil || identity.Rolling.Validate() != nil || identity.Rolling.ServiceID != serviceID ||
		identity.Rolling.ReleaseID != identity.ReleaseID || identity.Rolling.Candidate[0].ArtifactID != input.ArtifactID ||
		!workloadimage.LocalIDValid(identity.Image) || identity.Target != domain.WorkloadRolling ||
		ids.Validate(ids.KindService, serviceID) != nil {
		return errs.New(
			errs.KindValidationFailed,
			"rolling render requires the captured replica plan and immutable workload image",
		)
	}
	if composeServiceComponentOwner(input.Identities.Services, serviceID) != "" {
		return errs.New(errs.KindValidationFailed, "managed Components cannot use rolling deployment")
	}
	return nil
}

// Render distinct singleton Compose services so start, readiness, drain and
// removal all address the same physical replica. The stable proxy alone owns
// authored aliases, published ports and reserved addresses.
func renderRollingReplicas(project *composetypes.Project, input ComposeRenderInput, authored composetypes.ServiceConfig,
	serviceID string, identity ComposeReleaseIdentity, active bool,
) ([]*agentpb.ComposeService, error) {
	if !active || authored.GetScale() != len(identity.Rolling.Candidate) || authored.NetworkMode != "" ||
		len(authored.Networks) == 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"rolling requires an active Service with the captured replica count and Zone networking",
		)
	}
	for _, resource := range input.Identities.Services {
		if resource.ID == serviceID && resource.PostgresToolsImage != "" {
			return nil, errs.New(errs.KindValidationFailed, "managed databases cannot use rolling deployment")
		}
	}
	result := make([]*agentpb.ComposeService, 0, len(identity.Rolling.Candidate))
	for _, replica := range identity.Rolling.Candidate {
		if _, exists := project.Services[replica.ComposeName]; exists {
			return nil, errs.New(
				errs.KindValidationFailed,
				"rolling replica name collides with another Compose service",
			)
		}
		if _, exists := input.Project.Services[replica.ComposeName]; exists {
			return nil, errs.New(errs.KindValidationFailed, "rolling replica name collides with an authored Service")
		}
		if _, exists := input.Project.DisabledServices[replica.ComposeName]; exists {
			return nil, errs.New(errs.KindValidationFailed, "rolling replica name collides with a disabled Service")
		}
		workload := authored
		workload.Name, workload.ContainerName, workload.Image = replica.ComposeName, "", identity.Image
		workload.Ports = nil
		if authored.Deploy != nil {
			deploy := *authored.Deploy
			workload.Deploy = &deploy
		}
		workload.SetScale(1)
		workload.Networks = cloneProxyNetworks(authored.Networks)
		for name, network := range workload.Networks {
			if network == nil {
				network = &composetypes.ServiceNetworkConfig{}
				workload.Networks[name] = network
			}
			network.Aliases = []string{replica.ComposeName}
			network.Ipv4Address, network.Ipv6Address, network.MacAddress = "", "", ""
			network.LinkLocalIPs = nil
		}
		labels, expected, err := composeServiceRuntimeLabels(
			authored.Labels,
			serviceID,
			"replica",
			"",
			identity.ReleaseID,
			input,
		)
		if err != nil {
			return nil, err
		}
		const ordinalLabel = "com.groundplane.replica-ordinal"
		labels[ordinalLabel] = strconv.FormatUint(uint64(replica.Ordinal), 10)
		expected = append(expected, &agentpb.LabelPair{Key: ordinalLabel, Value: labels[ordinalLabel]})
		sort.Slice(expected, func(i, j int) bool { return expected[i].Key < expected[j].Key })
		workload.Labels = labels
		workload.Extensions, err = composeResourceExtensions(
			authored.Extensions,
			"service",
			serviceID,
			input.EnvironmentID,
		)
		if err != nil {
			return nil, err
		}
		project.Services[replica.ComposeName] = workload
		result = append(result, &agentpb.ComposeService{
			ServiceId: serviceID, ComposeName: replica.ComposeName, ExpectedLabels: expected,
			ExpectedReplicas: 1, HasHealthcheck: true, ImageReference: identity.Image,
			Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA, RollingReplicaOrdinal: replica.Ordinal,
		})
	}
	return result, nil
}
