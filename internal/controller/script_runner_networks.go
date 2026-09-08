package controller

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func projectScriptNetworks(
	service composetypes.ServiceConfig,
	sources etcd.ScriptExecutionSources,
	candidate *BlueprintScriptCandidateSources,
) ([]*agentpb.ScriptRunnerNetwork, error) {
	identityByName := make(map[string]string, len(sources.DesiredProjection.Record.DesiredZones))
	for _, desired := range sources.DesiredProjection.Record.DesiredZones {
		identityByName[desired.Desired.Name] = desired.Desired.ID
	}
	revisionByID := make(map[string]int64, len(sources.Networks))
	for _, network := range sources.Networks {
		revisionByID[network.Record.Desired.ID] = network.Revision
	}
	result := make([]*agentpb.ScriptRunnerNetwork, 0, len(service.Networks))
	for name, config := range service.Networks {
		networkID := identityByName[name]
		if networkID == "" || candidate == nil && revisionByID[networkID] <= 0 {
			return nil, errs.New(errs.KindStateConflict, "Script runner network is not part of the frozen topology")
		}
		if config == nil {
			config = &composetypes.ServiceNetworkConfig{}
		}
		if len(config.Extensions) != 0 {
			return nil, errs.New(errs.KindValidationFailed, "Script runner network contains unsupported extensions")
		}
		dockerName, err := networkname.New(networkID)
		if err != nil {
			return nil, errs.New(errs.KindStateConflict, "Script runner network identity is invalid")
		}
		source := existingScriptSourceAuthority(revisionByID[networkID])
		if candidate != nil {
			source = blueprintScriptStagedSourceAuthority(candidate, candidate.ProjectionSHA256)
		}
		result = append(result, &agentpb.ScriptRunnerNetwork{
			NetworkId: networkID, OwnerEnvironmentId: sources.Environment.Record.ID, Source: source,
			RenderedAttachment: &agentpb.ScriptNetworkAttachment{
				DockerNetworkName: dockerName,
				DriverOptions:     scriptPairs(config.DriverOpts), InterfaceName: config.InterfaceName,
				Priority: int64(config.Priority),
			},
		})
	}
	for _, network := range sources.AttachSources.Networks {
		if network.Revision <= 0 || network.ReadRevision != sources.Revision {
			return nil, errs.New(errs.KindStateConflict, "Script Attach network revision is invalid")
		}
		for _, existing := range result {
			if existing.NetworkId == network.Record.Desired.ID {
				return nil, errs.New(errs.KindStateConflict, "Script Attach network identity is duplicated")
			}
		}
		dockerName, err := networkname.New(network.Record.Desired.ID)
		if err != nil {
			return nil, errs.New(errs.KindStateConflict, "Script Attach network identity is invalid")
		}
		result = append(result, &agentpb.ScriptRunnerNetwork{
			NetworkId: network.Record.Desired.ID, OwnerEnvironmentId: network.Record.EnvironmentID,
			Source:             existingScriptSourceAuthority(network.Revision),
			RenderedAttachment: &agentpb.ScriptNetworkAttachment{DockerNetworkName: dockerName},
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].NetworkId < result[right].NetworkId })
	return result, nil
}
