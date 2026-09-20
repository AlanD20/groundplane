package scriptrunner

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"net/netip"
)

func connectSecondaryNetworks(
	ctx context.Context,
	connector networkConnector,
	containerID string,
	networks []*agentpb.ScriptRunnerNetwork,
	connected map[string]*network.EndpointSettings,
) error {
	for _, item := range networks[min(1, len(networks)):] {
		name := item.RenderedAttachment.DockerNetworkName
		if _, exists := connected[name]; exists {
			continue
		}
		if _, err := connector.NetworkConnect(ctx, name, client.NetworkConnectOptions{
			Container: containerID, EndpointConfig: scriptEndpointSettings(item),
		}); err != nil {
			return operationError(ctx, "connect secondary network "+name, err)
		}
	}
	return nil
}

func scriptEndpointSettings(item *agentpb.ScriptRunnerNetwork) *network.EndpointSettings {
	attachment := item.RenderedAttachment
	options := pairMap(attachment.DriverOptions)
	if attachment.InterfaceName != "" {
		if options == nil {
			options = make(map[string]string, 1)
		}
		options["com.docker.network.endpoint.ifname"] = attachment.InterfaceName
	}
	return &network.EndpointSettings{DriverOpts: options, GwPriority: int(attachment.Priority)}
}

func parseDNS(values []string) ([]netip.Addr, error) {
	result := make([]netip.Addr, len(values))
	for index, value := range values {
		parsed, err := netip.ParseAddr(value)
		if err != nil {
			return nil, errs.New(errs.KindInternal, "Script runner: invalid sealed DNS address")
		}
		result[index] = parsed
	}
	return result, nil
}
