package scriptrunner

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

func TestScriptRunnerCreatesOnPrimaryAndConnectsSecondaryBeforeStart(t *testing.T) {
	t.Parallel()
	const containerID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	primary := scriptRunnerNetwork("gp_net_primary", "eth0", 10)
	secondary := scriptRunnerNetwork("gp_net_secondary", "eth1", 20)
	projection := &agentpb.ScriptRunnerProjection{Networks: []*agentpb.ScriptRunnerNetwork{primary, secondary}}
	options, err := createOptions(scriptexecution.Request{Projection: projection}, "/tmp/body")
	if err != nil {
		t.Fatalf("createOptions() error = %v", err)
	}
	if options.HostConfig.NetworkMode != "gp_net_primary" || len(options.NetworkingConfig.EndpointsConfig) != 1 ||
		options.NetworkingConfig.EndpointsConfig["gp_net_primary"] == nil ||
		options.NetworkingConfig.EndpointsConfig["gp_net_secondary"] != nil {
		t.Fatalf(
			"create network projection = mode %q endpoints %#v",
			options.HostConfig.NetworkMode,
			options.NetworkingConfig.EndpointsConfig,
		)
	}

	connector := &recordingNetworkConnector{}
	if err := connectSecondaryNetworks(
		context.Background(), connector, containerID, projection.Networks,
		map[string]*network.EndpointSettings{"gp_net_primary": {}},
	); err != nil {
		t.Fatalf("connectSecondaryNetworks() error = %v", err)
	}
	if len(connector.calls) != 1 || connector.calls[0].name != "gp_net_secondary" ||
		connector.calls[0].options.Container != containerID ||
		connector.calls[0].options.EndpointConfig.DriverOpts["com.docker.network.endpoint.ifname"] != "eth1" ||
		connector.calls[0].options.EndpointConfig.GwPriority != 20 {
		t.Fatalf("secondary network calls = %#v", connector.calls)
	}

	sentinel := errors.New("secondary attach denied")
	connector = &recordingNetworkConnector{err: sentinel}
	err = connectSecondaryNetworks(context.Background(), connector, containerID, projection.Networks, nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("secondary network error = %v, want wrapped Docker cause", err)
	}
}

func scriptRunnerNetwork(name, interfaceName string, priority int64) *agentpb.ScriptRunnerNetwork {
	return &agentpb.ScriptRunnerNetwork{RenderedAttachment: &agentpb.ScriptNetworkAttachment{
		DockerNetworkName: name, InterfaceName: interfaceName, Priority: priority,
	}}
}

type networkConnectCall struct {
	name    string
	options client.NetworkConnectOptions
}

type recordingNetworkConnector struct {
	calls []networkConnectCall
	err   error
}

func (connector *recordingNetworkConnector) NetworkConnect(
	_ context.Context,
	name string,
	options client.NetworkConnectOptions,
) (client.NetworkConnectResult, error) {
	connector.calls = append(connector.calls, networkConnectCall{name: name, options: options})
	return client.NetworkConnectResult{}, connector.err
}
