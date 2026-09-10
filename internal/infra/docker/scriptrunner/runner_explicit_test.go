package scriptrunner

import (
	"bufio"
	"context"
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// Rationale: explicit setup has no network. The before-start network connector
// must accept an empty capture without panicking or attaching a default network.
func TestExplicitScriptWithoutNetworksSkipsConnections(t *testing.T) {
	connector := &recordingNetworkConnector{}
	if err := connectSecondaryNetworks(context.Background(), connector, "captured-container", nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(connector.calls) != 0 {
		t.Fatal("networkless setup connected to a network")
	}
}

// Rationale: Docker options must preserve the explicit setup identity and exact
// writable grant without network, ports, aliases, restart or image Volume copying.
func TestExplicitScriptDockerOptionsPreserveMinimalGrant(t *testing.T) {
	projection := &agentpb.ScriptRunnerProjection{
		Image: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Uid:   0, Gid: 0, WorkingDir: "/", Entrypoint: []string{"/bin/sh"}, Command: []string{bodyTarget},
		Mounts: []*agentpb.ScriptRunnerMount{{RenderedMount: &agentpb.ScriptMount{
			Type: "volume", Source: "gp_vol_vol_01arz3ndektsv4rrffq69g5fav", Target: "/etc/tls", VolumeNoCopy: true,
		}}},
	}
	options, err := createOptions(scriptexecution.Request{Projection: projection}, "/fixture/body")
	if err != nil {
		t.Fatal(err)
	}
	if options.Config.Image != projection.Image || options.Config.User != "0:0" || options.Config.WorkingDir != "/" ||
		len(options.Config.Env) != 0 || len(options.Config.ExposedPorts) != 0 ||
		options.HostConfig.NetworkMode != "none" || len(options.NetworkingConfig.EndpointsConfig) != 0 ||
		len(options.HostConfig.PortBindings) != 0 || options.HostConfig.RestartPolicy.Name != "no" ||
		options.HostConfig.LogConfig.Type != "none" {
		t.Fatal("explicit Docker options introduced ambient access or changed execution identity")
	}
	if len(options.HostConfig.Mounts) != 2 {
		t.Fatal("explicit Docker options did not mount only body and declared Volume")
	}
	body, volume := options.HostConfig.Mounts[0], options.HostConfig.Mounts[1]
	if !body.ReadOnly || body.Target != bodyTarget || volume.ReadOnly || volume.Target != "/etc/tls" ||
		volume.Source != projection.Mounts[0].RenderedMount.Source || volume.VolumeOptions == nil || !volume.VolumeOptions.NoCopy {
		t.Fatal("explicit body/Volume access or no-copy decision was changed")
	}
}

// Rationale: exercise the actual stopped-container start path, output drain and
// exact cleanup, not just option rendering; networkless setup must complete once.
func TestExplicitScriptWithoutNetworksRunsAndCleansCapturedContainer(t *testing.T) {
	runner, fixture, request, prepared := cleanupFixture(t)
	request.Projection.WorkingDir = "/"
	connection, peer := net.Pipe()
	t.Cleanup(
		func() { _ = connection.Close(); _ = peer.Close() },
	) // In-memory output fixture has no external resources.
	engine := &networklessScriptEngine{cleanupEngine: fixture, connection: connection}
	runner.client = engine
	body := bodyEvidence(prepared)
	evidence, err := runner.CreateContainer(t.Context(), request, body)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.RunContainer(t.Context(), request, body, evidence)
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("networkless Script run: %#v, %v", result, err)
	}
	proof, err := runner.Cleanup(request, &body, &evidence)
	if err != nil || !proof.ContainerAbsent || !proof.BodyAbsent || !proof.ExecutionDirectoryAbsent {
		t.Fatalf("networkless Script cleanup: %#v, %v", proof, err)
	}
	if !slices.Equal(engine.effects, []string{"create", "attach", "start", "wait"}) {
		t.Fatalf("unexpected Docker execution effects: %v", engine.effects)
	}
}

type networklessScriptEngine struct {
	*cleanupEngine
	connection net.Conn
	effects    []string
}

func (engine *networklessScriptEngine) ContainerCreate(
	ctx context.Context, options client.ContainerCreateOptions,
) (client.ContainerCreateResult, error) {
	engine.effects = append(engine.effects, "create")
	engine.created = true
	engine.inspected.Container.Config = options.Config
	engine.inspected.Container.HostConfig = options.HostConfig
	return engine.cleanupEngine.ContainerCreate(ctx, options)
}

func (engine *networklessScriptEngine) NetworkConnect(
	context.Context, string, client.NetworkConnectOptions,
) (client.NetworkConnectResult, error) {
	engine.effects = append(engine.effects, "network-connect")
	return client.NetworkConnectResult{}, nil
}

func (engine *networklessScriptEngine) ContainerAttach(
	context.Context, string, client.ContainerAttachOptions,
) (client.ContainerAttachResult, error) {
	engine.effects = append(engine.effects, "attach")
	return client.ContainerAttachResult{HijackedResponse: client.HijackedResponse{
		Conn: engine.connection, Reader: bufio.NewReader(strings.NewReader("")),
	}}, nil
}

func (engine *networklessScriptEngine) ContainerStart(
	context.Context, string, client.ContainerStartOptions,
) (client.ContainerStartResult, error) {
	engine.effects = append(engine.effects, "start")
	engine.created = false
	return client.ContainerStartResult{}, nil
}

func (engine *networklessScriptEngine) ContainerWait(
	context.Context, string, client.ContainerWaitOptions,
) client.ContainerWaitResult {
	engine.effects = append(engine.effects, "wait")
	result := make(chan container.WaitResponse, 1)
	result <- container.WaitResponse{StatusCode: 0}
	close(result)
	return client.ContainerWaitResult{Result: result}
}
