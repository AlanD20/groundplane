package composehelper

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ATT-08/10: endpoint reconciliation must never recreate a workload or select
// its proxy/unrelated Services, including retries and a stopped consumer.
func TestNativeAttachReconcilesOnlyCapturedContainerEndpoints(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		t.Run(map[bool]string{false: "running", true: "stopped"}[stopped], func(t *testing.T) {
			input := nativeAttachConsumerFixture(t)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			container, network := strings.Repeat("a", 64), strings.Repeat("b", 64)
			name, err := networkname.New(input.NetworkId)
			if err != nil {
				t.Fatal(err)
			}
			connected, changes := false, 0
			fake := runner.NewFake()
			fake.RunFunc = func(_ context.Context, command runner.RunCmdOpts) (runner.Result, error) {
				args := command.Args
				if command.Name != DockerExecutable || len(args) < 2 {
					t.Fatalf("unexpected command: %s %v", command.Name, args)
				}
				switch {
				case args[0] == "container" && args[1] == "ls":
					for _, arg := range args {
						if strings.HasPrefix(arg, "label=com.docker.compose.service=") &&
							arg != "label=com.docker.compose.service=app--blue" {
							t.Fatalf("selected uncaptured workload: %v", args)
						}
					}
					if stopped {
						return runner.Result{}, nil
					}
					return runner.Result{Stdout: []byte(container)}, nil
				case args[0] == "container" && args[1] == "inspect":
					if args[len(args)-1] != container {
						t.Fatalf("inspected an unrelated container: %v", args)
					}
					if slices.Contains(args, "{{json .Config.Labels}}") {
						labels := map[string]string{"com.docker.compose.project": input.Baseline.ProjectName,
							"com.docker.compose.service": "app--blue", "com.groundplane.managed": "true",
							"com.groundplane.environment-id": input.EnvironmentId, "com.groundplane.service-id": input.ServiceId}
						for _, label := range input.Baseline.Services[0].ExpectedLabels {
							labels[label.Key] = label.Value
						}
						return nativeAttachJSON(t, labels), nil
					}
					image := input.Baseline.Services[0].ImageReference
					return runner.Result{Stdout: []byte(image + "\n" + image)}, nil
				case args[0] == "network" && args[1] == "ls":
					return runner.Result{Stdout: []byte(network + " " + name)}, nil
				case args[0] == "network" && args[1] == "inspect":
					members := map[string]any{}
					if connected {
						members[container] = map[string]any{}
					}
					return nativeAttachJSON(t, map[string]any{"Id": network, "Name": name, "Containers": members,
						"Labels": map[string]string{
							"com.groundplane.managed":        "true",
							"com.groundplane.kind":           "network",
							"com.groundplane.environment-id": input.BackingEnvironmentId,
						}}), nil
				case args[0] == "network" && (args[1] == "connect" || args[1] == "disconnect"):
					if !slices.Equal(args[2:], []string{network, container}) || stopped {
						t.Fatalf("wrong endpoint mutation: %v", args)
					}
					connected = args[1] == "connect"
					changes++
					return runner.Result{}, nil
				default:
					t.Fatalf("Attach started/recreated runtime or expanded its authority: %v", args)
					return runner.Result{}, nil
				}
			}
			for range 2 {
				observation, err := ApplyAttachConsumerDocker(ctx, fake, input)
				if err != nil || !observation.Connected {
					t.Fatalf("connection = %v, %v", observation, err)
				}
			}
			detach, err := executionplan.PrepareAttachConsumerTransition(ctx, input.Candidate, input.EnvironmentId,
				input.BackingEnvironmentId, input.ServiceId, input.NetworkId, ids.New(ids.KindConfig))
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				observation, err := ApplyAttachConsumerDocker(ctx, fake, detach)
				if err != nil || !observation.Detached {
					t.Fatalf("disconnection = %v, %v", observation, err)
				}
			}
			expected := 2
			if stopped {
				expected = 0
			}
			if changes != expected {
				t.Fatalf("endpoint mutations=%d, expected=%d", changes, expected)
			}
		})
	}
}

func nativeAttachJSON(t *testing.T, value any) runner.Result {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return runner.Result{Stdout: raw}
}

func nativeAttachConsumerFixture(t *testing.T) *agentpb.AttachConsumerTransition {
	t.Helper()
	environment, service := ids.New(ids.KindEnvironment), ids.New(ids.KindService)
	image := "sha256:" + strings.Repeat("c", 64)
	raw := []byte(
		"services:\n  app--blue:\n    image: " + image + "\n    profiles: [configured]\n    command: [run, captured]\n  app:\n    image: caddy:2\n  other:\n    image: alpine:latest\n",
	)
	digest := sha256.Sum256(raw)
	baseline := &agentpb.ComposeArtifact{ArtifactId: ids.New(ids.KindConfig),
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: environment,
		ProjectName: "gp-" + strings.ToLower(environment), CanonicalYaml: raw, YamlSha256: digest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes/" + ids.New(
			ids.KindTenant,
		) + "/" + ids.New(
			ids.KindProject,
		) + "/" + environment,
		Services: []*agentpb.ComposeService{
			{ServiceId: service, ComposeName: "app--blue", ExpectedReplicas: 1, HasHealthcheck: true, Slot: "blue",
				ImageReference: image, Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.release-id", Value: ids.New(ids.KindDeployment)},
					{
						Key:   "com.groundplane.runtime-role",
						Value: "slot",
					},
					{Key: "com.groundplane.slot", Value: "blue"},
				}},
			{
				ServiceId:   service,
				ComposeName: "app",
				Role:        agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			},
			{ServiceId: ids.New(ids.KindService), ComposeName: "other"},
		}}
	input, err := executionplan.PrepareAttachConsumerConnection(t.Context(), baseline, environment,
		ids.New(ids.KindEnvironment), service, ids.New(ids.KindNetwork), ids.New(ids.KindConfig))
	if err != nil {
		t.Fatal(err)
	}
	return input
}
