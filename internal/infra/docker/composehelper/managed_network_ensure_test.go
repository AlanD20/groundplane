package composehelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestManagedNetworkEnsureCreatesOnlyAbsentOwnedBridge(t *testing.T) {
	request, name := networkEnsureRequest(t)
	fake := &managedNetworkRunner{
		results: []runner.Result{
			{},
			{Stdout: []byte(strings.Repeat("a", 64) + "\n")},
			{Stdout: networkInspection(t, name, "10.60.0.0/24", true)},
		},
	}
	response, err := Execute(context.Background(), fake, request)
	if err != nil || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
		t.Fatalf("response=%v error=%v commands=%v", response, err, fake.commands)
	}
	if len(fake.commands) != 3 || fake.commands[1][0] != "network" || fake.commands[1][1] != "create" {
		t.Fatalf("commands=%v", fake.commands)
	}
	joined := strings.Join(fake.commands[1], " ")
	for _, required := range []string{"--driver bridge", "--subnet 10.60.0.0/24", "--internal", "com.docker.compose.project=gp-" + strings.ToLower(helperEnvironmentID), "com.docker.compose.network=backend", name} {
		if !strings.Contains(joined, required) {
			t.Fatalf("create lacks %q: %s", required, joined)
		}
	}
	if strings.Contains(joined, "config-hash") {
		t.Fatal("preparation must not publish a Compose config hash")
	}
}

func networkEnsureRequest(t *testing.T) (*agentpb.ComposeHelperRequest, string) {
	t.Helper()
	request, _ := validComponentConfigRequest(t)
	artifact := request.Plan.Artifacts[0]
	const networkID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	name := "gp_net_" + networkID
	artifact.Networks = []*agentpb.ComposeNetwork{
		{
			NetworkId:   networkID,
			ComposeName: "backend",
			DockerName:  name,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.environment-id", Value: helperEnvironmentID},
				{Key: "com.groundplane.kind", Value: "network"},
				{Key: "com.groundplane.managed", Value: "true"},
			},
		},
	}
	artifact.CanonicalYaml = []byte(
		"networks:\n  backend:\n    name: " + name + "\n    driver: bridge\n    internal: true\n    ipam:\n      config:\n        - subnet: 10.60.0.0/24\n    labels:\n      com.groundplane.environment-id: " + helperEnvironmentID + "\n      com.groundplane.kind: network\n      com.groundplane.managed: 'true'\n",
	)
	hash := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = hash[:]
	request.Plan.Steps = []*agentpb.ExecutionStep{
		{
			StepId:         helperStepID,
			TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ManagedNetworkEnsure{
				ManagedNetworkEnsure: &agentpb.ManagedNetworkEnsure{
					ArtifactId: artifact.ArtifactId,
					NetworkId:  networkID,
				},
			},
		},
	}
	request.Plan.PlanHash = nil
	var err error
	request.Plan, err = executionplan.Seal(request.Plan)
	if err != nil {
		t.Fatal(err)
	}
	return request, name
}

func networkInspection(t *testing.T, name, subnet string, internal bool) []byte {
	t.Helper()
	value := inspectedOwnedNetwork{
		ID:       strings.Repeat("a", 64),
		Name:     name,
		Driver:   "bridge",
		Internal: internal,
		Labels: map[string]string{
			"com.groundplane.environment-id": helperEnvironmentID,
			"com.groundplane.kind":           "network",
			"com.groundplane.managed":        "true",
			"com.docker.compose.project":     "gp-" + strings.ToLower(helperEnvironmentID),
			"com.docker.compose.network":     "backend",
		},
	}
	value.IPAM.Driver = "default"
	value.IPAM.Config = append(value.IPAM.Config, struct {
		Subnet, Gateway, IPRange string
		AuxiliaryAddresses       map[string]string
	}{Subnet: subnet, Gateway: "10.60.0.1"})
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestManagedNetworkEnsureExistingNetworkNeverMutated(t *testing.T) {
	for _, name := range []string{"matching", "compose-created", "divergent-hash", "wrong-subnet", "wrong-internal", "foreign-owner", "wrong-compose-project", "wrong-compose-network", "wrong-id", "wrong-driver", "duplicate-name", "inspect-failed"} {
		t.Run(name, func(t *testing.T) {
			request, dockerName := networkEnsureRequest(t)
			var actual inspectedOwnedNetwork
			if err := json.Unmarshal(networkInspection(t, dockerName, "10.60.0.0/24", true), &actual); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "compose-created":
				// Golden JSON follows Compose2.40.3 / compose-go2.9.1 NetworkConfig;
				// it deliberately does not call the preparation hash implementation.
				encoded := `{"name":"` + dockerName + `","driver":"bridge","ipam":{"config":[{"subnet":"10.60.0.0/24"}]},"internal":true,"labels":{"com.groundplane.environment-id":"` + helperEnvironmentID + `","com.groundplane.kind":"network","com.groundplane.managed":"true"}}`
				hash := sha256.Sum256([]byte(encoded))
				actual.Labels["com.docker.compose.config-hash"] = hex.EncodeToString(hash[:])
			case "divergent-hash":
				actual.Labels["com.docker.compose.config-hash"] = strings.Repeat("f", 64)
			case "wrong-subnet":
				actual.IPAM.Config[0].Subnet = "10.61.0.0/24"
			case "wrong-internal":
				actual.Internal = false
			case "foreign-owner":
				actual.Labels["com.groundplane.environment-id"] = "other"
			case "wrong-compose-project":
				actual.Labels["com.docker.compose.project"] = "other"
			case "wrong-compose-network":
				actual.Labels["com.docker.compose.network"] = "other"
			case "wrong-id":
				actual.ID = strings.Repeat("b", 64)
			case "wrong-driver":
				actual.Driver = "overlay"
			}
			encoded, err := json.Marshal(actual)
			if err != nil {
				t.Fatal(err)
			}
			listed := strings.Repeat("a", 64) + " " + dockerName + "\n"
			if name == "duplicate-name" {
				listed += strings.Repeat("b", 64) + " " + dockerName + "\n"
			}
			fake := &managedNetworkRunner{results: []runner.Result{{Stdout: []byte(listed)}, {Stdout: encoded}}}
			if name == "inspect-failed" {
				fake.results[1] = runner.Result{ExitCode: 1}
			}
			response, err := Execute(context.Background(), fake, request)
			if (name == "matching" || name == "compose-created") != (err == nil && response.GetOutcome() == agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED) {
				t.Fatalf("response=%v error=%v", response, err)
			}
			for _, args := range fake.commands {
				if len(args) < 2 || args[0] != "network" || args[1] != "ls" && args[1] != "inspect" {
					t.Fatalf("existing network mutated: %v", args)
				}
			}
		})
	}
}

func TestManagedNetworkEnsureValidatesRenderedAuthorityBeforeCommands(t *testing.T) {
	for _, name := range []string{"external", "wrong-name", "invalid-subnet", "missing-owner", "multiple-subnets", "default-driver", "authored-label"} {
		t.Run(name, func(t *testing.T) {
			request, dockerName := networkEnsureRequest(t)
			artifact := request.Plan.Artifacts[0]
			content := string(artifact.CanonicalYaml)
			switch name {
			case "external":
				content += "    external: true\n"
			case "wrong-name":
				content = strings.ReplaceAll(content, dockerName, "other")
			case "invalid-subnet":
				content = strings.ReplaceAll(content, "10.60.0.0/24", "10.60.0.1/24")
			case "missing-owner":
				content = strings.ReplaceAll(
					content,
					"com.groundplane.managed: 'true'",
					"com.groundplane.managed: 'false'",
				)
			case "multiple-subnets":
				content = strings.ReplaceAll(
					content,
					"- subnet: 10.60.0.0/24",
					"- subnet: 10.60.0.0/24\n        - subnet: 10.61.0.0/24",
				)
			case "default-driver":
				content = strings.ReplaceAll(content, "    driver: bridge\n", "")
			case "authored-label":
				content += "      operator-label: preserved\n"
			}
			artifact.CanonicalYaml = []byte(content)
			hash := sha256.Sum256(artifact.CanonicalYaml)
			artifact.YamlSha256 = hash[:]
			request.Plan.PlanHash = nil
			var err error
			request.Plan, err = executionplan.Seal(request.Plan)
			if err != nil {
				t.Fatal(err)
			}
			physical := networkInspection(t, dockerName, "10.60.0.0/24", true)
			if name == "authored-label" {
				var actual inspectedOwnedNetwork
				if err := json.Unmarshal(physical, &actual); err != nil {
					t.Fatal(err)
				}
				actual.Labels["operator-label"] = "preserved"
				physical, err = json.Marshal(actual)
				if err != nil {
					t.Fatal(err)
				}
			}
			fake := &managedNetworkRunner{
				results: []runner.Result{{}, {Stdout: []byte(strings.Repeat("a", 64))}, {Stdout: physical}},
			}
			response, err := Execute(context.Background(), fake, request)
			valid := name == "default-driver" || name == "authored-label"
			if valid != (err == nil && response.GetOutcome() == agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED) ||
				!valid && len(fake.commands) != 0 {
				t.Fatalf("response=%v error=%v commands=%v", response, err, fake.commands)
			}
		})
	}
}
