package executionplan

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const remoteNetworkEnv = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
const remoteNetworkID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
const remoteSecondID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAW"
const remoteNetworkSeed = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestRemoteReleaseNetworksPrepareOnlyOwnedUsedBridges(t *testing.T) {
	source := remoteReleaseFixture()
	intent := sha256.Sum256([]byte("accepted remote deploy"))
	got, err := RemoteReleaseNetworkWorkInputs(source, intent[:], remoteNetworkSeed)
	if err != nil || len(got) != 2 {
		t.Fatalf("remote preparation got %d inputs, %v", len(got), err)
	}
	for index, want := range []string{remoteNetworkID, remoteSecondID} {
		input := got[index]
		if input.GetAction() != agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE ||
			input.GetNetworkId() != want || input.GetEnvironmentId() != remoteNetworkEnv ||
			len(input.GetCandidate().GetNetworks()) != 1 ||
			strings.Contains(string(input.GetCandidate().GetCanonicalYaml()), "unused:") ||
			strings.Contains(string(input.GetCandidate().GetCanonicalYaml()), "services:") {
			t.Fatalf("input %d prepared a different effect: %v", index, input)
		}
		if err := ValidateRemoteReleaseNetworkWorkInput(input); err != nil {
			t.Fatal(err)
		}
	}
	again, err := RemoteReleaseNetworkWorkInputs(proto.CloneOf(source), intent[:], remoteNetworkSeed)
	if err != nil || len(again) != len(got) || !proto.Equal(got[0], again[0]) || !proto.Equal(got[1], again[1]) {
		t.Fatalf("derived inputs changed on replay: %v", err)
	}
}

func TestRemoteReleaseNetworksDenyUnsealedOrSharedTopology(t *testing.T) {
	intent := sha256.Sum256([]byte("accepted remote deploy"))
	for _, change := range []struct {
		name, old, replacement string
	}{
		{"external", "internal: true", "internal: true\n    external: true"},
		{"not isolated", "internal: true", "internal: false"},
		{"shared DNS", "internal: true", "internal: true\n    x-gp-allow-dns: true"},
		{"different driver", "driver: bridge", "driver: macvlan"},
		{"managed bridge custom driver options", "driver: bridge", "driver: bridge\n    driver_opts: {example: invalid}"},
		{"implicit default", "networks: {zeta: {}, alpha: {}}", "networks: {}"},
		{"host network", "networks: {zeta: {}, alpha: {}}", "network_mode: host"},
		{"unowned bridge", "networks: {zeta: {}, alpha: {}}", "networks: {unused: {}}"},
		{"undeclared bridge", "networks: {zeta: {}, alpha: {}}", "networks: {other: {}}"},
		{"altered rendered name", "gp_net_" + remoteNetworkID, "wrong-owner"},
		{"altered rendered labels", "com.groundplane.managed: 'true'", "com.groundplane.managed: 'false'"},
		{"altered resource owner", "parent: {environment_id: " + remoteNetworkEnv + "}", "parent: {environment_id: else}"},
		{"reserved ownership label", "com.groundplane.kind: network", "com.docker.compose.project: wrong"},
		{"invalid subnet", "10.62.0.0/24", "10.62.0.1/24"},
	} {
		t.Run(change.name, func(t *testing.T) {
			source := remoteReleaseFixture()
			source.CanonicalYaml = []byte(
				strings.Replace(string(source.CanonicalYaml), change.old, change.replacement, 1),
			)
			resealRemoteTestArtifact(source)
			inputs, err := RemoteReleaseNetworkWorkInputs(source, intent[:], remoteNetworkSeed)
			if err == nil || len(inputs) != 0 {
				t.Fatalf("unsupported bridges were admitted: %v, %v", inputs, err)
			}
		})
	}
}

func TestRemoteNetworkReopenPreservesIsolationAndLegacyLocalDNSIdentity(t *testing.T) {
	source := remoteReleaseFixture()
	source.CanonicalYaml = []byte(strings.Replace(string(source.CanonicalYaml),
		"internal: true", "internal: true\n    x-gp-allow-dns: true", 1))
	resealRemoteTestArtifact(source)
	intent := sha256.Sum256([]byte("accepted local deploy"))
	local, err := ReleaseNetworkWorkInputs(source, intent[:], remoteNetworkSeed)
	if err != nil || len(local) != 1 || local[0].GetNetworkId() != remoteNetworkID {
		t.Fatalf("local DNS selection changed: %v, %v", local, err)
	}
	at, err := ids.Timestamp(ids.KindConfig, remoteNetworkSeed)
	if err != nil {
		t.Fatal(err)
	}
	want := ids.DeriveAt(ids.KindConfig, at, remoteNetworkSeed, "dns-network/"+remoteNetworkID)
	if local[0].GetCandidate().GetArtifactId() != want {
		t.Fatal("local DNS work identity changed")
	}
	if err := ValidateRemoteReleaseNetworkWorkInput(local[0]); err == nil {
		t.Fatal("reopened local DNS bridge as a remote prerequisite")
	}

	remote, err := RemoteReleaseNetworkWorkInputs(remoteReleaseFixture(), intent[:], remoteNetworkSeed)
	if err != nil || len(remote) != 2 {
		t.Fatalf("valid remote selection failed: %v, %v", remote, err)
	}
	input := remote[0]
	input.Candidate.CanonicalYaml = []byte(strings.Replace(string(input.Candidate.CanonicalYaml),
		"internal: true", "internal: false", 1))
	resealRemoteTestArtifact(input.Candidate)
	if err := ValidateNetworkWorkInput(input); err != nil {
		t.Fatalf("ordinary bridge validation failed: %v", err)
	}
	if err := ValidateRemoteReleaseNetworkWorkInput(input); err == nil {
		t.Fatal("reopened unisolated bridge as a remote prerequisite")
	}
}

func remoteReleaseFixture() *agentpb.ComposeArtifact {
	project := "gp-" + strings.ToLower(remoteNetworkEnv)
	content := fmt.Sprintf("name: %s\nservices:\n  app:\n    networks: {zeta: {}, alpha: {}}\n"+
		"  proxy:\n    networks: {alpha: {}}\nnetworks:\n", project)
	bridge := func(composeName, networkID, subnet string) (string, *agentpb.ComposeNetwork) {
		dockerName := "gp_net_" + networkID
		return fmt.Sprintf(`  %s:
    name: %s
    driver: bridge
    internal: true
    labels:
      com.groundplane.environment-id: %s
      com.groundplane.kind: network
      com.groundplane.managed: 'true'
      operator-purpose: application
    ipam:
      config:
        - subnet: %s
    x-gp-resource:
      kind: network
      id: %s
      parent: {environment_id: %s}
`, composeName, dockerName, remoteNetworkEnv, subnet, networkID, remoteNetworkEnv), &agentpb.ComposeNetwork{
				ComposeName: composeName, NetworkId: networkID, DockerName: dockerName,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: labelEnvironmentID, Value: remoteNetworkEnv},
					{Key: labelKind, Value: "network"},
					{Key: labelManaged, Value: "true"},
				},
			}
	}
	first, firstIdentity := bridge("zeta", remoteNetworkID, "10.62.0.0/24")
	second, secondIdentity := bridge("alpha", remoteSecondID, "10.62.1.0/24")
	source := &agentpb.ComposeArtifact{
		ArtifactId: remoteNetworkSeed, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: remoteNetworkEnv, ProjectName: project, AuthorizedVolumeDir: "/srv/" + remoteNetworkEnv,
		Networks:      []*agentpb.ComposeNetwork{firstIdentity, secondIdentity},
		CanonicalYaml: []byte(content + first + second + "  unused: {external: true}\n"),
	}
	resealRemoteTestArtifact(source)
	return source
}

func resealRemoteTestArtifact(artifact *agentpb.ComposeArtifact) {
	digest := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = digest[:]
}
