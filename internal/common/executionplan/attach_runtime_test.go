package executionplan

import (
	"bytes"
	"context"
	"crypto/sha256"
	"reflect"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// QA: ATT-08, ATT-10. Native captured-artifact proof, not live settlement.
// Rationale: Attach/Detach changes only selected workload network edges, never
// its image, command, proxy or unrelated definitions. Re-delivery is repeatable.
func TestAttachConsumerTransitionPreservesCapturedRuntime(t *testing.T) {
	ctx := context.Background()
	baseline := nativeAttachBaseline(t)
	original := proto.CloneOf(baseline)
	networkID := ids.New(ids.KindNetwork)
	connected, err := PrepareAttachConsumerConnection(ctx, baseline, baseline.OwnerId,
		"env_01ARZ3NDEKTSV4RRFFQ69G5FAW", candidateRuntimeServiceID, networkID, ids.New(ids.KindConfig))
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := PrepareAttachConsumerConnection(ctx, baseline, baseline.OwnerId,
		connected.BackingEnvironmentId, connected.ServiceId, networkID, connected.Candidate.ArtifactId)
	if err != nil || !proto.Equal(connected, repeated) {
		t.Fatalf("repeated connection changed captured mutation: %v", err)
	}
	if !proto.Equal(baseline, original) {
		t.Fatal("connection mutated captured baseline")
	}
	names, err := AttachConsumerNames(connected)
	if err != nil || !reflect.DeepEqual(names, []string{"api--blue"}) {
		t.Fatalf("selected workloads: %v, %v", names, err)
	}
	before := nativeAttachDocument(t, baseline)
	after := nativeAttachDocument(t, connected.Candidate)
	key := "gp_attach_" + strings.ToLower(networkID)
	services := after["services"].(map[string]any)
	networks := services["api--blue"].(map[string]any)["networks"].(map[string]any)
	if _, ok := networks[key]; !ok {
		t.Fatal("selected workload did not join backing network")
	}
	dockerName, err := networkname.New(networkID)
	if err != nil {
		t.Fatal(err)
	}
	declaration := after["networks"].(map[string]any)[key].(map[string]any)
	if declaration["name"] != dockerName || declaration["external"] != true {
		t.Fatalf("wrong backing network: %v", declaration)
	}
	delete(networks, key)
	delete(after["networks"].(map[string]any), key)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("connection changed more than the selected network edge")
	}
	disconnected, err := PrepareAttachConsumerTransition(ctx, connected.Candidate, baseline.OwnerId,
		connected.BackingEnvironmentId, connected.ServiceId, networkID, ids.New(ids.KindConfig))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, nativeAttachDocument(t, disconnected.Candidate)) {
		t.Fatal("detach failed to restore original network membership")
	}
}

// Rationale: detaching one consumer cannot erase an external network still
// referenced by another Service in the same captured Compose artifact.
func TestAttachConsumerTransitionRetainsSharedBackingNetwork(t *testing.T) {
	baseline := nativeAttachBaseline(t)
	networkID := ids.New(ids.KindNetwork)
	key := "gp_attach_" + strings.ToLower(networkID)
	dockerName, err := networkname.New(networkID)
	if err != nil {
		t.Fatal(err)
	}
	mutateNativeAttachYAML(t, baseline, func(document map[string]any) {
		document["networks"].(map[string]any)[key] = map[string]any{"name": dockerName, "external": true}
		document["services"].(map[string]any)["worker"].(map[string]any)["networks"].(map[string]any)[key] = map[string]any{}
	})
	connected, err := PrepareAttachConsumerConnection(context.Background(), baseline, baseline.OwnerId,
		"env_01ARZ3NDEKTSV4RRFFQ69G5FAW", candidateRuntimeServiceID, networkID, ids.New(ids.KindConfig))
	if err != nil {
		t.Fatal(err)
	}
	disconnected, err := PrepareAttachConsumerTransition(context.Background(), connected.Candidate, baseline.OwnerId,
		connected.BackingEnvironmentId, connected.ServiceId, networkID, ids.New(ids.KindConfig))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nativeAttachDocument(t, baseline), nativeAttachDocument(t, disconnected.Candidate)) {
		t.Fatal("detach disturbed another Service's backing network")
	}
}

// Rationale: network-only authority cannot carry desired Service edits,
// altered proxy/shared resources or an ambiguous workload selection.
func TestAttachConsumerTransitionRejectsWidenedMutation(t *testing.T) {
	baseline := nativeAttachBaseline(t)
	connected, err := PrepareAttachConsumerConnection(context.Background(), baseline, baseline.OwnerId,
		"env_01ARZ3NDEKTSV4RRFFQ69G5FAW", candidateRuntimeServiceID, ids.New(ids.KindNetwork), ids.New(ids.KindConfig))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*agentpb.AttachConsumerTransition)
	}{
		{"desired image", func(r *agentpb.AttachConsumerTransition) {
			mutateNativeAttachYAML(t, r.Candidate, func(d map[string]any) {
				d["services"].(map[string]any)["api--blue"].(map[string]any)["image"] = "sha256:" + strings.Repeat(
					"b",
					64,
				)
			})
		}},
		{"desired command", func(r *agentpb.AttachConsumerTransition) {
			mutateNativeAttachYAML(t, r.Candidate, func(d map[string]any) {
				d["services"].(map[string]any)["api--blue"].(map[string]any)["command"] = []any{
					"unrequested",
					"command",
				}
			})
		}},
		{"proxy", func(r *agentpb.AttachConsumerTransition) {
			mutateNativeAttachYAML(t, r.Candidate, func(d map[string]any) {
				d["services"].(map[string]any)["api"].(map[string]any)["image"] = "proxy:changed"
			})
		}},
		{"shared network", func(r *agentpb.AttachConsumerTransition) {
			mutateNativeAttachYAML(t, r.Candidate, func(d map[string]any) {
				d["networks"].(map[string]any)["shared"].(map[string]any)["name"] = "changed-shared"
			})
		}},
		{"release authority", func(r *agentpb.AttachConsumerTransition) { r.Candidate.Services[0].ExpectedReplicas++ }},
		{"foreign environment", func(r *agentpb.AttachConsumerTransition) { r.EnvironmentId = r.BackingEnvironmentId }},
		{"different action", func(r *agentpb.AttachConsumerTransition) {
			r.Action = agentpb.AttachNetworkAction_ATTACH_NETWORK_ACTION_DISCONNECT
		}},
		{"duplicate workload", func(r *agentpb.AttachConsumerTransition) {
			r.Baseline.Services = append(r.Baseline.Services, proto.CloneOf(r.Baseline.Services[1]))
			r.Candidate.Services = append(r.Candidate.Services, proto.CloneOf(r.Candidate.Services[1]))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			modified := proto.CloneOf(connected)
			test.mutate(modified)
			if err := ValidateAttachConsumerTransition(context.Background(), modified); err == nil {
				t.Fatal("widened mutation accepted")
			}
		})
	}
}

func nativeAttachBaseline(t *testing.T) *agentpb.ComposeArtifact {
	t.Helper()
	artifact := candidateRuntimeFixtureArtifact(t, ids.New(ids.KindConfig), testPlanID, 7,
		candidateRuntimeReleaseID, "blue", candidateRuntimeProxyConfig(candidateRuntimeReleaseID, "blue", 7), false)
	artifact.Services = append(
		artifact.Services,
		&agentpb.ComposeService{ServiceId: candidateRuntimeOtherService, ComposeName: "worker", ExpectedReplicas: 1},
	)
	mutateNativeAttachYAML(t, artifact, func(document map[string]any) {
		document["services"].(map[string]any)["worker"] = map[string]any{
			"image": "worker:captured", "command": []any{"process", "captured-input"},
			"networks": map[string]any{"shared": map[string]any{}},
		}
	})
	return artifact
}

func mutateNativeAttachYAML(t *testing.T, artifact *agentpb.ComposeArtifact, mutate func(map[string]any)) {
	t.Helper()
	var document map[string]any
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil {
		t.Fatal(err)
	}
	mutate(document)
	raw, err := yaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	artifact.CanonicalYaml = raw
	digest := sha256.Sum256(raw)
	artifact.YamlSha256 = bytes.Clone(digest[:])
}

func nativeAttachDocument(t *testing.T, artifact *agentpb.ComposeArtifact) map[string]any {
	t.Helper()
	project, err := loadCapturedComposeProject(context.Background(), artifact)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := project.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	return document
}
