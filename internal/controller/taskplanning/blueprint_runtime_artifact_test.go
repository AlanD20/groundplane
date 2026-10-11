package taskplanning

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Rationale: retained native references may preserve their own proxy config,
// but must neither restore nor veto obsolete Component-only resource config.
func TestRetainBlueprintRuntimeScopesResourcesToNativeReferences(t *testing.T) {
	prior := &agentpb.ComposeArtifact{
		OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:             "environment",
		ProjectName:         "project",
		AuthorizedVolumeDir: "/volume",
		CanonicalYaml: []byte(
			"services:\n  api--proxy:\n    image: old-proxy\n    configs:\n      - source: native-proxy\n        target: /config\n  managed:\n    image: old-managed\n    configs:\n      - source: obsolete-managed\n        target: /config\nconfigs:\n  native-proxy:\n    content: native\n  obsolete-managed:\n    content: obsolete\n  current-managed:\n    content: old\n",
		),
		Services: []*agentpb.ComposeService{
			{
				ServiceId:   "native",
				ComposeName: "api--proxy",
				Role:        agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			},
			{ServiceId: "managed", ComposeName: "managed", OwnerComponentId: "component"},
		},
	}
	current := &agentpb.ComposeArtifact{
		OwnerKind:           prior.OwnerKind,
		OwnerId:             prior.OwnerId,
		ProjectName:         prior.ProjectName,
		AuthorizedVolumeDir: prior.AuthorizedVolumeDir,
		CanonicalYaml: []byte(
			"services:\n  api:\n    image: authored\n  managed:\n    image: current-managed\n    configs:\n      - source: current-managed\n        target: /config\nconfigs:\n  current-managed:\n    content: new\n",
		),
		Services: []*agentpb.ComposeService{
			{ServiceId: "native", ComposeName: "api"},
			{ServiceId: "managed", ComposeName: "managed", OwnerComponentId: "component"},
		},
	}
	merged, err := composerender.RetainBlueprintNativeRuntime(current, prior, []string{"native"})
	if err != nil {
		t.Fatal(err)
	}
	yaml := string(merged.CanonicalYaml)
	if !strings.Contains(yaml, "content: native") || !strings.Contains(yaml, "content: new") ||
		strings.Contains(yaml, "obsolete") ||
		strings.Contains(yaml, "old-managed") ||
		strings.Contains(yaml, "image: authored") {
		t.Fatalf("resource retention escaped native references: %s", yaml)
	}
	if !proto.Equal(merged.Services[1], prior.Services[0]) {
		t.Fatal("native physical metadata was not retained exactly")
	}
	if string(current.CanonicalYaml) == yaml {
		t.Fatal("merge mutated or returned current artifact")
	}
	changed := proto.CloneOf(current)
	changed.CanonicalYaml = append(changed.CanonicalYaml, []byte("  native-proxy:\n    content: changed\n")...)
	if _, err := composerender.RetainBlueprintNativeRuntime(changed, prior, []string{"native"}); err == nil {
		t.Fatal("changed native config was accepted")
	}
}

// Rationale: both blue/green workload sources must survive one Service-level
// replacement; the historical source must never contribute a second proxy.
func TestRetainBlueprintNativeRuntimeSourcesKeepsBothSlotsAndOneProxy(t *testing.T) {
	current, active, prior := acknowledgedRuntimeArtifacts(t)
	serviceID := active.Services[0].ServiceId
	mixed, err := RetainBlueprintNativeRuntimeSources(
		current,
		[]serviceruntimerecord.Record{acknowledgedRuntimeSource(t, active, prior)},
		[]string{serviceID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(mixed.Services) != 3 || !strings.Contains(string(mixed.CanonicalYaml), strings.Repeat("c", 64)) ||
		!strings.Contains(string(mixed.CanonicalYaml), strings.Repeat("b", 64)) {
		t.Fatal("retaining inactive slot erased current members")
	}
	for _, bad := range []string{"duplicate", "proxy", "foreign-owner", "component"} {
		broken := proto.CloneOf(prior)
		switch bad {
		case "duplicate":
			broken = proto.CloneOf(active)
		case "proxy":
			broken.Services[0].Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY
		case "foreign-owner":
			broken.OwnerId = "foreign"
		case "component":
			broken.Services[0].OwnerComponentId = "component"
		}
		if _, err := RetainBlueprintNativeRuntimeSources(
			current,
			[]serviceruntimerecord.Record{
				acknowledgedRuntimeSource(t, active, broken),
			},
			[]string{serviceID},
		); err == nil {
			t.Fatalf("accepted %s source", bad)
		}
	}
}

// Rationale: retained-resource equality is semantic for YAML mappings, while
// still rejecting changes to network configuration and ownership values.
func TestRetainBlueprintRuntimeIgnoresNestedResourceMappingOrder(t *testing.T) {
	current, _, retained := retainedNetworkOrderingArtifacts()
	if _, err := composerender.RetainBlueprintNativeRuntime(current, retained, []string{"native"}); err != nil {
		t.Fatal("equivalent reordered retained network was rejected", err)
	}
	for name, changed := range map[string]string{
		"subnet":          "10.96.11.0/24",
		"ownership label": "false",
	} {
		t.Run(name, func(t *testing.T) {
			drifted := proto.CloneOf(retained)
			if name == "subnet" {
				drifted.CanonicalYaml = []byte(
					strings.ReplaceAll(string(drifted.CanonicalYaml), "10.96.10.0/24", changed),
				)
			} else {
				drifted.CanonicalYaml = []byte(
					strings.ReplaceAll(string(drifted.CanonicalYaml), `com.groundplane.managed: "true"`,
						`com.groundplane.managed: "`+changed+`"`,
					),
				)
			}
			if _, err := composerender.RetainBlueprintNativeRuntime(current, drifted, []string{"native"}); err == nil {
				t.Fatal("retained network drift was accepted")
			}
		})
	}
}

// Rationale: two physical sources for one retained Service may serialize the
// same nested resource in different map order, but may not disagree on values.
func TestRetainBlueprintRuntimeSourcesIgnoreNestedResourceMappingOrder(t *testing.T) {
	current, active, retained := acknowledgedRuntimeArtifacts(t)
	serviceID := active.Services[0].ServiceId
	var document yaml.Node
	if err := yaml.Unmarshal(retained.CanonicalYaml, &document); err != nil {
		t.Fatal(err)
	}
	root := document.Content[0]
	networks := composerender.EnsureMappingValue(root, "networks")
	for index := 1; index < len(networks.Content); index += 2 {
		mapping := networks.Content[index]
		for left, right := 0, len(mapping.Content)-2; left < right; left, right = left+2, right-2 {
			mapping.Content[left], mapping.Content[right] = mapping.Content[right], mapping.Content[left]
			mapping.Content[left+1], mapping.Content[right+1] = mapping.Content[right+1], mapping.Content[left+1]
		}
	}
	var err error
	retained.CanonicalYaml, err = yaml.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(retained.CanonicalYaml)
	retained.YamlSha256 = digest[:]
	if _, err := RetainBlueprintNativeRuntimeSources(
		current, []serviceruntimerecord.Record{acknowledgedRuntimeSource(t, active, retained)}, []string{serviceID},
	); err != nil {
		t.Fatal("equivalent reordered source networks were rejected", err)
	}
	drifted := proto.CloneOf(retained)
	drifted.CanonicalYaml = []byte(
		strings.ReplaceAll(string(drifted.CanonicalYaml), "frontend:", "frontend:\n    internal: true"),
	)
	driftedDigest := sha256.Sum256(drifted.CanonicalYaml)
	drifted.YamlSha256 = driftedDigest[:]
	if _, err := RetainBlueprintNativeRuntimeSources(
		current, []serviceruntimerecord.Record{acknowledgedRuntimeSource(t, active, drifted)}, []string{serviceID},
	); err == nil {
		t.Fatal("disagreeing retained source network was accepted")
	}
}

func acknowledgedRuntimeArtifacts(
	t *testing.T,
) (*agentpb.ComposeArtifact, *agentpb.ComposeArtifact, *agentpb.ComposeArtifact) {
	t.Helper()
	_, _, input := redeployRestorationInput(t, domain.StrategyBlueGreen)
	source := input.Members[0].Render
	current := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(source.Projection.ComposeArtifact, current); err != nil {
		t.Fatal(err)
	}
	source.CandidateWorkload.LocalImageID = "sha256:" + strings.Repeat("c", 64)
	active := nativeRuntimeFixture(t, source, true)
	source.ArtifactID, source.ReleaseID = source.PriorArtifactID, input.Members[0].Intent.PriorServingReleaseID
	source.CandidateTarget, source.Slot = source.PriorTarget, source.PriorSlot
	source.ProxyGeneration = source.PriorProxyGeneration
	source.CandidateWorkload.LocalImageID = "sha256:" + strings.Repeat("b", 64)
	return current, active, nativeRuntimeFixture(t, source, false)
}

// These explicit acknowledgement fields model a completed fixture deployment.
// Do not validate here: rejection cases deliberately corrupt captured artifacts.
func acknowledgedRuntimeSource(t *testing.T, active, retained *agentpb.ComposeArtifact) serviceruntimerecord.Record {
	t.Helper()
	runtime := executionplan.CandidateRuntime{}
	for _, member := range active.Services {
		runtime.ServiceID = member.ServiceId
		if member.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			runtime.ProxyConfigSHA256 = member.ProxyConfigSha256
			continue
		}
		runtime.Target = member.Slot
		for _, label := range member.ExpectedLabels {
			if label.Key == "com.groundplane.release-id" {
				runtime.ReleaseID = label.Value
			}
		}
	}
	for _, member := range active.Services {
		if member.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			generation, err := executionplan.ProxyConfigGeneration(
				member.ProxyImplementation,
				member.ProxyConfigJson,
				runtime.ReleaseID,
			)
			if err != nil {
				t.Fatal(err)
			}
			runtime.ProxyGeneration = generation
		}
	}
	var err error
	runtime.CurrentArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(active)
	if err != nil {
		t.Fatal(err)
	}
	runtime.RetainedPriorArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(retained)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("completed fixture deployment"))
	return serviceruntimerecord.Record{EnvironmentID: active.OwnerId, Runtime: runtime,
		Source: serviceruntimerecord.Acknowledgement{
			TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			StepID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", AgentID: "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			AssignmentID: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV", ExecutionEpoch: 1, RenderGeneration: 1,
			PlanHash: hex.EncodeToString(digest[:]), EffectDigest: hex.EncodeToString(digest[:]),
			AcknowledgedAt: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC),
		}}
}

func retainedNetworkOrderingArtifacts() (
	*agentpb.ComposeArtifact,
	*agentpb.ComposeArtifact,
	*agentpb.ComposeArtifact,
) {
	current := &agentpb.ComposeArtifact{
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:   "environment", ProjectName: "project", AuthorizedVolumeDir: "/volume",
		CanonicalYaml: []byte(`services:
  api:
    image: desired
    networks:
      backend: {}
networks:
  backend:
    name: gp_net_backend
    ipam:
      config:
        - subnet: 10.96.10.0/24
    labels:
      com.groundplane.kind: network
      com.groundplane.managed: "true"
`),
		Services: []*agentpb.ComposeService{{ServiceId: "native", ComposeName: "api"}},
		Networks: []*agentpb.ComposeNetwork{{
			NetworkId: "network", ComposeName: "backend", DockerName: "gp_net_backend",
		}},
	}
	proxy := proto.CloneOf(current)
	proxy.Services = []*agentpb.ComposeService{{
		ServiceId: "native", ComposeName: "api", Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	}}
	workload := proto.CloneOf(current)
	workload.CanonicalYaml = []byte(`services:
  api--green:
    networks:
      backend: {}
    image: sealed
networks:
  backend:
    labels:
      com.groundplane.managed: "true"
      com.groundplane.kind: network
    ipam:
      config:
        - subnet: 10.96.10.0/24
    name: gp_net_backend
`)
	workload.Services = []*agentpb.ComposeService{{
		ServiceId: "native", ComposeName: "api--green", Slot: "green",
		Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
	}}
	return current, proxy, workload
}
