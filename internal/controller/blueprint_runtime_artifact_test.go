package controller

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
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
	merged, err := RetainBlueprintNativeRuntime(current, prior, []string{"native"})
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
	if _, err := RetainBlueprintNativeRuntime(changed, prior, []string{"native"}); err == nil {
		t.Fatal("changed native config was accepted")
	}
}

// Rationale: both blue/green workload sources must survive one Service-level
// replacement; the historical source must never contribute a second proxy.
func TestRetainBlueprintNativeRuntimeSourcesKeepsBothSlotsAndOneProxy(t *testing.T) {
	current := &agentpb.ComposeArtifact{
		OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:             "environment",
		ProjectName:         "project",
		AuthorizedVolumeDir: "/volume",
		CanonicalYaml:       []byte("services:\n  api:\n    image: desired\n"),
		Services:            []*agentpb.ComposeService{{ServiceId: "native", ComposeName: "api"}},
	}
	active := proto.CloneOf(current)
	active.CanonicalYaml = []byte("services:\n  api:\n    image: proxy\n  api--green:\n    image: sealed-current\n")
	active.Services = []*agentpb.ComposeService{
		{ServiceId: "native", ComposeName: "api", Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY},
		{
			ServiceId:   "native",
			ComposeName: "api--green",
			Slot:        "green",
			Role:        agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
		},
	}
	prior := proto.CloneOf(current)
	prior.CanonicalYaml = []byte("services:\n  api--blue:\n    image: sealed-prior\n")
	prior.Services = []*agentpb.ComposeService{
		{
			ServiceId:   "native",
			ComposeName: "api--blue",
			Slot:        "blue",
			Role:        agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
		},
	}
	mixed, err := RetainBlueprintNativeRuntimeSources(
		current,
		[]*agentpb.ComposeArtifact{active, prior},
		[]string{"native"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(mixed.Services) != 3 || !strings.Contains(string(mixed.CanonicalYaml), "sealed-current") ||
		!strings.Contains(string(mixed.CanonicalYaml), "sealed-prior") {
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
		if _, err := RetainBlueprintNativeRuntimeSources(current, []*agentpb.ComposeArtifact{active, broken}, []string{"native"}); err == nil {
			t.Fatalf("accepted %s source", bad)
		}
	}
}

// Rationale: retained-resource equality is semantic for YAML mappings, while
// still rejecting changes to network configuration and ownership values.
func TestRetainBlueprintRuntimeIgnoresNestedResourceMappingOrder(t *testing.T) {
	current, _, retained := retainedNetworkOrderingArtifacts()
	if _, err := RetainBlueprintNativeRuntime(current, retained, []string{"native"}); err != nil {
		t.Fatal("equivalent reordered retained network was rejected", err)
	}
	for name, changed := range map[string]string{
		"subnet":          "10.96.11.0/24",
		"ownership label": "false",
	} {
		t.Run(name, func(t *testing.T) {
			drifted := proto.CloneOf(retained)
			if name == "subnet" {
				drifted.CanonicalYaml = []byte(strings.ReplaceAll(
					string(drifted.CanonicalYaml), "10.96.10.0/24", changed,
				))
			} else {
				drifted.CanonicalYaml = []byte(strings.ReplaceAll(
					string(drifted.CanonicalYaml), `com.groundplane.managed: "true"`,
					`com.groundplane.managed: "`+changed+`"`,
				))
			}
			if _, err := RetainBlueprintNativeRuntime(current, drifted, []string{"native"}); err == nil {
				t.Fatal("retained network drift was accepted")
			}
		})
	}
}

// Rationale: two physical sources for one retained Service may serialize the
// same nested resource in different map order, but may not disagree on values.
func TestRetainBlueprintRuntimeSourcesIgnoreNestedResourceMappingOrder(t *testing.T) {
	current, proxy, workload := retainedNetworkOrderingArtifacts()
	if _, err := RetainBlueprintNativeRuntimeSources(
		current, []*agentpb.ComposeArtifact{proxy, workload}, []string{"native"},
	); err != nil {
		t.Fatal("equivalent reordered source networks were rejected", err)
	}
	drifted := proto.CloneOf(workload)
	drifted.CanonicalYaml = []byte(strings.ReplaceAll(
		string(drifted.CanonicalYaml), "10.96.10.0/24", "10.96.11.0/24",
	))
	if _, err := RetainBlueprintNativeRuntimeSources(
		current, []*agentpb.ComposeArtifact{proxy, drifted}, []string{"native"},
	); err == nil {
		t.Fatal("disagreeing retained source network was accepted")
	}
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
