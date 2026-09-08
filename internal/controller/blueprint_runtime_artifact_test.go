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
