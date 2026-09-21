package composerender

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: direct Zone creation must materialize one canonical managed
// network without changing existing Service membership or workload fields.
func TestZoneArtifactCreationAddsCanonicalNetworkAndPreservesWorkloads(t *testing.T) {
	now := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	projectID := ids.NewAt(ids.KindProject, now, 2)
	zoneID := ids.NewAt(ids.KindNetwork, now, 3)
	serviceID := ids.NewAt(ids.KindService, now, 4)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(ids.KindConfig, now, 5),
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: environmentID,
		CanonicalYaml: []byte("services:\n  api:\n    image: example.invalid/api:1\n"),
		Services:      []*agentpb.ComposeService{{ServiceId: serviceID, ComposeName: "api"}},
	}
	mutated, err := AddEnvironmentZoneArtifact(artifact, ZoneArtifactAddition{
		Zone: core.Zone{
			ID: zoneID, Name: "private", Subnet: "10.34.20.0/24", Internal: true,
			OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
		},
		ProjectID: projectID, ArtifactID: ids.NewAt(ids.KindConfig, now, 6),
		PlanID: ids.NewAt(ids.KindPlan, now, 7), RenderGeneration: 2,
	})
	if err != nil {
		t.Fatalf("AddEnvironmentZoneArtifact() error = %v", err)
	}
	if len(mutated.GetNetworks()) != 1 || mutated.GetNetworks()[0].GetNetworkId() != zoneID ||
		len(mutated.GetServices()) != 1 || mutated.GetServices()[0].GetServiceId() != serviceID {
		t.Fatalf("mutated artifact resources = %#v / %#v", mutated.GetNetworks(), mutated.GetServices())
	}
	canonical := string(mutated.GetCanonicalYaml())
	for _, fragment := range []string{
		"image: example.invalid/api:1", "private:", "name: gp_net_" + zoneID,
		"subnet: 10.34.20.0/24", "internal: true", "com.groundplane.environment-id: " + environmentID,
	} {
		if !strings.Contains(canonical, fragment) {
			t.Fatalf("mutated canonical YAML %q does not contain %q", canonical, fragment)
		}
	}
}

// Rationale: Zone removal must detach every affected Service in the exact
// candidate artifact before the managed Docker network is removed.
func TestZoneRemovalArtifactDetachesServicesAndPreservesWorkloads(t *testing.T) {
	now := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	zoneID := ids.NewAt(ids.KindNetwork, now, 2)
	otherZoneID := ids.NewAt(ids.KindNetwork, now, 3)
	serviceID := ids.NewAt(ids.KindService, now, 4)
	otherServiceID := ids.NewAt(ids.KindService, now, 5)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(ids.KindConfig, now, 6),
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: environmentID,
		CanonicalYaml: []byte(
			"services:\n  api:\n    image: example.invalid/api:1\n    networks:\n      frontend: {}\n      backend: {}\n  worker:\n    image: example.invalid/worker:1\n    networks:\n      backend: {}\nnetworks:\n  frontend: {}\n  backend: {}\n",
		),
		Services: []*agentpb.ComposeService{
			{ServiceId: serviceID, ComposeName: "api"},
			{ServiceId: otherServiceID, ComposeName: "worker"},
		},
		Networks: []*agentpb.ComposeNetwork{
			{NetworkId: zoneID, ComposeName: "frontend", DockerName: "gp_net_" + zoneID},
			{NetworkId: otherZoneID, ComposeName: "backend", DockerName: "gp_net_" + otherZoneID},
		},
	}
	mutated, err := MutateEnvironmentZoneArtifact(artifact, ZoneArtifactMutation{
		ZoneID: zoneID, ZoneName: "frontend", ArtifactID: ids.NewAt(ids.KindConfig, now, 7),
		PlanID: ids.NewAt(ids.KindPlan, now, 8), RenderGeneration: 2,
	})
	if err != nil {
		t.Fatalf("MutateEnvironmentZoneArtifact() error = %v", err)
	}
	if len(mutated.Networks) != 1 || mutated.Networks[0].NetworkId != otherZoneID || len(mutated.Services) != 2 {
		t.Fatalf("mutated artifact resources = %#v / %#v", mutated.Networks, mutated.Services)
	}
	want := "services:\n    api:\n        image: example.invalid/api:1\n        networks:\n            backend: {}\n    worker:\n        image: example.invalid/worker:1\n        networks:\n            backend: {}\nnetworks:\n    backend: {}\n"
	if string(mutated.CanonicalYaml) != want {
		t.Fatalf("mutated canonical YAML = %q, want %q", mutated.CanonicalYaml, want)
	}
}
