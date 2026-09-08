package network

import (
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: the immutable Zone candidate must remove only Zone authority and
// authored memberships while retaining every Service identity and desired record.
func TestZoneRemovalCandidatePreservesServicesAndRemovesAuthoredMembership(t *testing.T) {
	now := time.Date(2026, 8, 31, 10, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	zoneID := ids.NewAt(ids.KindNetwork, now, 2)
	otherZoneID := ids.NewAt(ids.KindNetwork, now, 3)
	serviceID := ids.NewAt(ids.KindService, now, 4)
	revisionID := ids.NewAt(ids.KindTask, now, 5)
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(
			ids.KindConfig,
			now,
			6,
		), OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: environmentID, CanonicalYaml: []byte("services:\n  api:\n    image: example.invalid/api:1\n    networks:\n      frontend: {}\n      backend: {}\nnetworks:\n  frontend: {}\n  backend: {}\n"),
		Services: []*agentpb.ComposeService{{ServiceId: serviceID, ComposeName: "api"}},
		Networks: []*agentpb.ComposeNetwork{
			{NetworkId: zoneID, ComposeName: "frontend"},
			{NetworkId: otherZoneID, ComposeName: "backend"},
		},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	current := etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 7), RenderGeneration: 1,
		ComposeArtifact: artifact, NormalizedCompose: []byte("services:\n  api:\n    image: example.invalid/api:1\n    networks:\n      frontend: {}\n      backend: {}\nnetworks:\n  frontend: {}\n  backend: {}\n"),
		DesiredZones: []etcd.EnvironmentZoneProjection{
			{
				EnvironmentID: environmentID,
				Desired: core.Zone{
					ID:        zoneID,
					Name:      "frontend",
					Subnet:    "10.0.1.0/24",
					OwnerKind: core.ZoneOwnerEnvironment,
					OwnerID:   environmentID,
				},
			},
			{
				EnvironmentID: environmentID,
				Desired: core.Zone{
					ID:        otherZoneID,
					Name:      "backend",
					Subnet:    "10.0.2.0/24",
					OwnerKind: core.ZoneOwnerEnvironment,
					OwnerID:   environmentID,
				},
			},
		},
		DesiredServices: []etcd.EnvironmentServiceProjection{
			{
				EnvironmentID: environmentID,
				Desired: core.Service{
					ID:       serviceID,
					Name:     "api",
					Image:    "example.invalid/api:1",
					Zones:    []string{"frontend", "backend"},
					Strategy: core.StrategyRecreate,
					Replicas: 1,
				},
			},
		},
	}
	candidate, affected, err := buildZoneRemovalProjection(current, zoneID, "frontend", revisionID, 2)
	if err != nil {
		t.Fatalf("buildZoneRemovalProjection() error = %v", err)
	}
	if len(candidate.DesiredZones) != 1 || candidate.DesiredZones[0].Desired.ID != otherZoneID {
		t.Fatalf("candidate Zones = %#v", candidate.DesiredZones)
	}
	if len(candidate.DesiredServices) != 1 ||
		!slices.Equal(candidate.DesiredServices[0].Desired.Zones, []string{"backend"}) ||
		!slices.Equal(affected, []string{serviceID}) {
		t.Fatalf("candidate Services/affected = %#v / %#v", candidate.DesiredServices, affected)
	}
}
