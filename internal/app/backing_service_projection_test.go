package app

import (
	"crypto/sha256"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: backing creation must publish the adapter Service in the authoritative
// desired topology before projection preflight validates its managed Volume mount.
func TestBackingServiceCreationProjectionIncludesGeneratedTopology(t *testing.T) {
	t.Parallel()
	const (
		projectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		revisionID    = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		serviceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		networkID     = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		volumeID      = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		volumeDir     = "/var/lib/groundplane/vol/backing/project/main"
	)
	canonical := []byte("services:\n  postgres:\n    image: postgres:16-alpine\nvolumes:\n  data: {}\n")
	digest := sha256.Sum256(canonical)
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: environmentID, AuthorizedVolumeDir: volumeDir,
		CanonicalYaml: canonical, YamlSha256: digest[:],
		Services: []*agentpb.ComposeService{{ServiceId: serviceID, ComposeName: "postgres"}},
		Volumes:  []*agentpb.ComposeVolume{{VolumeId: volumeID, ComposeName: "data"}},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	zone := etcd.ZoneRecord{EnvironmentID: environmentID, Desired: core.Zone{
		ID: networkID, Name: "data", Subnet: "10.23.2.0/28",
		OwnerKind: core.ZoneOwnerBackingProject, OwnerID: projectID,
	}}
	service := etcd.ServiceRecord{
		EnvironmentID: environmentID, BackingNetworkID: networkID,
		Desired: core.Service{
			ID: serviceID, Name: "postgres", Image: "postgres:16-alpine",
			Zones: []string{networkID}, Strategy: core.StrategyRecreate, Adapter: "postgres:16",
			Authentication: core.BackingAuthenticationPassword,
			Mounts:         []core.Mount{{Volume: volumeID, Mount: "/var/lib/postgresql/data"}},
		},
	}
	projection := buildBackingServiceCreationProjection(
		environmentID, revisionID,
		controller.ComposeIdentitySnapshot{
			Services: []controller.ComposeResourceIdentity{{ID: serviceID, Name: "postgres"}},
			Networks: []controller.ComposeResourceIdentity{{ID: networkID, Name: "data"}},
			Volumes:  []controller.ComposeResourceIdentity{{ID: volumeID, Name: "data"}},
		},
		map[string]string{"data": "data"},
		[]etcd.EnvironmentServiceVolumeMount{{
			ServiceID: serviceID, VolumeID: volumeID, Target: "/var/lib/postgresql/data",
		}},
		artifact, canonical, zone, service, nil,
	)
	if len(projection.DesiredZones) != 1 || projection.DesiredZones[0].Desired.ID != networkID ||
		len(projection.DesiredServices) != 1 || projection.DesiredServices[0].Desired.ID != serviceID ||
		projection.DesiredServices[0].BackingNetworkID != networkID {
		t.Fatalf("backing desired topology = %#v / %#v", projection.DesiredZones, projection.DesiredServices)
	}
	if projection.DesiredServices[0].Desired.Authentication != core.BackingAuthenticationPassword {
		t.Fatalf("backing authentication was not preserved: %#v", projection.DesiredServices[0].Desired)
	}
	if _, err := desiredrevision.PreflightProjection(projection); err != nil {
		t.Fatalf("PreflightProjection() error = %v", err)
	}
}
