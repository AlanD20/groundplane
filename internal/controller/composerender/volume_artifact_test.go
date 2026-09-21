package composerender

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestMutateEnvironmentVolumeArtifactAddsOwnedBindVolume(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	volumeID := ids.NewAt(ids.KindVolume, at, 2)
	artifact, err := MutateEnvironmentVolumeArtifact(&agentpb.ComposeArtifact{
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:   environmentID, AuthorizedVolumeDir: "/var/lib/groundplane/vol/tenant/project/" + environmentID,
		CanonicalYaml: []byte("services: {}\nnetworks: {}\n"),
	}, VolumeArtifactMutation{
		Action: VolumeArtifactAdd, VolumeID: volumeID, Key: "websocket-data",
		ArtifactID: ids.NewAt(ids.KindConfig, at, 3), PlanID: ids.NewAt(ids.KindPlan, at, 4),
		TenantID: ids.NewAt(ids.KindTenant, at, 5), ProjectID: ids.NewAt(ids.KindProject, at, 6),
		RenderGeneration: 2,
	})
	if err != nil {
		t.Fatalf("MutateEnvironmentVolumeArtifact() error = %v", err)
	}
	wantPath := "/var/lib/groundplane/vol/tenant/project/" + environmentID + "/websocket-data"
	if len(artifact.GetVolumes()) != 1 || artifact.GetVolumes()[0].GetVolumeId() != volumeID ||
		!strings.Contains(string(artifact.GetCanonicalYaml()), wantPath) ||
		!strings.Contains(string(artifact.GetCanonicalYaml()), "x-gp-resource:") {
		t.Fatalf("mutated artifact = %#v\n%s", artifact, artifact.GetCanonicalYaml())
	}
}

func TestMutateEnvironmentVolumeArtifactRemovalDetachesConsumers(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 26, 12, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	volumeID := ids.NewAt(ids.KindVolume, at, 2)
	current := &agentpb.ComposeArtifact{
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:   environmentID, AuthorizedVolumeDir: "/var/lib/groundplane/vol/tenant/project/" + environmentID,
		CanonicalYaml: []byte(
			"services:\n  websocket:\n    image: websocket:1\n    volumes:\n      - websocket-data:/app/storage\n      - type: bind\n        source: /tmp/socket\n        target: /run/socket\nvolumes:\n  websocket-data:\n    name: gp_vol_" + strings.ToLower(
				volumeID,
			) + "\n",
		),
		Volumes: []*agentpb.ComposeVolume{{
			VolumeId: volumeID, ComposeName: "websocket-data", DockerName: "gp_vol_" + strings.ToLower(volumeID),
		}},
	}
	artifact, err := MutateEnvironmentVolumeArtifact(current, VolumeArtifactMutation{
		Action: VolumeArtifactRemove, VolumeID: volumeID, Key: "websocket-data",
		ArtifactID: ids.NewAt(ids.KindConfig, at, 3), PlanID: ids.NewAt(ids.KindPlan, at, 4),
		TenantID: ids.NewAt(ids.KindTenant, at, 5), ProjectID: ids.NewAt(ids.KindProject, at, 6),
		RenderGeneration: 3,
	})
	if err != nil {
		t.Fatalf("MutateEnvironmentVolumeArtifact() error = %v", err)
	}
	canonical := string(artifact.GetCanonicalYaml())
	if len(artifact.GetVolumes()) != 0 || strings.Contains(canonical, "websocket-data") ||
		!strings.Contains(canonical, "/tmp/socket") {
		t.Fatalf("mutated artifact = %#v\n%s", artifact, canonical)
	}
	if !strings.Contains(string(current.GetCanonicalYaml()), "websocket-data:/app/storage") {
		t.Fatal("MutateEnvironmentVolumeArtifact() mutated its input")
	}
}
