package services

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

func TestServiceMountSaveUpdatesBothDeploymentAndExportConfiguration(t *testing.T) {
	// A successful settings save must reach both deployment YAML and canonical
	// desired export; updating just the Service response would silently do nothing.
	at := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	environment := hierarchyrecord.EnvironmentRecord{
		ID:        ids.NewAt(ids.KindEnvironment, at, 1),
		VolumeDir: "/managed/env",
	}
	tenantID, projectID := ids.NewAt(ids.KindTenant, at, 2), ids.NewAt(ids.KindProject, at, 3)
	record, err := servicerecord.NewServiceRecord(environment.ID, core.Service{
		ID: ids.NewAt(
			ids.KindService,
			at,
			4,
		), Name: "app", Image: "example/app:1", Strategy: core.StrategyRecreate, OnFailure: core.OnFailureLeaveActive, Replicas: 1,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	current, err := buildServiceDesiredProjection(
		tenantID,
		projectID,
		environment,
		projectionrecord.EnvironmentComposeProjection{},
		false,
		record,
		servicerecord.ServiceMutationReferences{},
		true,
		nil,
		ids.NewAt(ids.KindTask, at, 5),
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	volumeID := ids.NewAt(ids.KindVolume, at, 6)
	current.Volumes = []projectionrecord.EnvironmentVolumeIdentity{{ID: volumeID, Slug: "renamed", Key: "data"}}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(current.ComposeArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	artifact.Volumes = []*agentpb.ComposeVolume{{VolumeId: volumeID, ComposeName: "data"}}
	artifact.CanonicalYaml = append(artifact.CanonicalYaml, []byte("volumes:\n  data: {}\n")...)
	digest := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = digest[:]
	current.ComposeArtifact, err = proto.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	current.NormalizedCompose = append(current.NormalizedCompose, []byte("volumes:\n  data: {}\n")...)
	mounts := []core.Mount{{Volume: volumeID, Mount: "/app/data", RO: true}}
	record.Desired.Mounts = mounts
	candidate, err := buildServiceDesiredProjection(
		tenantID,
		projectID,
		environment,
		current,
		true,
		record,
		servicerecord.ServiceMutationReferences{},
		false,
		&mounts,
		ids.NewAt(ids.KindTask, at, 7),
		2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(candidate.ComposeArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	for _, content := range [][]byte{candidate.NormalizedCompose, artifact.CanonicalYaml} {
		var document struct {
			Services map[string]struct {
				Image   string
				Volumes []struct {
					Source   string
					Target   string
					ReadOnly bool `yaml:"read_only"`
				}
			}
		}
		if err := yaml.Unmarshal(content, &document); err != nil {
			t.Fatal(err)
		}
		service := document.Services["app"]
		if service.Image != "example/app:1" || len(service.Volumes) != 1 || service.Volumes[0].Source != "data" ||
			service.Volumes[0].Target != "/app/data" ||
			!service.Volumes[0].ReadOnly {
			t.Fatalf("mount save did not reach Compose: %+v", service)
		}
	}
	if len(current.VolumeMounts) != 0 || len(candidate.VolumeMounts) != 1 ||
		candidate.VolumeMounts[0].VolumeID != volumeID {
		t.Fatal("incorrect Volume references or previous revision mutated")
	}
}

func TestVolumeMountReplacementPreservesOtherConsumersAndVolumeIdentity(t *testing.T) {
	// A mount edit must update removal/backup references without deleting data
	// or changing another Service's access to the same Volume.
	projection := projectionrecord.EnvironmentComposeProjection{
		Volumes: []projectionrecord.EnvironmentVolumeIdentity{
			{ID: "volume-id", Slug: "renamed-data", Key: "original-key"},
		},
		VolumeMounts: []projectionrecord.EnvironmentServiceVolumeMount{
			{ServiceID: "service-a", VolumeID: "volume-id", Target: "/old"},
			{ServiceID: "service-b", VolumeID: "volume-id", Target: "/shared"},
		},
	}
	replacement := []core.Mount{{Volume: "volume-id", Mount: "/new", RO: true}}
	rendered, err := replaceServiceVolumeMounts(&projection, "service-a", &replacement)
	if err != nil {
		t.Fatal(err)
	}
	if len(*rendered) != 1 || (*rendered)[0].Key != "original-key" || !(*rendered)[0].ReadOnly {
		t.Fatalf("wrong Compose binding: %+v", rendered)
	}
	if len(projection.VolumeMounts) != 2 || projection.VolumeMounts[0].Target != "/new" ||
		!projection.VolumeMounts[0].ReadOnly ||
		projection.VolumeMounts[1].Target != "/shared" {
		t.Fatalf("wrong mount references: %+v", projection.VolumeMounts)
	}
	empty := []core.Mount{}
	if _, err := replaceServiceVolumeMounts(&projection, "service-a", &empty); err != nil {
		t.Fatal(err)
	}
	if len(projection.Volumes) != 1 || len(projection.VolumeMounts) != 1 ||
		projection.VolumeMounts[0].ServiceID != "service-b" {
		t.Fatal("unmount removed Volume or another consumer")
	}
}

func TestVolumeMountReplacementRejectsForeignVolumesAndInvalidTargets(t *testing.T) {
	for _, mounts := range [][]core.Mount{
		{{Volume: "foreign", Mount: "/data"}},
		{{Volume: "local", Mount: "data"}},
		{{Volume: "local", Mount: "/data/../secret"}},
		{{Volume: "local", Mount: "/data\x00"}},
		{{Volume: "local", Mount: "/data"}, {Volume: "local", Mount: "/data"}},
	} {
		projection := projectionrecord.EnvironmentComposeProjection{
			Volumes: []projectionrecord.EnvironmentVolumeIdentity{{ID: "local", Key: "data"}},
		}
		if _, err := replaceServiceVolumeMounts(&projection, "service", &mounts); err == nil {
			t.Fatalf("accepted invalid mounts: %+v", mounts)
		}
		if len(projection.VolumeMounts) != 0 {
			t.Fatal("invalid request changed references")
		}
	}
}

func TestServiceEditMountOmissionAndEmptyListHaveDifferentEffects(t *testing.T) {
	current := core.Service{
		Mounts: []core.Mount{{Volume: "volume", Mount: "/data"}, {File: "config", Mount: "/etc/app", RO: true}},
	}
	if got := applyServiceEdit(current, apiTypes.ServiceEdit{}); len(got.Mounts) != 2 {
		t.Fatal("ordinary edit dropped mounts")
	}
	empty := []apiTypes.ServiceVolumeMount{}
	got := applyServiceEdit(current, apiTypes.ServiceEdit{VolumeMounts: &empty})
	if len(got.Mounts) != 1 || got.Mounts[0].File != "config" || len(current.Mounts) != 2 {
		t.Fatal("unmount must preserve files and the previous revision")
	}
}
