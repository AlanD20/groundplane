package volume

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Rationale: mutating a Volume must not turn an independently serving proxy and
// one retained slot into a fresh, incomplete blue/green deployment.
func TestVolumeMutationProjectionPreservesRetainedRuntime(t *testing.T) {
	for _, action := range []string{volumeMutationActionAdd, volumeMutationActionEdit, volumeMutationActionRemove} {
		for _, slot := range []domain.WorkloadTarget{domain.WorkloadBlue, domain.WorkloadGreen} {
			t.Run(action+"/"+string(slot), func(t *testing.T) {
				tenantID, projectID, environment, baseline := volumePolicyProjectionFixture(t)
				serviceID := ids.NewAt(ids.KindService, environment.CreatedAt, 50)
				planID := ids.NewAt(ids.KindPlan, environment.CreatedAt, 51)
				name, err := domain.WorkloadComposeName("api", slot)
				if err != nil {
					t.Fatal(err)
				}
				artifact := &agentpb.ComposeArtifact{}
				if err := proto.Unmarshal(baseline.ComposeArtifact, artifact); err != nil {
					t.Fatal(err)
				}
				var document yaml.Node
				if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil {
					t.Fatal(err)
				}
				var services yaml.Node
				if err := yaml.Unmarshal([]byte("api:\n  image: proxy:1\n  labels:\n    com.groundplane.plan-id: "+planID+
					"\n    com.groundplane.render-generation: '1'\n"+name+
					":\n  image: api:1\n  volumes: [scratch:/scratch, data:/data]\n  labels:\n    com.groundplane.plan-id: "+
					planID+"\n    com.groundplane.render-generation: '1'\n"), &services); err != nil {
					t.Fatal(err)
				}
				root := document.Content[0]
				for index := 0; index < len(root.Content); index += 2 {
					if root.Content[index].Value == "services" {
						root.Content[index+1] = services.Content[0]
					}
				}
				artifact.CanonicalYaml, err = yaml.Marshal(&document)
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(artifact.CanonicalYaml)
				artifact.YamlSha256 = digest[:]
				labels := func() []*agentpb.LabelPair {
					return []*agentpb.LabelPair{
						{
							Key:   "com.groundplane.plan-id",
							Value: planID,
						}, {Key: "com.groundplane.render-generation", Value: "1"},
					}
				}
				artifact.Services = []*agentpb.ComposeService{
					{
						ServiceId:      serviceID,
						ComposeName:    "api",
						Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
						ExpectedLabels: labels(),
					},
					{
						ServiceId:      serviceID,
						ComposeName:    name,
						Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
						Slot:           string(slot),
						ExpectedLabels: labels(),
					},
				}
				baseline.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
				if err != nil {
					t.Fatal(err)
				}
				baseline.NormalizedCompose = []byte(
					"services:\n  api:\n    image: api:1\n    volumes: [scratch:/scratch, data:/data]\nvolumes:\n  data: {}\n  scratch: {}\n",
				)
				baseline.DesiredServices = []etcd.EnvironmentServiceProjection{{EnvironmentID: environment.ID,
					Desired: core.Service{ID: serviceID, Name: "api", Image: "api:1"}}}
				for _, volume := range baseline.Volumes {
					baseline.VolumeMounts = append(baseline.VolumeMounts, etcd.EnvironmentServiceVolumeMount{
						ServiceID: serviceID, VolumeID: volume.ID, Target: "/" + volume.Key,
					})
				}
				if _, err := etcd.EncodeEnvironmentComposeProjectionStorage(baseline); err != nil {
					t.Fatalf("baseline: %v", err)
				}
				volume := baseline.Volumes[1]
				if action == volumeMutationActionAdd {
					volume = etcd.EnvironmentVolumeIdentity{Key: "cache", Slug: "cache"}
				}
				_, candidate, cleanup, next, err := buildVolumeMutationCandidate(
					tenantID,
					projectID,
					environment,
					baseline,
					true,
					volumeMutationRequest{action: action, environmentID: environment.ID,
						volumeID: volume.ID, key: volume.Key, slug: volume.Slug},
					ids.NewAt(ids.KindTask, environment.CreatedAt, 52),
					3,
				)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := etcd.EncodeEnvironmentComposeProjectionStorage(candidate); err != nil {
					t.Fatalf("removal publication projection: %v", err)
				}
				for index, before := range artifact.Services {
					if !proto.Equal(before, next.Services[index]) || !proto.Equal(before, cleanup.Services[index]) {
						t.Fatal("removal rewrote retained runtime ownership")
					}
				}
				if !bytes.Equal(cleanup.CanonicalYaml, artifact.CanonicalYaml) ||
					!strings.Contains(string(next.CanonicalYaml), planID) ||
					(action == volumeMutationActionRemove && strings.Contains(string(next.CanonicalYaml), "scratch")) ||
					!strings.Contains(string(next.CanonicalYaml), "data:/data") {
					t.Fatal("removal changed baseline evidence or an unrelated mount")
				}
				corrupt := proto.CloneOf(artifact)
				corrupt.Services[0].ExpectedLabels = nil
				baseline.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(corrupt)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, _, _, err := buildVolumeMutationCandidate(tenantID, projectID, environment, baseline, true,
					volumeMutationRequest{action: action, environmentID: environment.ID,
						volumeID: volume.ID, key: volume.Key, slug: volume.Slug}, ids.NewAt(ids.KindTask, environment.CreatedAt, 53), 3); err == nil {
					t.Fatal("removal accepted missing retained ownership")
				}
			})
		}
	}
}
