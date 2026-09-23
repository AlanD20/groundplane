package volume

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Rationale: authored Service identities intentionally have no execution labels;
// Volume operations must preserve that stream without weakening runtime checks.
func TestVolumeMutationPreservesAuthoredServicesWithoutExecutionLabels(t *testing.T) {
	at := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	serviceID := ids.NewAt(ids.KindService, at, 2)
	volumeID := ids.NewAt(ids.KindVolume, at, 3)
	planID := ids.NewAt(ids.KindPlan, at, 4)
	environment := testhierarchy.EnvironmentRecord{ID: environmentID, VolumeDir: "/var/lib/groundplane/vol/test"}
	runtime := &agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(ids.KindConfig, at, 5), OwnerId: environmentID,
		OwnerKind:   agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		ProjectName: "gp-" + strings.ToLower(environmentID), AuthorizedVolumeDir: environment.VolumeDir,
		CanonicalYaml: []byte("services:\n  app:\n    image: app:1\n    labels:\n" +
			"      com.groundplane.plan-id: " + planID + "\n      com.groundplane.render-generation: '1'\n"),
		Services: []*agentpb.ComposeService{
			{ServiceId: serviceID, ComposeName: "app", ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.plan-id", Value: planID}, {Key: "com.groundplane.render-generation", Value: "1"},
			}},
		},
	}
	runtimeBytes, err := proto.Marshal(runtime)
	if err != nil {
		t.Fatal(err)
	}
	current := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, ComposeArtifact: runtimeBytes,
		NormalizedCompose: []byte("services:\n  app:\n    image: app:1\n"),
		DesiredServices: []testservices.EnvironmentServiceProjection{
			{Desired: core.Service{ID: serviceID, Name: "app"}},
		},
	}
	corruptRuntime := proto.Clone(runtime).(*agentpb.ComposeArtifact)
	corruptRuntime.Services[0].ExpectedLabels = nil
	corruptCurrent := current
	corruptCurrent.ComposeArtifact, err = proto.Marshal(corruptRuntime)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, buildErr := buildVolumeMutationProjection(
		ids.NewAt(ids.KindTenant, at, 6), ids.NewAt(ids.KindProject, at, 7), environment,
		corruptCurrent, true, volumeMutationRequest{
			action: volumeMutationActionAdd, volumeID: volumeID, slug: "data", key: "data",
		}, ids.NewAt(ids.KindTask, at, 8), 2,
	); buildErr == nil {
		t.Fatal("runtime artifact without execution labels was accepted")
	}
	for index, action := range []string{volumeMutationActionAdd, volumeMutationActionEdit, volumeMutationActionRemove} {
		request := volumeMutationRequest{action: action, volumeID: volumeID, slug: "data", key: "data"}
		candidate, _, next, buildErr := buildVolumeMutationProjection(
			ids.NewAt(ids.KindTenant, at, 6), ids.NewAt(ids.KindProject, at, 7), environment,
			current, true, request, ids.NewAt(ids.KindTask, at, int64(index+8)), uint64(index+2),
		)
		if buildErr != nil {
			t.Fatalf("%s Volume: %v", action, buildErr)
		}
		if !strings.Contains(string(candidate.NormalizedCompose), "image: app:1") ||
			strings.Contains(string(candidate.NormalizedCompose), "com.groundplane.plan-id") ||
			len(
				next.Services,
			) != 1 || next.Services[0].ServiceId != serviceID || len(next.Services[0].ExpectedLabels) != 2 {
			t.Fatalf("%s changed the authored Service or runtime identity", action)
		}
		if action != volumeMutationActionRemove {
			var authored struct {
				Volumes map[string]map[string]any `yaml:"volumes"`
			}
			if err := yaml.Unmarshal(candidate.NormalizedCompose, &authored); err != nil {
				t.Fatal(err)
			}
			volume := authored.Volumes["data"]
			if volume["x-gp-slug"] != "data" || volume["driver_opts"] != nil {
				t.Fatalf("%s stored runtime Volume bind as authored intent: %#v", action, volume)
			}
		}
		candidate.ComposeArtifact, err = proto.Marshal(next)
		if err != nil {
			t.Fatal(err)
		}
		current = candidate
	}
}
