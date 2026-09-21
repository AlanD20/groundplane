package composerender

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testenvironmentfile "github.com/AlanD20/groundplane/internal/controller/environmentfile"
	"github.com/AlanD20/groundplane/internal/core"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: Entry exposure edits retain operator files and all other desired
// resources while preserving the existing runtime ownership.
func TestProjectEnvironmentEntryMutationReplacesOnlyEntryDecorations(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	serviceID := ids.NewAt(ids.KindService, now, 2)
	volumeDir := "/var/lib/groundplane/vol/" + environmentID
	oldEntry := testentries.Record{EnvironmentID: environmentID, Entry: core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 3), Kind: core.EntryKindEnv, Key: "MODE",
		Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"},
	}, CurrentValueGenerationID: ids.NewAt(ids.KindConfig, now, 4)}
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(ids.KindConfig, now, 5),
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: environmentID,
		AuthorizedVolumeDir: volumeDir,
		CanonicalYaml: []byte(
			"services:\n  api:\n    image: example.test/api:1\n    env_file:\n      - path: " + filepath.Join(
				volumeDir,
				filepath.FromSlash(testenvironmentfile.ServiceEnvFileName(environmentID, "api")),
			) + "\n        required: true\n      - path: /operator.env\n        required: true\n",
		),
		Services: []*agentpb.ComposeService{
			{ServiceId: serviceID, ComposeName: "api", ExpectedLabels: []*agentpb.LabelPair{
				{Key: composeLabelPlanID, Value: ids.NewAt(ids.KindPlan, now, 11)},
				{Key: composeLabelRenderGen, Value: "1"},
			}},
		},
	}
	digest := []byte(strings.Repeat("x", 32))
	artifact.YamlSha256 = digest
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	newEntry := oldEntry
	newEntry.Entry.Exposure = []string{"all"}
	newEntry.CurrentValueGenerationID = ids.NewAt(ids.KindConfig, now, 6)
	current := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 7),
		RenderGeneration: 1, ComposeArtifact: encoded, NormalizedCompose: []byte("services:\n  api:\n    image: example.test/api:1\n"),
		DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{
			{EnvironmentID: environmentID, Desired: core.Zone{ID: "zone-id", Name: "frontend"}},
		},
		DesiredServices: []testservices.EnvironmentServiceProjection{
			{EnvironmentID: environmentID, Desired: core.Service{
				ID: serviceID, Name: "api", Image: "example.test/api:1",
			}},
		},
		DesiredRoutes: []testenvironmentprojection.EnvironmentRouteProjection{
			{EnvironmentID: environmentID, Desired: core.Route{ID: "route-id", Host: "api.example.test", Path: "/"}},
		},
		Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{{ID: "volume-id", Slug: "data", Key: "data"}},
		VolumeMounts: []testenvironmentprojection.EnvironmentServiceVolumeMount{
			{ServiceID: serviceID, VolumeID: "volume-id", Target: "/data"},
		},
		Components: []testcomponents.Record{{Desired: testcomponents.DesiredRecord{ID: "component-id"}}},
		Entries:    []testentries.Record{oldEntry},
	}
	candidate, materializations, err := ProjectEnvironmentEntryMutation(current, EnvironmentEntryArtifactMutation{
		RevisionID: ids.NewAt(ids.KindTask, now, 8), ArtifactID: ids.NewAt(ids.KindConfig, now, 9),
		PlanID: ids.NewAt(ids.KindPlan, now, 10), RenderGeneration: 2, Entries: []testentries.Record{newEntry},
	})
	if err != nil {
		t.Fatal(err)
	}
	mutated := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(candidate.ComposeArtifact, mutated); err != nil {
		t.Fatal(err)
	}
	yaml := string(mutated.CanonicalYaml)
	if !strings.Contains(yaml, "/operator.env") ||
		strings.Contains(yaml, testenvironmentfile.ServiceEnvFileName(environmentID, "api")) ||
		!strings.Contains(yaml, testenvironmentfile.EnvFileName(environmentID)) ||
		len(materializations) != 1 {
		t.Fatalf("mutated artifact:\n%s\nmaterializations=%#v", yaml, materializations)
	}
	if string(candidate.NormalizedCompose) != string(current.NormalizedCompose) ||
		&candidate.DesiredZones[0] == &current.DesiredZones[0] ||
		&candidate.DesiredServices[0] == &current.DesiredServices[0] ||
		&candidate.DesiredRoutes[0] == &current.DesiredRoutes[0] ||
		&candidate.Volumes[0] == &current.Volumes[0] ||
		&candidate.VolumeMounts[0] == &current.VolumeMounts[0] ||
		&candidate.Components[0] == &current.Components[0] ||
		&candidate.Entries[0] == &current.Entries[0] {
		t.Fatal("entry mutation candidate aliases current projection or changes normalized Compose")
	}
}
