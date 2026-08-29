package controller

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestProjectEnvironmentEntryMutationReplacesOnlyEntryDecorations(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	serviceID := ids.NewAt(ids.KindService, now, 2)
	volumeDir := "/var/lib/groundplane/vol/" + environmentID
	oldEntry := etcd.EntryRecord{EnvironmentID: environmentID, Entry: core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 3), Kind: core.EntryKindEnv, Key: "MODE",
		Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"},
	}, CurrentValueGenerationID: ids.NewAt(ids.KindConfig, now, 4)}
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(ids.KindConfig, now, 5),
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: environmentID,
		AuthorizedVolumeDir: volumeDir,
		CanonicalYaml:       []byte("services:\n  api:\n    image: example.test/api:1\n    env_file:\n      - path: " + filepath.Join(volumeDir, filepath.FromSlash(ServiceEnvFileName(environmentID, "api"))) + "\n        required: true\n      - path: /operator.env\n        required: true\n"),
		Services:            []*agentpb.ComposeService{{ServiceId: serviceID, ComposeName: "api"}},
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
	candidate, materializations, err := ProjectEnvironmentEntryMutation(etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 7),
		RenderGeneration: 1, ComposeArtifact: encoded,
		Services: []etcd.EnvironmentComposeIdentity{{ID: serviceID, Name: "api"}}, Entries: []etcd.EntryRecord{oldEntry},
	}, EnvironmentEntryArtifactMutation{
		RevisionID: ids.NewAt(ids.KindTask, now, 8), ArtifactID: ids.NewAt(ids.KindConfig, now, 9),
		PlanID: ids.NewAt(ids.KindPlan, now, 10), RenderGeneration: 2, Entries: []etcd.EntryRecord{newEntry},
	})
	if err != nil {
		t.Fatal(err)
	}
	mutated := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(candidate.ComposeArtifact, mutated); err != nil {
		t.Fatal(err)
	}
	yaml := string(mutated.CanonicalYaml)
	if !strings.Contains(yaml, "/operator.env") || strings.Contains(yaml, ServiceEnvFileName(environmentID, "api")) ||
		!strings.Contains(yaml, EnvFileName(environmentID)) || len(materializations) != 1 {
		t.Fatalf("mutated artifact:\n%s\nmaterializations=%#v", yaml, materializations)
	}
}
