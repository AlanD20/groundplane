package composerender

import (
	"crypto/sha256"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// ENTRY-04: editing configuration after a native Release cannot replace its
// slot/proxy lineage or activate pending image/command changes from desired state.
func TestEntryMutationPreservesNativeReleaseOwnership(t *testing.T) {
	environment, service := ids.New(ids.KindEnvironment), ids.New(ids.KindService)
	release := ids.New(ids.KindDeployment)
	raw := []byte(
		"services:\n  api--blue:\n    image: app:captured\n    command: [serve, captured]\n  api:\n    image: proxy:captured\n    command: [proxy, captured]\n",
	)
	digest := sha256.Sum256(raw)
	artifact := &agentpb.ComposeArtifact{ArtifactId: ids.New(ids.KindConfig),
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: environment,
		ProjectName: "gp-" + strings.ToLower(environment), CanonicalYaml: raw, YamlSha256: digest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/" + environment,
		Services: []*agentpb.ComposeService{
			{ServiceId: service, ComposeName: "api", ExpectedReplicas: 1,
				Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
				ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.runtime-role", Value: "proxy"}}},
			{ServiceId: service, ComposeName: "api--blue", ExpectedReplicas: 1, Slot: "blue", HasHealthcheck: true,
				Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
				ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.release-id", Value: release},
					{
						Key:   "com.groundplane.runtime-role",
						Value: "slot",
					}, {Key: "com.groundplane.slot", Value: "blue"}}},
		}}
	planID := ids.New(ids.KindPlan)
	for _, member := range artifact.Services {
		member.ExpectedLabels = append(member.ExpectedLabels,
			&agentpb.LabelPair{Key: composeLabelPlanID, Value: planID},
			&agentpb.LabelPair{Key: composeLabelRenderGen, Value: "1"})
		slices.SortFunc(member.ExpectedLabels, func(left, right *agentpb.LabelPair) int {
			return strings.Compare(left.Key, right.Key)
		})
	}
	encoded, err := proto.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	projection := environmentprojection.EnvironmentComposeProjection{EnvironmentID: environment,
		RevisionID: ids.New(ids.KindTask), RenderGeneration: 1, ComposeArtifact: encoded,
		DesiredServices: []services.EnvironmentServiceProjection{{EnvironmentID: environment,
			Desired: core.Service{
				ID:      service,
				Name:    "api",
				Image:   "app:pending",
				Command: []string{"serve", "pending"},
			}}}}
	entry := entries.Record{EnvironmentID: environment, CurrentValueGenerationID: ids.New(ids.KindConfig),
		Entry: core.EnvEntry{ID: ids.New(ids.KindEnvEntry), Kind: core.EntryKindEnv, Key: "MODE",
			Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"}}}
	candidate, _, err := ProjectEnvironmentEntryMutation(projection, EnvironmentEntryArtifactMutation{
		RevisionID: ids.New(ids.KindTask), ArtifactID: ids.New(ids.KindConfig), PlanID: ids.New(ids.KindPlan),
		RenderGeneration: 2, Entries: []entries.Record{entry}})
	if err != nil {
		t.Fatal(err)
	}
	mutated := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(candidate.ComposeArtifact, mutated); err != nil {
		t.Fatal(err)
	}
	for index, member := range artifact.Services {
		if !proto.Equal(member, mutated.Services[index]) {
			t.Fatal("Entry mutation replaced native Release ownership")
		}
	}
	var document struct {
		Services map[string]struct {
			Image   string   `yaml:"image"`
			Command []string `yaml:"command"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(mutated.CanonicalYaml, &document); err != nil {
		t.Fatal(err)
	}
	if document.Services["api--blue"].Image != "app:captured" ||
		strings.Join(document.Services["api--blue"].Command, " ") != "serve captured" ||
		document.Services["api"].Image != "proxy:captured" {
		t.Fatal("configuration edit activated pending image/command edits or changed the stable proxy")
	}
}
