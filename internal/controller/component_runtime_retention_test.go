package controller

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: the second render used for a native Blueprint candidate must not
// overwrite the source ownership retained before immutable desired staging.
func TestBlueprintCandidateRetainsFrozenComponentRuntime(t *testing.T) {
	reader, _, task := routeRemovalPlanTestState(t)
	projection := reader.projection
	task.RenderGeneration = int32(projection.RenderGeneration + 1)
	prior := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, prior); err != nil {
		t.Fatal(err)
	}
	input := etcd.ReleaseRenderInput{
		PlanID: task.PlanID, ArtifactID: prior.ArtifactId,
		TenantID: reader.tenant.ID, ProjectID: reader.project.ID, EnvironmentID: reader.environment.ID,
		AuthorizedVolumeDir: reader.environment.VolumeDir, Projection: projection,
	}
	result, err := renderBlueprintCandidateArtifact(t.Context(), task, input, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, before := range prior.Services {
		if before.OwnerComponentId == "" {
			continue
		}
		found := false
		for _, after := range result.Services {
			if after.ServiceId == before.ServiceId {
				found = proto.Equal(after, before)
			}
		}
		if !found {
			t.Fatal("candidate rehydration changed frozen Component runtime ownership")
		}
	}
}

func TestRetainEnvironmentComponentRuntimeRejectsCorruptSources(t *testing.T) {
	for _, mutation := range []string{"digest", "owner", "duplicate", "labels"} {
		t.Run(mutation, func(t *testing.T) {
			current := componentRetentionArtifact("plan_01ARZ3NDEKTSV4RRFFQ69G5FAW", "2")
			prior := componentRetentionArtifact("plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", "1")
			switch mutation {
			case "digest":
				prior.YamlSha256[0] ^= 1
			case "owner":
				prior.OwnerId = "foreign"
			case "duplicate":
				prior.Services = append(prior.Services, proto.CloneOf(prior.Services[0]))
			case "labels":
				prior.Services[0].ExpectedLabels[2].Value = "01"
			}
			encoded, err := proto.Marshal(prior)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := RetainEnvironmentComponentRuntime(current, encoded); err == nil {
				t.Fatal("accepted corrupt runtime authority")
			}
		})
	}
}

// Rationale: file contents are outside Compose; advancing only Task ownership
// must not restart an identical Component, but changed runtime must reconcile.
func TestRetainEnvironmentComponentRuntimeOnlyWhenIdentical(t *testing.T) {
	for _, change := range []string{"unchanged", "image", "user", "mount", "network", "metadata", "native"} {
		t.Run(change, func(t *testing.T) {
			prior := componentRetentionArtifact("plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", "1")
			current := componentRetentionArtifact("plan_01ARZ3NDEKTSV4RRFFQ69G5FAW", "2")
			switch change {
			case "image":
				current.CanonicalYaml = []byte(
					strings.ReplaceAll(string(current.CanonicalYaml), "image: pinned", "image: changed"),
				)
			case "user":
				current.CanonicalYaml = append(current.CanonicalYaml, []byte("    user: 123:123\n")...)
			case "mount":
				current.CanonicalYaml = append(
					current.CanonicalYaml,
					[]byte("    volumes: [\"/different:/config\"]\n")...)
			case "network":
				current.CanonicalYaml = append(current.CanonicalYaml, []byte("networks:\n  new: {}\n")...)
			case "metadata":
				current.Services[0].ExpectedReplicas = 2
			case "native":
				prior.Services[0].OwnerComponentId = ""
				current.Services[0].OwnerComponentId = ""
			}
			digest := sha256.Sum256(current.CanonicalYaml)
			current.YamlSha256 = digest[:]
			before := proto.CloneOf(current)
			encoded, err := proto.Marshal(prior)
			if err != nil {
				t.Fatal(err)
			}
			result, err := RetainEnvironmentComponentRuntime(current, encoded)
			if err != nil {
				t.Fatal(err)
			}
			want := current.Services[0]
			if change == "unchanged" {
				want = prior.Services[0]
			}
			if !proto.Equal(result.Services[0], want) {
				t.Fatal("wrong Component ownership retained")
			}
			if change == "unchanged" &&
				!strings.Contains(string(result.CanonicalYaml), "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV") {
				t.Fatal("canonical YAML did not retain source ownership")
			}
			if !proto.Equal(current, before) {
				t.Fatal("retention mutated its input")
			}
		})
	}
}

func componentRetentionArtifact(plan, generation string) *agentpb.ComposeArtifact {
	value := []byte(
		"services:\n  router:\n    image: pinned\n    labels:\n      com.groundplane.component-id: cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV\n      com.groundplane.plan-id: " + plan + "\n      com.groundplane.render-generation: \"" + generation + "\"\n",
	)
	digest := sha256.Sum256(value)
	return &agentpb.ComposeArtifact{
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:   "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", ProjectName: "gp-environment", AuthorizedVolumeDir: "/volume",
		CanonicalYaml: value, YamlSha256: digest[:],
		Services: []*agentpb.ComposeService{{
			ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ComposeName: "router",
			OwnerComponentId: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ExpectedReplicas: 1,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: composeLabelComponentID, Value: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
				{Key: composeLabelPlanID, Value: plan}, {Key: composeLabelRenderGen, Value: generation},
			},
		}},
	}
}
