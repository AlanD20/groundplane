package composerender

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

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
				current.CanonicalYaml = append(
					current.CanonicalYaml,
					[]byte("    networks: [new]\nnetworks:\n  new: {}\n")...)
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

// Rationale: BP-05/H29: normal native Deploy adds proxy configs to the mixed
// artifact. Unreferenced resources must not restart an unchanged Component;
// changes to a resource it actually consumes must still reconcile.
func TestComponentRetentionScopesResourceChangesToConsumer(t *testing.T) {
	for _, section := range []string{"networks", "volumes", "configs", "secrets"} {
		for _, change := range []string{"unrelated-added", "unrelated-changed", "consumed-changed", "consumed-missing"} {
			t.Run(section+"/"+change, func(t *testing.T) {
				prior := componentRetentionArtifact("plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", "1")
				current := componentRetentionArtifact("plan_01ARZ3NDEKTSV4RRFFQ69G5FAW", "2")
				reference := "    " + section + ": [used]\n"
				if section == "volumes" {
					reference = "    volumes:\n      - type: volume\n        source: used\n        target: /data\n"
				}
				for _, artifact := range []*agentpb.ComposeArtifact{prior, current} {
					artifact.CanonicalYaml = append(artifact.CanonicalYaml,
						[]byte(reference+section+":\n  used: {name: original}\n  unrelated: {name: original}\n")...)
				}
				switch change {
				case "unrelated-added":
					current.CanonicalYaml = append(current.CanonicalYaml, []byte("  native-proxy: {name: added}\n")...)
				case "unrelated-changed":
					current.CanonicalYaml = []byte(
						strings.ReplaceAll(
							string(current.CanonicalYaml),
							"unrelated: {name: original}",
							"unrelated: {name: changed}",
						),
					)
				case "consumed-changed":
					current.CanonicalYaml = []byte(
						strings.ReplaceAll(
							string(current.CanonicalYaml),
							"  used: {name: original}",
							"  used: {name: changed}",
						),
					)
				case "consumed-missing":
					current.CanonicalYaml = []byte(
						strings.ReplaceAll(string(current.CanonicalYaml), "  used: {name: original}\n", ""),
					)
				}
				for _, artifact := range []*agentpb.ComposeArtifact{prior, current} {
					digest := sha256.Sum256(artifact.CanonicalYaml)
					artifact.YamlSha256 = digest[:]
				}
				before, priorBefore := proto.CloneOf(current), proto.CloneOf(prior)
				encoded, err := proto.Marshal(prior)
				if err != nil {
					t.Fatal(err)
				}
				result, err := RetainEnvironmentComponentRuntime(current, encoded)
				if err != nil {
					t.Fatal(err)
				}
				want := current.Services[0]
				if strings.HasPrefix(change, "unrelated-") {
					want = prior.Services[0]
				}
				if !proto.Equal(result.Services[0], want) {
					t.Fatal("resource change selected the wrong Component runtime ownership")
				}
				var actual, desired yaml.Node
				if yaml.Unmarshal(result.CanonicalYaml, &actual) != nil ||
					yaml.Unmarshal(current.CanonicalYaml, &desired) != nil {
					t.Fatal("result or desired YAML is invalid")
				}
				actualResources, actualErr := yaml.Marshal(componentRetentionValue(actual.Content[0], section))
				desiredResources, desiredErr := yaml.Marshal(componentRetentionValue(desired.Content[0], section))
				if actualErr != nil || desiredErr != nil || !bytes.Equal(actualResources, desiredResources) {
					t.Fatal("Component retention replaced desired shared resources")
				}
				if !proto.Equal(current, before) || !proto.Equal(prior, priorBefore) {
					t.Fatal("retention mutated input authority")
				}
			})
		}
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
