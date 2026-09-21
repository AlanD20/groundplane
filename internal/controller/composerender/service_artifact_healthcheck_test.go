package composerender

import (
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"gopkg.in/yaml.v3"
)

// Rationale: the CLI's replicas-only edit round-trips the typed Service, which
// cannot represent an authored custom CMD healthcheck. Replica edits must retain
// that native check for the next sealed Release acknowledgement.
func TestServiceReplicaEditPreservesUnrepresentedComposeHealthcheck(t *testing.T) {
	mutated := mutateReplicaHealthcheck(t, "    healthcheck:\n      test: [CMD, php, probe.php]\n", core.Healthcheck{})
	var document struct {
		Services map[string]struct {
			Healthcheck *struct {
				Test []string `yaml:"test"`
			} `yaml:"healthcheck"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(mutated.CanonicalYaml, &document); err != nil {
		t.Fatal(err)
	}
	health := document.Services["api"].Healthcheck
	if health == nil || !slices.Equal(health.Test, []string{"CMD", "php", "probe.php"}) ||
		!mutated.Services[0].HasHealthcheck || mutated.Services[0].ExpectedReplicas != 3 {
		t.Fatal("replica edit lost authored CMD healthcheck or sealed replica/health metadata")
	}
}

// Rationale: native preservation must not disable typed clear/replacement or
// promote an explicitly disabled native check into Release health authority.
func TestServiceHealthcheckEditBoundaries(t *testing.T) {
	for _, test := range []struct {
		name, native         string
		typed                core.Healthcheck
		wantHealth, wantNode bool
	}{
		{name: "clear HTTP", native: `    healthcheck:
      test: [CMD-SHELL, "curl -fsS -- 'http://127.0.0.1/up' >/dev/null"]
      interval: 10s
      retries: 3
`},
		{name: "clear TCP", native: `    healthcheck:
      test: [CMD-SHELL, "nc -z -- 'localhost' '5432'"]
`},
		{name: "clear pgrep", native: `    healthcheck:
      test: [CMD-SHELL, "pgrep -f -- 'worker' >/dev/null"]
`},
		{name: "replace custom", native: "    healthcheck:\n      test: [CMD, php, probe.php]\n",
			typed: core.Healthcheck{HTTP: "/up"}, wantHealth: true, wantNode: true},
		{name: "native timing", native: `    healthcheck:
      test: [CMD-SHELL, "curl -fsS -- 'http://127.0.0.1/up' >/dev/null"]
      start_interval: 1s
`, wantHealth: true, wantNode: true},
		{name: "disabled", native: "    healthcheck:\n      disable: true\n      test: [CMD, php, probe.php]\n", wantNode: true},
		{name: "NONE", native: "    healthcheck:\n      test: [NONE]\n", wantNode: true},
		{name: "absent"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutated := mutateReplicaHealthcheck(t, test.native, test.typed)
			var document yaml.Node
			if err := yaml.Unmarshal(mutated.CanonicalYaml, &document); err != nil {
				t.Fatal(err)
			}
			root := document.Content[0]
			services := root.Content[MappingIndex(root, "services")+1]
			service := services.Content[MappingIndex(services, "api")+1]
			index := MappingIndex(service, "healthcheck")
			if (index >= 0) != test.wantNode || mutated.Services[0].HasHealthcheck != test.wantHealth {
				t.Fatalf(
					"health node=%t, sealed health=%t; want %t/%t",
					index >= 0,
					mutated.Services[0].HasHealthcheck,
					test.wantNode,
					test.wantHealth,
				)
			}
			if test.name == "replace custom" {
				var health struct {
					Test []string `yaml:"test"`
				}
				if err := service.Content[index+1].Decode(&health); err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(health.Test, []string{"CMD-SHELL", "curl -fsS -- 'http://127.0.0.1/up' >/dev/null"}) {
					t.Fatalf("replacement healthcheck = %v", health.Test)
				}
			}
		})
	}
}

func mutateReplicaHealthcheck(t *testing.T, native string, health core.Healthcheck) *agentpb.ComposeArtifact {
	t.Helper()
	at := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, at, 1)
	artifact := &agentpb.ComposeArtifact{
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:   ids.NewAt(ids.KindEnvironment, at, 2), ProjectName: "gp-env",
		CanonicalYaml: []byte(
			"services:\n  api:\n    image: example/api:1\n    deploy:\n      replicas: 2\n" + native,
		),
		Services: []*agentpb.ComposeService{{
			ServiceId: serviceID, ComposeName: "api", ExpectedReplicas: 2, HasHealthcheck: true,
		}},
	}
	mutated, err := MutateEnvironmentServiceArtifact(artifact, ServiceArtifactMutation{
		Action: ServiceArtifactEdit,
		Desired: core.Service{
			ID: serviceID, Name: "api", Image: "example/api:1", Replicas: 3,
			Strategy: core.StrategyRecreate, OnFailure: core.OnFailureSwitchBack, Healthcheck: health,
		},
		ArtifactID: ids.NewAt(ids.KindConfig, at, 3), PlanID: ids.NewAt(ids.KindPlan, at, 4),
		TenantID: ids.NewAt(ids.KindTenant, at, 5), ProjectID: ids.NewAt(ids.KindProject, at, 6),
		RenderGeneration: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	return mutated
}
