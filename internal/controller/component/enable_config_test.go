package component

import (
	"context"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testidempotencyowner "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"gopkg.in/yaml.v3"
)

type enableBlueprintFixture struct {
	bundle           core.BlueprintBundle
	calls            int
	revision         string
	expectedRevision string
}

func (fixture *enableBlueprintFixture) GetBlueprint(
	_ context.Context, environmentID string,
) (apiTypes.EnvironmentBlueprintDocument, error) {
	return apiTypes.EnvironmentBlueprintDocument{
		EnvironmentID: environmentID, Revision: fixture.revision,
		Document: string(fixture.bundle.Files[0].Content),
	}, nil
}

func (fixture *enableBlueprintFixture) ApplyComponentBlueprint(
	_ context.Context,
	_, _ string,
	bundle core.BlueprintBundle,
	expectedRevision string,
	_ string,
) (testidempotencyowner.IdempotencyResponse, error) {
	fixture.calls++
	fixture.expectedRevision = expectedRevision
	fixture.bundle = bundle
	return testidempotencyowner.IdempotencyResponse{
		Status: 202,
		Body:   []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}, nil
}

// CMP-01: first enable must author the missing declaration in one guarded Apply;
// enabling an authored router must preserve its saved configuration without an
// explicit replacement, and neither path may replace unrelated desired state.
func TestEnableComponentAuthorsConfigurationInOneApply(t *testing.T) {
	zones := []string{"net_01ARZ3NDEKTSV4RRFFQ69G5FAX", "net_01ARZ3NDEKTSV4RRFFQ69G5FAW"}
	input := apiTypes.ComponentConfigMutationInput{Caddy: &apiTypes.CaddyComponentConfigMutationInput{ZoneIDs: zones}}
	for _, test := range []struct {
		name       string
		components string
		config     *apiTypes.ComponentConfigMutationInput
		alias      string
		template   string
		reject     bool
	}{
		{name: "missing section", config: &input},
		{name: "missing router", components: "x-gp-components:\n  edge-tunnel:\n    implementation: cloudflare-tunnel\n    enabled: false\n", config: &input},
		{name: "existing router with replacement", components: "x-gp-components:\n  http-router:\n    implementation: caddy\n    enabled: false\n", config: &input},
		{name: "existing router without replacement", components: "x-gp-components:\n  http-router:\n    implementation: caddy\n    enabled: false\n    settings:\n      zone_ids: [net_01ARZ3NDEKTSV4RRFFQ69G5FAX, net_01ARZ3NDEKTSV4RRFFQ69G5FAW]\n      alias: saved-router\n    implementation_config:\n      caddyfile_template: |\n        {gp.routes}\n", alias: "saved-router", template: "{gp.routes}\n"},
		{name: "wrong implementation", components: "x-gp-components:\n  http-router:\n    implementation: cloudflare-tunnel\n    enabled: false\n", config: &input, reject: true},
		{name: "malformed section", components: "x-gp-components: broken\n", config: &input, reject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := "services:\n  app:\n    image: example/app:qa\n" + test.components
			fixture := &enableBlueprintFixture{revision: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", bundle: core.BlueprintBundle{
				RootPath: "compose.yaml", Files: []core.BlueprintFile{{Path: "compose.yaml", Content: []byte(original)}},
			}}
			service := &MutationService{
				components: managedConfigReadRepository{record: testcomponents.Record{Desired: testcomponents.DesiredRecord{
					ID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Owner: core.ComponentOwnerEnvironment,
					OwnerID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", Kind: core.ComponentKindIngressCaddy,
				}}}, applier: fixture,
			}
			_, err := service.EnableComponent(context.Background(), "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				apiTypes.ComponentEnableRequest{Config: test.config}, "enable-key-123456")
			if test.reject {
				if err == nil || fixture.calls != 0 || string(fixture.bundle.Files[0].Content) != original {
					t.Fatalf("invalid declaration published or rewritten: calls %d, error %v", fixture.calls, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var decoded struct {
				Components map[string]core.ComponentSpec     `yaml:"x-gp-components"`
				Services   map[string]struct{ Image string } `yaml:"services"`
			}
			if err := yaml.Unmarshal(fixture.bundle.Files[0].Content, &decoded); err != nil {
				t.Fatal(err)
			}
			got := decoded.Components["http-router"]
			if fixture.calls != 1 || fixture.expectedRevision != fixture.revision ||
				!got.Enabled || got.Implementation != core.ComponentKindIngressCaddy ||
				!reflect.DeepEqual(got.Settings.ZoneIDs, zones) || got.Settings.Alias != test.alias ||
				got.ImplementationConfig.CaddyfileTemplate != test.template || decoded.Services["app"].Image != "example/app:qa" {
				t.Fatalf("enable = calls %d, spec %#v, services %#v", fixture.calls, got, decoded.Services)
			}
			if test.name == "missing router" {
				other := decoded.Components["edge-tunnel"]
				if len(decoded.Components) != 2 || other.Enabled || other.Implementation != core.ComponentKindEdgeCloudflare {
					t.Fatalf("unrelated Tunnel was changed: %#v", decoded.Components)
				}
			} else if len(decoded.Components) != 1 {
				t.Fatalf("extra declarations published: %#v", decoded.Components)
			}
		})
	}
}

// Rationale: invalid placement must reject before creating credentials or
// publishing desired state, including callers that bypass JSON decoding.
func TestEnvironmentComponentConfigRejectsInvalidPlacementBeforeCredentials(t *testing.T) {
	service := &MutationService{}
	_, err := service.environmentComponentConfig(context.Background(), testcomponents.DesiredRecord{
		Kind: core.ComponentKindEdgeCloudflare,
	}, apiTypes.ComponentConfigMutationInput{CloudflareTunnel: &apiTypes.CloudflareTunnelComponentConfigMutationInput{
		Credential: apiTypes.CloudflareTunnelCredentialInput{
			Mode:     "existing",
			SecretID: "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
	}}, "enable-key-123456")
	if err == nil {
		t.Fatal("accepted Tunnel without explicit Zones")
	}
}
