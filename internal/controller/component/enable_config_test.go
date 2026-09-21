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

// Rationale: enable must author placement and enabled=true in one Blueprint
// application; a separate configure Task leaves first-enable ordering ambiguous.
func TestEnableComponentAuthorsConfigurationInOneApply(t *testing.T) {
	zones := []string{"net_01ARZ3NDEKTSV4RRFFQ69G5FAX", "net_01ARZ3NDEKTSV4RRFFQ69G5FAW"}
	fixture := &enableBlueprintFixture{revision: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", bundle: core.BlueprintBundle{
		RootPath: "compose.yaml", Files: []core.BlueprintFile{{Path: "compose.yaml", Content: []byte("x-gp-components:\n  http-router:\n    implementation: caddy\n    enabled: false\n")}},
	}}
	service := &MutationService{
		components: managedConfigReadRepository{record: testcomponents.Record{Desired: testcomponents.DesiredRecord{
			ID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Owner: core.ComponentOwnerEnvironment,
			OwnerID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", Kind: core.ComponentKindIngressCaddy,
		}}}, applier: fixture,
	}
	input := apiTypes.ComponentConfigMutationInput{Caddy: &apiTypes.CaddyComponentConfigMutationInput{ZoneIDs: zones}}
	if _, err := service.EnableComponent(context.Background(), "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		apiTypes.ComponentEnableRequest{Config: &input}, "enable-key-123456"); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Components map[string]core.ComponentSpec `yaml:"x-gp-components"`
	}
	if err := yaml.Unmarshal(fixture.bundle.Files[0].Content, &decoded); err != nil {
		t.Fatal(err)
	}
	got := decoded.Components["http-router"]
	if fixture.calls != 1 || fixture.expectedRevision != fixture.revision ||
		!got.Enabled || !reflect.DeepEqual(got.Settings.ZoneIDs, zones) {
		t.Fatalf("enable = calls %d, spec %#v", fixture.calls, got)
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
