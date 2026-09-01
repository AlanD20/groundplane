package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/compose-spec/compose-go/v2/loader"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func TestMutateEnvironmentServiceArtifactProjectsOwnedZoneNetwork(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	zoneID := ids.NewAt(ids.KindNetwork, now, 2)
	artifact, err := MutateEnvironmentServiceArtifact(&agentpb.ComposeArtifact{
		ArtifactId:    ids.NewAt(ids.KindConfig, now, 3),
		OwnerKind:     agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:       environmentID,
		ProjectName:   "gp-" + strings.ToLower(environmentID),
		CanonicalYaml: []byte("services: {}\n"),
	}, ServiceArtifactMutation{
		Action: ServiceArtifactCreate,
		Desired: core.Service{
			ID: ids.NewAt(ids.KindService, now, 4), Name: "proof", Image: "caddy",
			Zones: []string{"frontend"}, Strategy: core.StrategyRecreate,
			OnFailure: core.OnFailureSwitchBack, Restart: "unless-stopped", Replicas: 1,
		},
		Zones: []ServiceArtifactZone{{
			ID: zoneID, Name: "frontend", Subnet: "10.120.250.0/25",
		}},
		ArtifactID: ids.NewAt(ids.KindConfig, now, 5), PlanID: ids.NewAt(ids.KindPlan, now, 6),
		TenantID: ids.NewAt(ids.KindTenant, now, 7), ProjectID: ids.NewAt(ids.KindProject, now, 8),
		RenderGeneration: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	project, err := loader.LoadWithContext(context.Background(), composetypes.ConfigDetails{
		WorkingDir:  "/",
		ConfigFiles: []composetypes.ConfigFile{{Filename: "compose.yaml", Content: artifact.CanonicalYaml}},
	}, func(options *loader.Options) {
		options.ResolvePaths = false
		options.SkipResolveEnvironment = true
		options.SetProjectName(artifact.ProjectName, true)
	})
	if err != nil {
		t.Fatal(err)
	}
	network := project.Networks["frontend"]
	if network.External || network.Name != "gp_net_"+zoneID || len(artifact.Networks) != 1 ||
		artifact.Networks[0].NetworkId != zoneID || artifact.Networks[0].ComposeName != "frontend" {
		t.Fatalf("frontend network = %#v", network)
	}
}

func TestProjectEnvironmentServiceNativeComposePreservesOperatorFields(t *testing.T) {
	t.Parallel()
	environmentID := ids.NewAt(ids.KindEnvironment, time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC), 1)
	native, err := ProjectEnvironmentServiceNativeCompose(&agentpb.ComposeArtifact{
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:   environmentID,
		CanonicalYaml: []byte(`services:
  api:
    image: example/api:1
    command: [serve]
    labels:
      example.role: api
      com.groundplane.managed: "true"
    x-gp-resource: {kind: service, id: svc_generated}
`),
	}, "api")
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"image: example/api:1", "command:", "example.role: api"} {
		if !strings.Contains(native, fragment) {
			t.Fatalf("native Compose = %q, missing %q", native, fragment)
		}
	}
	if strings.Contains(native, "com.groundplane.") || strings.Contains(native, "x-gp-resource") {
		t.Fatalf("native Compose retains Controller-owned metadata: %q", native)
	}
}

func TestServiceArtifactRejectsComponentManagedOrdinaryActions(t *testing.T) {
	// Rationale: an ordinary Service detail or mutation must not bypass Component
	// ownership merely because the managed Service is present in the Compose artifact.
	t.Parallel()
	at := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	serviceID := ids.NewAt(ids.KindService, at, 2)
	componentID := ids.NewAt(ids.KindComponent, at, 3)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(ids.KindConfig, at, 4),
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: environmentID,
		CanonicalYaml: []byte("services:\n  caddy:\n    image: caddy:2\n"),
		Services: []*agentpb.ComposeService{{
			ServiceId: serviceID, ComposeName: "caddy",
			ExpectedLabels: []*agentpb.LabelPair{{Key: composeLabelComponentID, Value: componentID}},
		}},
	}
	if _, err := ProjectEnvironmentServiceNativeCompose(
		artifact, "caddy",
	); !isArtifactErrorKind(err, errs.KindServiceNotFound) {
		t.Fatalf("ProjectEnvironmentServiceNativeCompose(generated) error = %v", err)
	}
	for _, action := range []ServiceArtifactAction{ServiceArtifactEdit, ServiceArtifactRemove} {
		_, err := MutateEnvironmentServiceArtifact(artifact, ServiceArtifactMutation{
			Action: action,
			Desired: core.Service{
				ID: serviceID, Name: "caddy", Image: "caddy:2", Strategy: core.StrategyRecreate,
				OnFailure: core.OnFailureSwitchBack, Replicas: 1,
			},
			ArtifactID: ids.NewAt(ids.KindConfig, at, 5), PlanID: ids.NewAt(ids.KindPlan, at, 6),
			TenantID: ids.NewAt(ids.KindTenant, at, 7), ProjectID: ids.NewAt(ids.KindProject, at, 8),
			RenderGeneration: 2,
		})
		if !isArtifactErrorKind(err, errs.KindResourceInUse) {
			t.Fatalf("MutateEnvironmentServiceArtifact(%s, generated) error = %v", action, err)
		}
	}
}

func isArtifactErrorKind(err error, kind errs.Kind) bool {
	actual, ok := errs.KindOf(err)
	return ok && actual == kind
}
