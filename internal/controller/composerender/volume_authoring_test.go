package composerender

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/core"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: a directly added Volume must export as parser-valid authored
// intent, without its writable host bind, while native authored options stay.
func TestDirectVolumeAuthoringRoundTrips(t *testing.T) {
	at := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	volumeID := ids.NewAt(ids.KindVolume, at, 2)
	volumeDirectory := "/var/lib/groundplane/vol/test"
	rendered, err := MutateEnvironmentVolumeArtifact(&agentpb.ComposeArtifact{
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:   environmentID, AuthorizedVolumeDir: volumeDirectory,
		CanonicalYaml: []byte(
			"services: {}\nvolumes:\n  authored:\n    driver: local\n    driver_opts:\n      o: uid=1000\n",
		),
	}, VolumeArtifactMutation{
		Action: VolumeArtifactAdd, VolumeID: volumeID, Key: "data",
		ArtifactID: ids.NewAt(ids.KindConfig, at, 3), PlanID: ids.NewAt(ids.KindPlan, at, 4),
		TenantID: ids.NewAt(ids.KindTenant, at, 5), ProjectID: ids.NewAt(ids.KindProject, at, 6),
		RenderGeneration: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	authored, err := AuthoringComposeVolumes(rendered.CanonicalYaml,
		[]projectionrecord.EnvironmentVolumeIdentity{{ID: volumeID, Key: "data", Slug: "stable-data"}},
		volumeDirectory)
	if err != nil {
		t.Fatal(err)
	}
	scope := blueprintparser.EnvironmentScope{EnvironmentID: environmentID,
		Tenant: "tenant", Project: "project", Environment: "production"}
	document, err := blueprintparser.MarshalAuthoringDocument(blueprintparser.AuthoringDocument{
		Envelope: core.Envelope{Kind: core.KindDocEnvironment, Schema: core.EnvelopeSchema,
			Metadata: core.EnvelopeMetadata{
				Tenant:      scope.Tenant,
				Project:     scope.Project,
				Environment: scope.Environment,
			}},
		NetworkPool: "10.40.0.0/16", Compose: authored,
	})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := blueprintparser.Parse(t.Context(), scope, core.BlueprintBundle{
		RootPath: "groundplane.yaml", ComposeSources: []string{"groundplane.yaml"},
		Files: []core.BlueprintFile{{Path: "groundplane.yaml", Content: document}},
	})
	if err != nil {
		t.Fatalf("canonical direct Volume could not be applied: %v", err)
	}
	if len(parsed.Project.Volumes["data"].DriverOpts) != 0 ||
		parsed.Project.Volumes["data"].Extensions[ComposeVolumeSlugExtension] != "stable-data" ||
		parsed.Project.Volumes["authored"].DriverOpts["o"] != "uid=1000" {
		t.Fatalf("authoring lost Volume decisions or retained runtime bind: %#v", parsed.Project.Volumes)
	}
}
