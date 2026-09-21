package blueprint

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	testreleasegroup "github.com/AlanD20/groundplane/internal/controller/releasegroup"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: normalized Compose and ordinary Zone artifact addition introduce
// generated names and metadata. Real GET authoring must remain a legal parser
// input while retaining native decisions and opaque managed-file sources.
func TestGetBlueprintNormalizedProjectParsesAgain(t *testing.T) {
	at := time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC)
	tenantID, projectID := ids.NewAt(ids.KindTenant, at, 1), ids.NewAt(ids.KindProject, at, 2)
	environmentID, revisionID := ids.NewAt(ids.KindEnvironment, at, 3), ids.NewAt(ids.KindTask, at, 4)
	scope := blueprintparser.EnvironmentScope{EnvironmentID: environmentID,
		Tenant: "tenant", Project: "project", Environment: "production"}
	source := "kind: environment\nschema: 1\nmetadata:\n  tenant: tenant\n  project: project\n" +
		"  environment: production\nx-gp-network-pool: 10.40.0.0/16\nservices:\n  web:\n" +
		"    image: example/web:1\n    volumes: [data:/data]\nvolumes:\n  data: {}\n"
	bundle := core.BlueprintBundle{RootPath: "groundplane.yaml", ComposeSources: []string{"groundplane.yaml"},
		Files: []core.BlueprintFile{{Path: "groundplane.yaml", Content: []byte(source)}}}
	parsed, err := blueprintparser.Parse(t.Context(), scope, bundle)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := testcomposerender.MarshalNormalizedEnvironmentProject(parsed.Project)
	if err != nil {
		t.Fatal(err)
	}
	zoneArtifact, err := testcomposerender.AddEnvironmentZoneArtifact(&agentpb.ComposeArtifact{
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:   environmentID, CanonicalYaml: normalized,
	}, testcomposerender.ZoneArtifactAddition{
		Zone:      core.Zone{ID: ids.NewAt(ids.KindNetwork, at, 6), Name: "secondary", Subnet: "10.40.20.0/24"},
		ProjectID: projectID, TenantID: tenantID, ArtifactID: ids.NewAt(ids.KindConfig, at, 7),
		PlanID: ids.NewAt(ids.KindPlan, at, 8), RenderGeneration: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	normalized = zoneArtifact.CanonicalYaml
	store := &blueprintTestStore{values: map[string]testkeyvalue.KeyValue{}, revision: 1}
	hierarchy, err := etcd.NewEnvironmentBlueprintRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	groups, err := groupstore.New(store)
	if err != nil {
		t.Fatal(err)
	}
	planner, err := testreleasegroup.NewReleaseGroupBlueprintPlanner(groups, hierarchy.HierarchyRepository)
	if err != nil {
		t.Fatal(err)
	}
	secretID := ids.NewAt(ids.KindSecret, at, 5)
	uid, gid := uint32(1000), uint32(1000)
	repository := &authoringRoundtripRepository{
		blueprintPreflightRepository: &blueprintPreflightRepository{
			tenant: testhierarchy.TenantRecord{ID: tenantID, Slug: scope.Tenant},
			project: testhierarchy.ProjectRecord{
				ID:       projectID,
				TenantID: tenantID,
				Slug:     scope.Project,
				Kind:     testhierarchy.ProjectKindTenant,
			},
			environment: testhierarchy.EnvironmentRecord{ID: environmentID, ProjectID: projectID,
				Name: scope.Environment, NetworkPool: "10.40.0.0/16"},
		},
		projection: testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID:     environmentID,
			RevisionID:        revisionID,
			NormalizedCompose: normalized,
			Entries: []testentries.Record{{BlueprintKey: "settings", Entry: core.EnvEntry{
				Kind: core.EntryKindFile, Path: "config/settings", Exposure: []string{"web"}, Secret: true, UID: &uid, GID: &gid,
				Source: core.EntrySource{Kind: core.SourceSecretRef, SecretRef: secretID},
			}}},
		},
	}
	service := &Service{repository: repository, releaseGroups: planner}
	document, err := service.GetBlueprint(t.Context(), environmentID)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Files[0].Content = []byte(document.Document)
	reparsed, err := blueprintparser.Parse(t.Context(), scope, bundle)
	if err != nil {
		t.Fatalf("GetBlueprint document failed real parser: %v", err)
	}
	if document.Revision != revisionID || len(reparsed.Project.Services) != 1 ||
		len(reparsed.Project.Volumes) != 1 || reparsed.Project.Services["web"].Image != "example/web:1" ||
		reparsed.Project.Networks["secondary"].Ipam.Config[0].Subnet != "10.40.20.0/24" ||
		len(reparsed.Project.Networks["secondary"].Labels) != 0 ||
		len(reparsed.Project.Networks["secondary"].Extensions) != 0 ||
		reparsed.Extensions.Entries["settings"].Source.SecretRef != secretID ||
		reparsed.Extensions.Entries["settings"].Path != "config/settings" ||
		!reparsed.Extensions.Entries["settings"].Secret ||
		reparsed.Extensions.Entries["settings"].UID == nil || *reparsed.Extensions.Entries["settings"].UID != uid ||
		reparsed.Extensions.Entries["settings"].GID == nil || *reparsed.Extensions.Entries["settings"].GID != gid {
		t.Fatal("authoring changed desired decisions or managed-file source")
	}
}

type authoringRoundtripRepository struct {
	*blueprintPreflightRepository
	projection testenvironmentprojection.EnvironmentComposeProjection
	scripts    []testscripts.Record
}

func (r *authoringRoundtripRepository) GetEnvironmentBlueprintHead(
	context.Context,
	string,
) (testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead], bool, error) {
	return testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead]{
		Record: testblueprints.EnvironmentBlueprintHead{
			EnvironmentID: r.projection.EnvironmentID, RevisionID: r.projection.RevisionID},
		Revision: 1,
	}, true, nil
}

func (r *authoringRoundtripRepository) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record:   r.projection,
		Revision: 1,
	}, true, nil
}

func (r *authoringRoundtripRepository) ListScripts(
	context.Context,
	string, testkeyvalue.PageRequest,

) (testkeyvalue.Page[testscripts.Record], error) {
	items := make([]testkeyvalue.Versioned[testscripts.Record], len(r.scripts))
	for index, record := range r.scripts {
		items[index] = testkeyvalue.Versioned[testscripts.Record]{Record: record, Revision: 1, ReadRevision: 1}
	}
	return testkeyvalue.Page[testscripts.Record]{Revision: 1, Items: items}, nil
}

func (*authoringRoundtripRepository) ListAttaches(
	context.Context,
	string, testkeyvalue.PageRequest,

) (testkeyvalue.Page[testattachments.Record], error) {
	return testkeyvalue.Page[testattachments.Record]{Revision: 1}, nil
}
