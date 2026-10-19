package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

type componentWorkRender struct {
	Artifact   *agentpb.ComposeArtifact
	Projection composerender.EnvironmentComponentComposeProjection
	Entries    []entryrecord.Record
}

// renderComponentWork keeps authored Services as routing context only. They are
// removed from executable Compose content before it is sealed for the Agent.
func (service *Service) renderComponentWork(
	ctx context.Context,
	work workqueue.View,
	placement componentWorkPlacement,
) (componentWorkRender, error) {
	source := placement.Selection.Source
	if placement.Candidate.Desired.ID != work.Item.Item.TargetID ||
		placement.Candidate.Desired.OwnerID != work.Task.Task.Scope.EnvironmentID ||
		source.desired.Record.RevisionID != work.Task.Task.ID {
		return componentWorkRender{}, errs.New(
			errs.KindStateConflict,
			"Component render input belongs to another work item",
		)
	}
	if !placement.Candidate.Desired.Enabled {
		return componentWorkRender{}, nil
	}
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(
		ctx,
		source.desired.Record,
		source.identities.Record,
	)
	if err != nil {
		return componentWorkRender{}, err
	}
	identities, err := authoredOwnedIdentitySnapshot(ctx, source.desired.Record, source.identities.Record)
	if err != nil {
		return componentWorkRender{}, err
	}
	candidate, err := componentrecord.ProjectRecord(placement.Candidate)
	if err != nil {
		return componentWorkRender{}, err
	}
	routeProjection, err := projectAuthoredRuntimeRoutes(
		source.desired.Record,
		source.identities.Record,
		placement.Services,
	)
	if err != nil {
		return componentWorkRender{}, err
	}
	routes := make([]core.Route, len(routeProjection))
	for index, route := range routeProjection {
		routes[index] = route.Desired
	}
	entries, err := projectionrecord.AuthoredEntryRecords(source.desired.Record, source.identities.Record)
	if err != nil {
		return componentWorkRender{}, err
	}
	zones := make([]core.Zone, len(placement.Zones))
	for index, zone := range placement.Zones {
		zones[index] = zone.Record.Desired
	}
	environment := blueprintComponentEnvironment(
		source.environment.Record,
		zones,
		placement.Services,
		routes,
		[]core.Component{candidate},
		entries,
	)
	projected, err := composerender.ProjectEnvironmentComponents(project, environment, service.componentCatalog)
	if err != nil {
		return componentWorkRender{}, err
	}
	if len(projected.Services) != 1 {
		return componentWorkRender{}, errs.New(
			errs.KindStateConflict,
			"Component must render exactly one owned workload",
		)
	}
	generated := make(composetypes.Services, 1)
	for _, identity := range projected.Services {
		definition, found := projected.Project.Services[identity.Name]
		if !found {
			return componentWorkRender{}, errs.New(errs.KindStateConflict, "Component generated workload is missing")
		}
		generated[identity.Name] = definition
	}
	projected.Project.Services, projected.Project.DisabledServices = generated, nil
	projected.Project.Volumes = nil
	owner := composerender.ComposeProjectOwnerTenant
	if source.project.Record.Kind == hierarchyrecord.ProjectKindBacking {
		owner = composerender.ComposeProjectOwnerBacking
	}
	inputID := ids.DeriveAt(ids.KindPlan, work.Item.Item.CreatedAt, work.Item.Item.ID, "component-input")
	artifactID := ids.DeriveAt(ids.KindConfig, work.Item.Item.CreatedAt, inputID, "candidate")
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: projected.Project, ArtifactID: artifactID, ProjectOwnerKind: owner,
		TenantID: source.tenant.Record.ID, ProjectID: source.project.Record.ID, EnvironmentID: work.Task.Task.Scope.EnvironmentID,
		PlanID: inputID, RenderGeneration: source.desired.Record.RenderGeneration, AuthorizedVolumeDir: source.environment.Record.VolumeDir,
		Identities: composeidentity.Snapshot{Services: projected.Services, Networks: identities.Networks},
	})
	if err != nil {
		return componentWorkRender{}, err
	}
	return componentWorkRender{Artifact: artifact, Projection: projected, Entries: entries}, nil
}
