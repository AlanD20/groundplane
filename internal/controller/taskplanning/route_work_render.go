package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type RouteWorkRender struct {
	Artifact *agentpb.ComposeArtifact
	File     taskmaterialization.Record
	Content  []byte
}

// RenderWork retains authored Services as routing context, never as executable
// targets. Its output owns exactly the pinned router and one configuration file.
func (capture *RouteProviderCapture) RenderWork(ctx context.Context, task workexecution.Task, item workexecution.Item,
	project hierarchy.ProjectRecord, environment hierarchy.EnvironmentRecord,
	candidate environmentprojection.EnvironmentComposeProjection, pin environmentchanges.RouteProviderPin,
) (RouteWorkRender, error) {
	if capture == nil || ctx == nil || task.Validate() != nil || item.Validate() != nil ||
		item.TaskID != task.ID || item.Operation != workexecution.Component || item.TargetID != pin.ComponentID ||
		task.Scope.ProjectID != project.ID || task.Scope.EnvironmentID != environment.ID ||
		environment.ProjectID != project.ID || candidate.EnvironmentID != environment.ID || candidate.RevisionID != task.ID ||
		candidate.RenderGeneration != pin.InputGeneration || environmentchanges.ValidateRouteProviderPin(&pin) != nil || pin.Runtime == nil {
		return RouteWorkRender{}, errs.New(errs.KindStateConflict, "Route render scope differs from its captured work")
	}
	identity := pinnedEnvironmentIdentity{TenantID: project.TenantID, ProjectID: project.ID,
		EnvironmentID: environment.ID, EnvironmentName: environment.Name, AuthorizedVolumeDir: environment.VolumeDir}
	compose, err := loadPinnedEnvironmentProject(ctx, candidate, environment.VolumeDir, nil)
	if err != nil {
		return RouteWorkRender{}, err
	}
	managed, err := projectPinnedEnvironmentComponents(compose, nil, identity, candidate, nil,
		projectedEnvironmentEntries(candidate.Entries), capture.catalog)
	if err != nil {
		return RouteWorkRender{}, err
	}
	external, err := routeProviderCompose(pin)(managed.Project, candidate)
	if err != nil {
		return RouteWorkRender{}, err
	}
	identities, err := composerender.ComposeIdentitySnapshotFromProjection(candidate)
	if err != nil {
		return RouteWorkRender{}, err
	}
	selected := composeidentity.Snapshot{Networks: identities.Networks}
	for _, service := range identities.Services {
		if service.ID == pin.ServiceID && service.ComponentID == pin.ComponentID {
			selected.Services = append(selected.Services, service)
		}
	}
	if len(selected.Services) != 1 {
		return RouteWorkRender{}, errs.New(errs.KindStateConflict, "Route router has no unique generated workload identity")
	}
	inputID := ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "captured-input")
	artifactID := ids.DeriveAt(ids.KindConfig, item.CreatedAt, item.ID, "router-artifact")
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: managed.Project, ArtifactID: artifactID, ProjectOwnerKind: pinnedProjectOwner(project.TenantID),
		TenantID: project.TenantID, ProjectID: project.ID, EnvironmentID: environment.ID,
		PlanID: inputID, RenderGeneration: candidate.RenderGeneration, AuthorizedVolumeDir: environment.VolumeDir,
		Identities: selected, ExternalNetworks: external, RetainedComponentRuntime: pin.Runtime.Artifact,
	})
	if err != nil {
		return RouteWorkRender{}, err
	}
	content, err := capture.Configuration(pin, candidate)
	if err != nil {
		return RouteWorkRender{}, err
	}
	digest := sha256.Sum256(content)
	file := taskmaterialization.Record{
		StepID:            ids.DeriveAt(ids.KindStep, item.CreatedAt, item.ID, "router-configuration"),
		MaterializationID: ids.DeriveAt(ids.KindConfig, item.CreatedAt, item.ID, "router-file"),
		EnvironmentID:     environment.ID, Destination: pin.Destination, OutputKind: taskmaterialization.OutputPlainFile,
		Mode: uint32(entrymaterialization.ModeReadOnly), Length: uint64(len(content)), SHA256: hex.EncodeToString(digest[:]),
		Source: taskmaterialization.Source{Kind: taskmaterialization.SourceComponentFile,
			ComponentFile: &taskmaterialization.ComponentFileValueReference{RevisionID: task.ID,
				ComponentID: pin.ComponentID, Path: pin.Destination}},
	}
	return RouteWorkRender{Artifact: artifact, File: file, Content: content}, nil
}
