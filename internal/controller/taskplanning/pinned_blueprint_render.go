package taskplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"maps"
)

type pinnedEnvironmentBlueprintArtifact struct {
	artifact     *agentpb.ComposeArtifact
	projection   etcd.EnvironmentComposeProjection
	components   []componentrecord.Record
	requirements core.BlueprintRequirements
}

func (resolver *TaskPlanResolver) renderPinnedEnvironmentBlueprintArtifact(
	ctx context.Context,
	task etcd.TaskRecord,
	revisionID string,
	artifactID string,
) (pinnedEnvironmentBlueprintArtifact, error) {
	environment, err := resolver.blueprints.GetEnvironment(ctx, task.Target)
	if err != nil {
		return pinnedEnvironmentBlueprintArtifact{}, err
	}
	if environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return pinnedEnvironmentBlueprintArtifact{}, errs.New(
			errs.KindInternal,
			"Blueprint Task Environment is not ready",
		)
	}
	project, err := resolver.blueprints.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return pinnedEnvironmentBlueprintArtifact{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant && project.Record.Kind != hierarchyrecord.ProjectKindBacking {
		return pinnedEnvironmentBlueprintArtifact{}, errs.New(
			errs.KindInternal,
			"Blueprint Task Project kind is invalid",
		)
	}
	projection, found, err := resolver.blueprints.GetEnvironmentComposeProjectionRevision(
		ctx,
		task.Target,
		revisionID,
	)
	if err != nil {
		return pinnedEnvironmentBlueprintArtifact{}, err
	}
	if !found || projection.Record.RevisionID != revisionID ||
		projection.Record.RenderGeneration != uint64(task.RenderGeneration) {
		return pinnedEnvironmentBlueprintArtifact{}, errs.New(
			errs.KindInternal,
			"Blueprint Task Compose projection is stale",
		)
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		projection.Record.ComposeArtifact,
		artifact,
	); err != nil || artifact.GetArtifactId() != artifactID || artifact.GetOwnerId() != task.Target {
		return pinnedEnvironmentBlueprintArtifact{}, errs.New(
			errs.KindInternal,
			"Blueprint Task normalized Compose artifact is corrupt",
		)
	}
	return pinnedEnvironmentBlueprintArtifact{
		artifact:     proto.Clone(artifact).(*agentpb.ComposeArtifact),
		projection:   projection.Record,
		components:   projection.Record.Components,
		requirements: projection.Record.BlueprintRequirements.Clone(),
	}, nil
}

type pinnedEnvironmentIdentity struct {
	TenantID            string
	TenantSlug          string
	ProjectID           string
	ProjectSlug         string
	EnvironmentID       string
	EnvironmentName     string
	AuthorizedVolumeDir string
}

func (resolver *TaskPlanResolver) renderPinnedEnvironmentArtifact(
	ctx context.Context,
	task etcd.TaskRecord,
	identity pinnedEnvironmentIdentity,
	revisionID string,
	artifactID string,
	projection etcd.EnvironmentComposeProjection,
	transform environmentComposeTransform,
) (*agentpb.ComposeArtifact, error) {
	return resolver.renderPinnedEnvironmentArtifactForPhase(
		ctx,
		task,
		identity,
		revisionID,
		artifactID,
		projection,
		"",
		transform,
	)
}

func (resolver *TaskPlanResolver) renderPinnedEnvironmentArtifactForPhase(
	ctx context.Context,
	task etcd.TaskRecord,
	identity pinnedEnvironmentIdentity,
	revisionID string,
	artifactID string,
	projection etcd.EnvironmentComposeProjection,
	phase core.ServiceLifecyclePhase,
	transform environmentComposeTransform,
) (*agentpb.ComposeArtifact, error) {
	return resolver.renderPinnedEnvironmentArtifactForPhaseWithReleases(
		ctx, task, identity, revisionID, artifactID, projection, phase, transform, nil,
	)
}

func (resolver *TaskPlanResolver) renderPinnedEnvironmentArtifactWithReleases(
	ctx context.Context,
	task etcd.TaskRecord,
	identity pinnedEnvironmentIdentity,
	revisionID string,
	artifactID string,
	projection etcd.EnvironmentComposeProjection,
	transform environmentComposeTransform,
	releases map[string]composerender.ComposeReleaseIdentity,
) (*agentpb.ComposeArtifact, error) {
	return resolver.renderPinnedEnvironmentArtifactForPhaseWithReleases(
		ctx, task, identity, revisionID, artifactID, projection, "", transform, releases,
	)
}

func (resolver *TaskPlanResolver) renderPinnedEnvironmentArtifactForPhaseWithReleases(
	ctx context.Context,
	task etcd.TaskRecord,
	identity pinnedEnvironmentIdentity,
	revisionID string,
	artifactID string,
	projection etcd.EnvironmentComposeProjection,
	phase core.ServiceLifecyclePhase,
	transform environmentComposeTransform,
	releases map[string]composerender.ComposeReleaseIdentity,
) (*agentpb.ComposeArtifact, error) {
	if projection.RevisionID != revisionID || projection.EnvironmentID != identity.EnvironmentID {
		return nil, errs.New(errs.KindInternal, "pinned Environment normalized projection changed")
	}
	project, err := loadPinnedEnvironmentProject(ctx, projection, identity.AuthorizedVolumeDir, releases)
	if err != nil {
		return nil, err
	}
	if len(projection.Components) != 0 {
		managed, err := projectPinnedEnvironmentComponents(project, nil, identity, projection,
			nil, nil, projectedEnvironmentEntries(projection.Entries), resolver.componentCatalog)
		if err != nil {
			return nil, err
		}
		project = managed.Project
	}
	if err := composerender.ApplyProjectedServiceDependencyPhase(project, projection.ServiceDependencyPlans, phase); err != nil {
		return nil, err
	}
	externalNetworks := []composeidentity.Resource(nil)
	if transform != nil {
		externalNetworks, err = transform(project, projection)
		if err != nil {
			return nil, err
		}
	} else {
		externalNetworks, err = managedAttachExternalNetworks(project)
		if err != nil {
			return nil, err
		}
	}
	identities, err := composerender.ComposeIdentitySnapshotFromProjection(projection)
	if err != nil {
		return nil, err
	}
	return composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: project, ArtifactID: artifactID,
		ProjectOwnerKind: composerender.ComposeProjectOwnerTenant,
		TenantID:         identity.TenantID, ProjectID: identity.ProjectID, EnvironmentID: identity.EnvironmentID,
		PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
		AuthorizedVolumeDir:      identity.AuthorizedVolumeDir,
		Identities:               identities,
		ExternalNetworks:         externalNetworks,
		Releases:                 releases,
		RetainedComponentRuntime: projection.ComposeArtifact,
	})
}

func (resolver *TaskPlanResolver) parsePinnedEnvironmentBlueprint(
	ctx context.Context,
	identity pinnedEnvironmentIdentity,
	revisionID string,
) (blueprintparser.Result, error) {
	revision, found, err := resolver.blueprints.GetEnvironmentBlueprintRevision(
		ctx, identity.EnvironmentID, revisionID,
	)
	if err != nil {
		return blueprintparser.Result{}, err
	}
	if !found {
		return blueprintparser.Result{}, errs.New(errs.KindInternal, "Blueprint Task immutable revision is missing")
	}
	bundle := core.BlueprintBundle{
		RootPath:       revision.Record.RootPath,
		ComposeSources: append([]string(nil), revision.Record.ComposeSources...),
		Interpolation:  maps.Clone(revision.Record.Interpolation),
		Files:          make([]core.BlueprintFile, len(revision.Record.Files)),
	}
	for index, file := range revision.Record.Files {
		bundle.Files[index] = core.BlueprintFile{Path: file.Path, Content: append([]byte(nil), file.Content...)}
		defer clear(bundle.Files[index].Content)
	}
	return blueprintparser.Parse(ctx, blueprintparser.EnvironmentScope{
		EnvironmentID: identity.EnvironmentID,
		Tenant:        identity.TenantSlug, Project: identity.ProjectSlug, Environment: identity.EnvironmentName,
	}, bundle)
}

func projectedEnvironmentEntries(records []entryrecord.Record) []core.EnvEntry {
	entries := make([]core.EnvEntry, len(records))
	for index, record := range records {
		entries[index] = record.Entry
	}
	return entries
}
