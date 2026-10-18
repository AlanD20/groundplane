package taskplanning

import (
	"context"
	"maps"

	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type pinnedEnvironmentIdentity struct {
	TenantID            string
	TenantSlug          string
	ProjectID           string
	ProjectSlug         string
	EnvironmentID       string
	EnvironmentName     string
	AuthorizedVolumeDir string
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

func pinnedProjectOwner(tenantID string) composerender.ComposeProjectOwnerKind {
	if tenantID == "" {
		return composerender.ComposeProjectOwnerBacking
	}
	return composerender.ComposeProjectOwnerTenant
}

func projectedEnvironmentEntries(records []entryrecord.Record) []core.EnvEntry {
	entries := make([]core.EnvEntry, len(records))
	for index, record := range records {
		entries[index] = record.Entry
	}
	return entries
}
