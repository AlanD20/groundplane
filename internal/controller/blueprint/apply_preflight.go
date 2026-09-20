package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"sort"
)

// applyPreflight holds parsed inputs and workload seals before durable staging.
type applyPreflight struct {
	bundle                core.BlueprintBundle
	parsed                blueprintparser.Result
	currentAttaches       []etcdstore.Versioned[attachrecord.Record]
	attachReadRevision    int64
	requirements          core.BlueprintRequirements
	submittedServiceNames map[string]struct{}
	priorProject          *composetypes.Project
	desiredEnvironment    etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	workloads             blueprintrelease.WorkloadPreparation
}

func (service *Service) prepareApplyPreflight(ctx context.Context, environmentID string, bundle core.BlueprintBundle, baseline applyBaseline, preserveRoutes bool) (applyPreflight, error) {
	environment, project, tenant := baseline.environment, baseline.project, baseline.tenant
	previousProjection, hasProjection, previous := baseline.previousProjection, baseline.hasProjection, baseline.previous
	if preserveRoutes && hasProjection {
		// Component-only edits preserve the selected native workload, including
		// its companion files. The revision guard above binds these bytes to
		// the authoring document; external Blueprint inputs remain closed.
		bundle.Files = append(
			append([]core.BlueprintFile(nil), bundle.Files...),
			previousProjection.Record.RuntimeFiles...)
		sort.Slice(
			bundle.Files,
			func(left, right int) bool { return bundle.Files[left].Path < bundle.Files[right].Path },
		)
	}
	parsed, err := blueprintparser.Parse(ctx, blueprintparser.EnvironmentScope{
		EnvironmentID: environmentID,
		Tenant:        tenant.Record.Slug, Project: project.Record.Slug, Environment: environment.Record.Name,
	}, bundle)
	if err != nil {
		return applyPreflight{}, err
	}
	if err := taskplanning.ValidateEnvironmentBlueprintAvailability(parsed); err != nil {
		return applyPreflight{}, err
	}
	currentAttaches, attachReadRevision, err := service.listBlueprintAttaches(ctx, environmentID)
	if err != nil {
		return applyPreflight{}, err
	}
	requirements, err := environmentBlueprintRequirements(
		parsed.Extensions.Requires,
		parsed.Extensions.Attachments,
		currentAttaches,
		attachReadRevision,
	)
	if err != nil {
		return applyPreflight{}, err
	}
	submittedServiceNames := environmentBlueprintServiceNames(parsed.Project)
	var priorProject *composetypes.Project
	if hasProjection {
		priorProject, err = taskplanning.LoadNormalizedEnvironmentProject(ctx, previousProjection.Record)
		if err != nil {
			return applyPreflight{}, err
		}
	}
	desiredEnvironment := environment
	desiredEnvironment.Record.NetworkPool = parsed.Extensions.NetworkPool
	if err := preserveEnvironmentBlueprintResources(
		parsed.Project, priorProject, previous, previousProjection.Record.Volumes, hasProjection,
	); err != nil {
		return applyPreflight{}, err
	}
	preflightServices, err := service.listBlueprintServices(ctx, environmentID)
	if err != nil {
		return applyPreflight{}, err
	}
	preflightExtensions, err := preserveEnvironmentBlueprintServiceExtensions(
		parsed.ServiceExtensions, submittedServiceNames, previous.Services, previousProjection.Record.ServiceExtensions,
	)
	if err != nil {
		return applyPreflight{}, err
	}
	workloads, err := service.blueprintReleases.PreflightBlueprint(ctx, blueprintrelease.BlueprintPreflightInput{
		EnvironmentID: environmentID, Project: parsed.Project, PriorProject: priorProject,
		PreviousIdentities: previous, ServiceExtensions: preflightExtensions,
		CurrentServices: preflightServices, AuthoredGroups: parsed.Extensions.ReleaseGroups,
	}, service.releaseGroups)
	if err != nil {
		return applyPreflight{}, err
	}

	return applyPreflight{bundle: bundle, parsed: parsed, currentAttaches: currentAttaches, attachReadRevision: attachReadRevision, requirements: requirements, submittedServiceNames: submittedServiceNames, priorProject: priorProject, desiredEnvironment: desiredEnvironment, workloads: workloads}, nil
}
