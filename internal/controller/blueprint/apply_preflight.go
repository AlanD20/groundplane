package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
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

type applyPreflightSource struct {
	authored           bool
	environment        etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	project            etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	tenant             etcdstore.Versioned[hierarchyrecord.TenantRecord]
	previousFiles      []core.BlueprintFile
	previousVolumes    []projectionrecord.EnvironmentVolumeIdentity
	previousExtensions map[string]core.ServiceExtensionSpec
	previous           composeidentity.Snapshot
	hasPrevious        bool
	loadPrior          func(context.Context) (*composetypes.Project, error)
}

func (service *Service) prepareApplyPreflight(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	baseline applyBaseline,
	preserveRoutes bool,
) (applyPreflight, error) {
	projection := baseline.previousProjection.Record
	return service.prepareApplyPreflightFromSource(ctx, environmentID, bundle, applyPreflightSource{
		authored:    true,
		environment: baseline.environment, project: baseline.project, tenant: baseline.tenant,
		previousFiles: projection.RuntimeFiles, previousVolumes: projection.Volumes,
		previousExtensions: projection.ServiceExtensions, previous: baseline.previous,
		hasPrevious: baseline.hasProjection,
		loadPrior: func(ctx context.Context) (*composetypes.Project, error) {
			return composerender.LoadNormalizedEnvironmentProject(ctx, projection)
		},
	}, preserveRoutes)
}

func (service *Service) prepareAuthoredApplyPreflight(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	baseline authoredApplyBaseline,
	preserveRoutes bool,
) (applyPreflight, error) {
	previous := baseline.previousInput.Record
	identities := baseline.previousIdentities.Record
	volumes := make([]projectionrecord.EnvironmentVolumeIdentity, len(identities.Volumes))
	for index, identity := range identities.Volumes {
		volumes[index] = projectionrecord.EnvironmentVolumeIdentity{
			ID: identity.ID, Slug: identity.Slug, Key: identity.Name,
		}
	}
	return service.prepareApplyPreflightFromSource(ctx, environmentID, bundle, applyPreflightSource{
		environment: baseline.environment, project: baseline.project, tenant: baseline.tenant,
		previousFiles: previous.Input.RuntimeFiles, previousVolumes: volumes,
		previousExtensions: previous.Input.ServiceExtensions, previous: baseline.previous,
		hasPrevious: baseline.hasHead,
		loadPrior: func(ctx context.Context) (*composetypes.Project, error) {
			return composerender.LoadNormalizedEnvironmentDesiredProject(ctx, previous, identities)
		},
	}, preserveRoutes)
}

func (service *Service) prepareApplyPreflightFromSource(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	source applyPreflightSource,
	preserveRoutes bool,
) (applyPreflight, error) {
	environment, project, tenant := source.environment, source.project, source.tenant
	if preserveRoutes && source.hasPrevious {
		// Component-only edits preserve the selected native workload, including
		// its companion files. The revision guard above binds these bytes to
		// the authoring document; external Blueprint inputs remain closed.
		bundle.Files = append(
			append([]core.BlueprintFile(nil), bundle.Files...),
			source.previousFiles...)
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
	if source.hasPrevious && !preserveRoutes {
		if err := requireExplicitBlueprintVolumes(parsed.Project, source.previousVolumes); err != nil {
			return applyPreflight{}, err
		}
	}
	currentAttaches, attachReadRevision, err := service.listBlueprintAttaches(ctx, environmentID)
	if err != nil {
		return applyPreflight{}, err
	}
	var preflightServices []etcdstore.Versioned[servicerecord.ServiceRecord]
	if !source.authored {
		preflightServices, err = service.listBlueprintServices(ctx, environmentID)
		if err != nil {
			return applyPreflight{}, err
		}
	}
	if !preserveRoutes && !source.authored {
		if err := service.rejectUnsupportedBlueprintOmissions(
			ctx, environmentID, parsed, preflightServices, currentAttaches,
		); err != nil {
			return applyPreflight{}, err
		}
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
	if source.hasPrevious {
		priorProject, err = source.loadPrior(ctx)
		if err != nil {
			return applyPreflight{}, err
		}
	}
	desiredEnvironment := environment
	desiredEnvironment.Record.NetworkPool = parsed.Extensions.NetworkPool
	if preserveRoutes {
		if err := preserveEnvironmentBlueprintResources(
			parsed.Project, priorProject, source.previous, source.previousVolumes, source.hasPrevious,
		); err != nil {
			return applyPreflight{}, err
		}
	}
	preflightExtensions := cloneEnvironmentBlueprintServiceExtensions(parsed.ServiceExtensions)
	if preserveRoutes {
		preflightExtensions, err = preserveEnvironmentBlueprintServiceExtensions(
			parsed.ServiceExtensions,
			submittedServiceNames,
			source.previous.Services,
			source.previousExtensions,
		)
		if err != nil {
			return applyPreflight{}, err
		}
	}
	var workloads blueprintrelease.WorkloadPreparation
	if !source.authored {
		workloads, err = service.blueprintReleases.PreflightBlueprint(ctx, blueprintrelease.BlueprintPreflightInput{
			EnvironmentID: environmentID, Project: parsed.Project, PriorProject: priorProject,
			PreviousIdentities: source.previous, ServiceExtensions: preflightExtensions,
			CurrentServices: preflightServices, AuthoredGroups: parsed.Extensions.ReleaseGroups,
		})
		if err != nil {
			return applyPreflight{}, err
		}
	}

	return applyPreflight{
		bundle:                bundle,
		parsed:                parsed,
		currentAttaches:       currentAttaches,
		attachReadRevision:    attachReadRevision,
		requirements:          requirements,
		submittedServiceNames: submittedServiceNames,
		priorProject:          priorProject,
		desiredEnvironment:    desiredEnvironment,
		workloads:             workloads,
	}, nil
}
