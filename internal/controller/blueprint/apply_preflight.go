package blueprint

import (
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// applyPreflight validates authored input before claiming its desired revision.
// Effective files and workload execution are sealed later, after Attach facts.
type applyPreflight struct {
	bundle                core.BlueprintBundle
	parsed                blueprintparser.Result
	currentAttaches       []etcdstore.Versioned[attachrecord.Record]
	requirements          core.BlueprintRequirements
	submittedServiceNames map[string]struct{}
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
	if preserveRoutes && baseline.hasHead {
		bundle.Files = append(append([]core.BlueprintFile(nil), bundle.Files...), previous.Input.RuntimeFiles...)
		sort.Slice(
			bundle.Files,
			func(left, right int) bool { return bundle.Files[left].Path < bundle.Files[right].Path },
		)
	}
	parsed, err := blueprintparser.Parse(ctx, blueprintparser.EnvironmentScope{
		EnvironmentID: environmentID,
		Tenant:        baseline.tenant.Record.Slug, Project: baseline.project.Record.Slug,
		Environment: baseline.environment.Record.Name,
	}, bundle)
	if err != nil {
		return applyPreflight{}, err
	}
	if err := dnsrecords.ValidateNamespaceChanges(
		previous.Input.DNS, parsed.Extensions.DNS,
		baseline.tenant.Record.Slug, baseline.project.Record.Slug, baseline.environment.Record.Name,
	); err != nil {
		return applyPreflight{}, err
	}
	if err := taskplanning.ValidateEnvironmentBlueprintAvailability(parsed); err != nil {
		return applyPreflight{}, err
	}
	if err := service.validateServiceSelection(ctx, environmentID, bundle, parsed); err != nil {
		return applyPreflight{}, err
	}
	if baseline.hasHead && !preserveRoutes {
		if err := requireExplicitBlueprintVolumes(parsed.Project, volumes); err != nil {
			return applyPreflight{}, err
		}
	}
	if err := service.validateBlueprintZoneOmissions(
		ctx, environmentID, baseline.project.Record, parsed, baseline.previous.Networks,
	); err != nil {
		return applyPreflight{}, err
	}
	currentAttaches, attachReadRevision, err := service.listBlueprintAttaches(ctx, environmentID)
	if err != nil {
		return applyPreflight{}, err
	}
	requirements, err := environmentBlueprintRequirements(
		parsed.Extensions.Requires, parsed.Extensions.Attachments, currentAttaches, attachReadRevision,
	)
	if err != nil {
		return applyPreflight{}, err
	}
	submittedServiceNames := environmentBlueprintServiceNames(parsed.Project)
	if preserveRoutes {
		var priorProject = parsed.Project
		if baseline.hasHead {
			priorProject, err = composerender.LoadNormalizedEnvironmentDesiredProject(ctx, previous, identities)
			if err != nil {
				return applyPreflight{}, err
			}
		}
		if err := preserveEnvironmentBlueprintResources(
			parsed.Project, priorProject, baseline.previous, volumes, baseline.hasHead,
		); err != nil {
			return applyPreflight{}, err
		}
	}
	return applyPreflight{
		bundle: bundle, parsed: parsed, currentAttaches: currentAttaches,
		requirements: requirements, submittedServiceNames: submittedServiceNames,
	}, nil
}

// The Environment revision cannot approve removal effects on external consumers.
func (service *Service) validateBlueprintZoneOmissions(
	ctx context.Context, environmentID string, project hierarchyrecord.ProjectRecord,
	parsed blueprintparser.Result, previous []composeidentity.Resource,
) error {
	if project.Kind != hierarchyrecord.ProjectKindBacking {
		return nil
	}
	return service.requireBackingZoneOmissionApproval(ctx, project.ID, environmentID, parsed, previous)
}
