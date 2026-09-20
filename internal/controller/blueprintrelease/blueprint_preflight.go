package blueprintrelease

import (
	"context"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	releasegroup "github.com/AlanD20/groundplane/internal/controller/releasegroup"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// BlueprintPreflightInput carries the candidate and existing desired-state
// snapshots. It carries no durable claim or publication authority.
type BlueprintPreflightInput struct {
	EnvironmentID      string
	Project            *composetypes.Project
	PriorProject       *composetypes.Project
	PreviousIdentities composeidentity.Snapshot
	ServiceExtensions  map[string]core.ServiceExtensionSpec
	CurrentServices    []etcd.Versioned[etcd.ServiceRecord]
	AuthoredGroups     map[string]core.ReleaseGroupSpec
}

// PreflightBlueprint projects candidates and resolves their complete image
// batch before the caller may claim or stage a Blueprint revision.
func (service *Service) PreflightBlueprint(ctx context.Context, input BlueprintPreflightInput,
	groups *releasegroup.ReleaseGroupBlueprintPlanner,
) (WorkloadPreparation, error) {
	if ctx == nil || groups == nil || input.Project == nil {
		return WorkloadPreparation{}, errs.New(errs.KindInternal, "Blueprint image preflight inputs are incomplete")
	}
	// Provisional IDs permit the same projection rules for new Services before a
	// durable claim. Preflight returns only name-bound image/count seals, not IDs.
	identities, err := composeidentity.ReconcileOwned(input.Project, input.PreviousIdentities, ids.New)
	if err != nil {
		return WorkloadPreparation{}, err
	}
	desired, err := controller.ProjectServiceProjection(input.Project, identities.Current, input.ServiceExtensions)
	if err != nil {
		return WorkloadPreparation{}, err
	}
	changes, err := PrepareServiceChanges(input.EnvironmentID, desired, input.CurrentServices)
	if err != nil {
		return WorkloadPreparation{}, err
	}
	memberships, err := BuildNormalizedServiceMemberships(input.PriorProject, input.Project)
	if err != nil {
		return WorkloadPreparation{}, err
	}
	names := make(map[string]string, len(input.CurrentServices))
	for _, record := range input.CurrentServices {
		names[record.Record.Desired.ID] = record.Record.Desired.Name
	}
	effectiveGroups, err := groups.AuthoringSpecs(ctx, input.EnvironmentID, names)
	if err != nil {
		return WorkloadPreparation{}, err
	}
	for name, spec := range input.AuthoredGroups {
		effectiveGroups[name] = spec
	}
	return service.Preflight(ctx, input.EnvironmentID, changes, memberships, effectiveGroups)
}
