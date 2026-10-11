package blueprint

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredServiceConfigurationRepository interface {
	TrySettleBlueprintServiceConfiguration(
		context.Context, etcd.TaskRecord, blueprintunits.Unit, bool,
	) (bool, error)
}

func (service *Service) TrySettleServiceConfiguration(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
) (bool, error) {
	if service == nil || ctx == nil {
		return false, errs.New(errs.KindInternal, "Blueprint Service configuration settler is not configured")
	}
	settler, ok := service.repository.(authoredServiceConfigurationRepository)
	if !ok {
		return false, errs.New(errs.KindInternal, "Blueprint Service configuration repository is not configured")
	}
	input, err := service.loadAuthoredParentInput(ctx, parent)
	if err != nil {
		return false, err
	}
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(ctx, input.desired, input.identities)
	if err != nil {
		return false, err
	}
	name := ""
	for _, identity := range input.identities.Services {
		if identity.ID == unit.Target.ID {
			name = identity.Name
			break
		}
	}
	_, active := project.Services[name]
	_, profileDisabled := project.DisabledServices[name]
	if name == "" || !active && !profileDisabled {
		return false, errs.New(errs.KindStateConflict, "Blueprint Service definition is unavailable")
	}
	configurationOnly := profileDisabled
	for _, group := range input.desired.Input.ReleaseGroups {
		configurationOnly = configurationOnly || slices.Contains(group.Services, name)
	}
	// This decision comes from the parent's hash-checked immutable input, not
	// operator-supplied execution flags. The transaction below rechecks its
	// parent, desired unit, projection and independent runtime revision.
	return settler.TrySettleBlueprintServiceConfiguration(ctx, parent, unit, configurationOnly)
}
