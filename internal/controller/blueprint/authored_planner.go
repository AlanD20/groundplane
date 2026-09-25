package blueprint

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Plan derives only effective inputs that can be reproduced from the parent's
// immutable desired revision. A plan stays open when an Attach resolution or
// hook output has not been pinned into that revision; desired text alone is not
// evidence that those runtime inputs exist.
func (service *Service) Plan(
	ctx context.Context,
	parent etcd.TaskRecord,
	snapshot blueprintunits.Snapshot,
) (blueprintunits.DesiredPlan, error) {
	if service == nil || service.repository == nil || ctx == nil ||
		snapshot.EnvironmentID != parent.Owner.EnvironmentID ||
		snapshot.HeadTaskID != parent.ID || snapshot.ReadRevision <= 0 {
		return blueprintunits.DesiredPlan{}, errs.New(
			errs.KindStateConflict,
			"Blueprint parent unit snapshot changed",
		)
	}
	input, err := service.loadAuthoredParentInput(ctx, parent)
	if err != nil {
		return blueprintunits.DesiredPlan{}, err
	}
	targets, err := selectAuthoredOwnedUnitTargets(input.identities)
	if err != nil {
		return blueprintunits.DesiredPlan{}, err
	}
	planner, err := newAuthoredUnitPlanner(ctx, input, targets)
	if err != nil {
		return blueprintunits.DesiredPlan{}, err
	}
	units, complete, err := planner.plan()
	if err != nil {
		return blueprintunits.DesiredPlan{}, err
	}
	omissions, err := planAcknowledgedOwnedOmissions(
		parent.Owner.EnvironmentID,
		targets,
		snapshot,
	)
	if err != nil {
		return blueprintunits.DesiredPlan{}, err
	}
	omissions = orderAuthoredRemovalUnits(omissions)
	units = append(units, omissions...)
	slices.SortFunc(units, func(left, right blueprintunits.Unit) int {
		return compareBlueprintUnitKeys(left.Target, right.Target)
	})
	for index, unit := range units {
		if index > 0 && units[index-1].Target == unit.Target {
			return blueprintunits.DesiredPlan{}, errs.New(
				errs.KindInternal,
				"Blueprint desired unit target is duplicated",
			)
		}
	}
	if len(units) > 512 {
		return blueprintunits.DesiredPlan{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint desired unit count exceeds the supported bound",
		)
	}
	return blueprintunits.DesiredPlan{
		EnvironmentID: parent.Owner.EnvironmentID,
		ParentTaskID:  parent.ID,
		Complete:      complete,
		Units:         units,
	}, nil
}

func orderAuthoredRemovalUnits(units []blueprintunits.Unit) []blueprintunits.Unit {
	byKind := make(map[ids.Kind][]blueprintunits.ResourceKey)
	for _, unit := range units {
		byKind[unit.Target.Kind] = append(byKind[unit.Target.Kind], unit.Target)
	}
	for index := range units {
		var after []blueprintunits.ResourceKey
		switch units[index].Target.Kind {
		case ids.KindService:
			after = append(after, byKind[ids.KindRoute]...)
			after = append(after, byKind[ids.KindAttach]...)
			after = append(after, byKind[ids.KindScript]...)
		case ids.KindNetwork:
			after = append(after, byKind[ids.KindService]...)
			after = append(after, byKind[ids.KindComponent]...)
		case ids.KindEnvEntry, ids.KindVolume:
			after = append(after, byKind[ids.KindScript]...)
		}
		units[index].After = canonicalAuthoredUnitKeys(after)
	}
	return units
}
