package blueprint

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// planAuthoredUnits captures resource membership and ordering without publishing
// runtime files or running hooks. Deferred outputs affect executable input
// readiness, never which resources belong to this accepted Apply.
func (service *Service) planAuthoredUnits(
	ctx context.Context,
	input authoredParentInput,
	attachGenerations map[string]authoredAttachPlanInput,
	snapshot blueprintunits.Snapshot,
) (blueprintunits.DesiredPlan, error) {
	if service == nil || service.repository == nil || ctx == nil ||
		input.desired.EnvironmentID != snapshot.EnvironmentID || snapshot.ReadRevision <= 0 ||
		input.owner.EnvironmentID != input.desired.EnvironmentID ||
		input.identities.EnvironmentID != input.desired.EnvironmentID ||
		input.identities.RevisionID != input.desired.RevisionID ||
		input.identities.RenderGeneration != input.desired.RenderGeneration {
		return blueprintunits.DesiredPlan{}, errs.New(errs.KindStateConflict, "Blueprint planning sources do not describe one captured revision")
	}
	targets, err := selectAuthoredOwnedUnitTargets(input.identities)
	if err != nil {
		return blueprintunits.DesiredPlan{}, err
	}
	planner, err := newAuthoredUnitPlanner(ctx, input, targets, attachGenerations, snapshot)
	if err != nil {
		return blueprintunits.DesiredPlan{}, err
	}
	planner.secretIDs, err = service.resolveAuthoredSecretIDs(ctx, input)
	if err != nil {
		return blueprintunits.DesiredPlan{}, err
	}
	units, complete, err := planner.plan()
	if err != nil {
		return blueprintunits.DesiredPlan{}, err
	}
	omissions := orderAuthoredRemovalUnits(planner.omissions)
	omissions, err = service.orderAuthoredAttachRemovals(
		ctx,
		input.desired.EnvironmentID,
		omissions,
		snapshot.ReadRevision,
	)
	if err != nil {
		return blueprintunits.DesiredPlan{}, err
	}
	componentTargets := make([]blueprintunits.ResourceKey, 0)
	for _, candidates := range [][]blueprintunits.Unit{units, omissions} {
		for _, candidate := range candidates {
			if candidate.Target.Kind == ids.KindComponent {
				componentTargets = append(componentTargets, candidate.Target)
			}
		}
	}
	for index := range omissions {
		if omissions[index].Target.Kind == ids.KindService {
			// Retained definitions and managed Entry files must stop referring
			// to an omitted Service before its physical removal starts.
			for _, candidate := range units {
				if candidate.Target.Kind == ids.KindService || candidate.Target.Kind == ids.KindEnvEntry {
					omissions[index].After = append(omissions[index].After, candidate.Target)
				}
			}
			omissions[index].After = canonicalAuthoredUnitKeys(omissions[index].After)
		}
		if omissions[index].Target.Kind == ids.KindNetwork {
			// Retained Services may be moving off an omitted Zone. Their release
			// strategy must settle before native bridge removal is attempted.
			for _, candidate := range units {
				if candidate.Target.Kind == ids.KindService || candidate.Target.Kind == ids.KindComponent {
					omissions[index].After = append(omissions[index].After, candidate.Target)
				}
			}
			omissions[index].After = canonicalAuthoredUnitKeys(omissions[index].After)
		}
		if omissions[index].Target.Kind != ids.KindRoute {
			continue
		}
		// The new router configuration withdraws omitted matches before their
		// Route absence releases a removed Service's dependency.
		omissions[index].After = append(omissions[index].After, componentTargets...)
		omissions[index].After = canonicalAuthoredUnitKeys(omissions[index].After)
	}
	if err := service.orderAuthoredDNSWithdrawal(ctx, units, omissions, snapshot); err != nil {
		return blueprintunits.DesiredPlan{}, err
	}
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
		EnvironmentID: input.desired.EnvironmentID,
		ParentTaskID:  input.desired.RevisionID,
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
			after = append(after, byKind[ids.KindEnvEntry]...)
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
