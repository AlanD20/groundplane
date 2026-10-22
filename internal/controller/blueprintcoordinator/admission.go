package blueprintcoordinator

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResourceWorkPlanner selects an operation and placement from the accepted
// resource intent. It performs no effects or independent publication. A missing
// executable input remains waiting until the operation owner captures it from
// this Task's intent and exact dependency results.
type ResourceWorkPlanner interface {
	PrepareBlueprintWork(
		context.Context,
		workexecution.Task,
		blueprintunits.Unit,
		blueprintunits.Snapshot,
		map[string]core.ComponentSpec,
	) (PreparedUnit, error)
}

type ResourceRegistration struct {
	Kind    ids.Kind
	Planner ResourceWorkPlanner
}

type Admission struct {
	planners map[ids.Kind]ResourceWorkPlanner
}

func NewAdmission(registrations []ResourceRegistration) (*Admission, error) {
	planners := make(map[ids.Kind]ResourceWorkPlanner, len(registrations))
	for _, registration := range registrations {
		if registration.Planner == nil || planners[registration.Kind] != nil {
			return nil, errs.New(errs.KindInternal, "Blueprint work planner registration is missing or duplicated")
		}
		switch registration.Kind {
		case ids.KindService, ids.KindNetwork, ids.KindVolume,
			ids.KindAttach, ids.KindComponent:
		default:
			return nil, errs.New(errs.KindInternal, "Blueprint work planner resource kind is unsupported")
		}
		planners[registration.Kind] = registration.Planner
	}
	for _, kind := range []ids.Kind{ids.KindService, ids.KindNetwork, ids.KindVolume,
		ids.KindAttach, ids.KindComponent} {
		if planners[kind] == nil {
			return nil, errs.New(errs.KindInternal, "Blueprint work planner catalog is incomplete")
		}
	}
	return &Admission{planners: planners}, nil
}

func (admission *Admission) Prepare(ctx context.Context, task workexecution.Task,
	plan blueprintunits.DesiredPlan, baseline blueprintunits.Snapshot, components map[string]core.ComponentSpec,
	attaches []blueprintattachinputs.Generation,
) (blueprintunits.CapturedGraph, error) {
	if admission == nil || ctx == nil || !plan.Complete || plan.ParentTaskID != task.ID ||
		plan.EnvironmentID != task.Scope.EnvironmentID || baseline.EnvironmentID != plan.EnvironmentID ||
		baseline.ReadRevision <= 0 || len(plan.Units) != int(task.ExpectedItems) {
		return blueprintunits.CapturedGraph{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint admission does not describe one complete captured plan",
		)
	}
	prepared := make([]PreparedUnit, 0, len(plan.Units))
	controllerBindings := make(map[string]bool, len(attaches))
	for _, generation := range attaches {
		if blueprintattachinputs.ValidateDraft(generation) != nil || generation.ParentTaskID != task.ID ||
			generation.EnvironmentID != task.Scope.EnvironmentID {
			return blueprintunits.CapturedGraph{}, errs.New(errs.KindValidationFailed,
				"Blueprint Attach placement requires this Apply's captured binding")
		}
		controllerBindings[generation.AttachID] = generation.SettlesInController()
	}
	for _, unit := range plan.Units {
		if err := ctx.Err(); err != nil {
			return blueprintunits.CapturedGraph{}, err
		}
		if err := blueprintunits.ValidateUnit(unit); err != nil {
			return blueprintunits.CapturedGraph{}, err
		}
		selected := PreparedUnit{Unit: unit, Operation: workexecution.EntrySettlement,
			Placement: workexecution.Placement{Executor: workexecution.Controller}}
		switch unit.Target.Kind {
		case ids.KindEnvEntry:
		case ids.KindRoute, ids.KindScript, ids.KindDNSRecord:
			selected.Operation = workexecution.ResourceDefinition
			input, err := blueprintunits.DefinitionWorkInput(task, unit)
			if err != nil {
				return blueprintunits.CapturedGraph{}, err
			}
			selected.Input = &input
		default:
			if unit.Target.Kind == ids.KindAttach && !unit.Removal && controllerBindings[unit.Target.ID] {
				selected.Operation = workexecution.Attach
				break
			}
			owner := admission.planners[unit.Target.Kind]
			if owner == nil {
				return blueprintunits.CapturedGraph{}, errs.New(
					errs.KindInternal,
					"Blueprint resource has no work planner",
				)
			}
			// An owner cannot mutate the captured dependency or conflict lists.
			copy := unit
			copy.Reads, copy.Writes, copy.After = slices.Clone(
				unit.Reads,
			), slices.Clone(
				unit.Writes,
			), slices.Clone(
				unit.After,
			)
			var err error
			selected, err = owner.PrepareBlueprintWork(ctx, task, copy, baseline, components)
			if err != nil {
				return blueprintunits.CapturedGraph{}, err
			}
			if selected.Unit.Target != unit.Target || selected.Unit.Removal != unit.Removal ||
				selected.Unit.Fingerprint != unit.Fingerprint || !slices.Equal(selected.Unit.Reads, unit.Reads) ||
				!slices.Equal(selected.Unit.Writes, unit.Writes) || !slices.Equal(selected.Unit.After, unit.After) {
				return blueprintunits.CapturedGraph{}, errs.New(
					errs.KindInternal,
					"Blueprint work planner changed the captured resource plan",
				)
			}
		}
		prepared = append(prepared, selected)
	}
	return BuildWorkGraph(task, prepared)
}
