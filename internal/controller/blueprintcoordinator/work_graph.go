package blueprintcoordinator

import (
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PreparedUnit is supplied by the resource's typed operation planner. Work IDs
// derive from captured Apply/resource identity; rebuilding a publication batch
// cannot manufacture new identities. Nil Input means dependency outputs are
// still required, not permission to render against the latest desired state.
type PreparedUnit struct {
	Unit          blueprintunits.Unit
	Operation     workexecution.Operation
	Placement     workexecution.Placement
	Input         *workexecution.Input
	EffectClaims  []workexecution.Claim
	RemovalSource *blueprintunits.ServiceRemovalSource
}

// BuildWorkGraph replaces child-Task publication with one captured resource
// graph. Include already-satisfied units as well: their operation owner must
// settle them with applied-state evidence, not omit required dependencies.
func BuildWorkGraph(task workexecution.Task, prepared []PreparedUnit) (blueprintunits.CapturedGraph, error) {
	if err := task.Validate(); err != nil {
		return blueprintunits.CapturedGraph{}, err
	}
	if task.Action != workexecution.ActionApply || task.PublicationComplete ||
		task.Scope.EnvironmentID == "" || task.TargetID != task.Scope.EnvironmentID ||
		len(prepared) != int(task.ExpectedItems) {
		return blueprintunits.CapturedGraph{}, errs.New(errs.KindValidationFailed, "blueprint work graph does not match its captured apply")
	}
	byTarget := make(map[blueprintunits.ResourceKey]string, len(prepared))
	used := make(map[string]struct{}, len(prepared))
	for _, plan := range prepared {
		if err := blueprintunits.ValidateUnit(plan.Unit); err != nil {
			return blueprintunits.CapturedGraph{}, err
		}
		if !workexecution.ValidOperation(plan.Operation) {
			return blueprintunits.CapturedGraph{}, errs.New(errs.KindValidationFailed, "blueprint work requires a stable identity and operation owner")
		}
		if _, exists := byTarget[plan.Unit.Target]; exists {
			return blueprintunits.CapturedGraph{}, errs.New(errs.KindValidationFailed, "blueprint work contains a duplicate resource target")
		}
		workID := ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "blueprint-resource/"+string(plan.Unit.Target.Kind)+"/"+plan.Unit.Target.ID)
		if _, exists := used[workID]; exists {
			return blueprintunits.CapturedGraph{}, errs.New(errs.KindValidationFailed, "blueprint work identity is duplicated")
		}
		byTarget[plan.Unit.Target], used[workID] = workID, struct{}{}
	}
	items := make([]workexecution.Item, 0, len(prepared))
	for _, plan := range prepared {
		item := workexecution.Item{
			ID: byTarget[plan.Unit.Target], TaskID: task.ID, Operation: plan.Operation, TargetID: plan.Unit.Target.ID,
			Placement: plan.Placement, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
			CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline,
		}
		if plan.Operation == workexecution.EntrySettlement {
			captured, err := blueprintunits.EntryWorkInput(task, plan.Unit)
			if err != nil {
				return blueprintunits.CapturedGraph{}, err
			}
			if plan.Placement.Executor != workexecution.Controller || plan.Input != nil && *plan.Input != captured {
				return blueprintunits.CapturedGraph{}, errs.New(errs.KindValidationFailed, "Entry work placement or captured input is inconsistent")
			}
			item.Input = &captured
		} else if plan.Input == nil {
			item.Phase, item.Wait = workexecution.Waiting, &workexecution.Wait{Reason: workexecution.WaitInput}
		} else {
			captured := *plan.Input
			item.Input = &captured
		}
		for _, target := range plan.Unit.After {
			dependency, exists := byTarget[target]
			if !exists {
				return blueprintunits.CapturedGraph{}, errs.New(errs.KindValidationFailed, "blueprint dependency has no captured work item")
			}
			item.Dependencies = append(item.Dependencies, dependency)
		}
		slices.Sort(item.Dependencies)
		// Resource claims are global across hosts. The operation planner supplies
		// additional captured effect claims; placement cannot bypass exclusion.
		for _, target := range plan.Unit.Writes {
			item.Claims = append(item.Claims, workexecution.Claim{ScopeID: target.ID})
		}
		for _, claim := range plan.EffectClaims {
			if !slices.Contains(item.Claims, claim) {
				item.Claims = append(item.Claims, claim)
			}
		}
		slices.SortFunc(item.Claims, func(left, right workexecution.Claim) int {
			if order := strings.Compare(left.HostID, right.HostID); order != 0 {
				return order
			}
			return strings.Compare(left.ScopeID, right.ScopeID)
		})
		items = append(items, item)
	}
	slices.SortFunc(items, func(left, right workexecution.Item) int {
		if left.ID < right.ID {
			return -1
		}
		if left.ID > right.ID {
			return 1
		}
		return 0
	})
	if err := workexecution.ValidateGraph(task, items); err != nil {
		return blueprintunits.CapturedGraph{}, err
	}
	units := make(map[string]blueprintunits.Unit, len(prepared))
	for _, plan := range prepared {
		unit := plan.Unit
		unit.Reads = slices.Clone(unit.Reads)
		unit.Writes = slices.Clone(unit.Writes)
		unit.After = slices.Clone(unit.After)
		units[byTarget[unit.Target]] = unit
	}
	graph := blueprintunits.CapturedGraph{
		TaskID: task.ID, EnvironmentID: task.Scope.EnvironmentID, Items: items,
		Units: make([]blueprintunits.Unit, len(items)),
	}
	for index, item := range items {
		graph.Units[index] = units[item.ID]
	}
	for _, plan := range prepared {
		if plan.RemovalSource != nil {
			source := *plan.RemovalSource
			source.WorkItemID = byTarget[plan.Unit.Target]
			graph.ServiceRemovals = append(graph.ServiceRemovals, source)
		}
	}
	slices.SortFunc(graph.ServiceRemovals, func(a, b blueprintunits.ServiceRemovalSource) int {
		return strings.Compare(a.WorkItemID, b.WorkItemID)
	})
	return graph, nil
}
