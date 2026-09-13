package blueprintreconcile

import (
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func normalizeSnapshot(input Snapshot) (Snapshot, error) {
	result := Snapshot{
		Desired: slices.Clone(
			input.Desired,
		), Applied: slices.Clone(input.Applied), Executions: slices.Clone(input.Executions),
	}
	desired := make(map[ResourceKey]Unit, len(result.Desired))
	for index, unit := range result.Desired {
		normalized, err := normalizeUnit(unit)
		if err != nil {
			return Snapshot{}, err
		}
		if _, duplicate := desired[unit.Target]; duplicate {
			return Snapshot{}, invalidSnapshot("blueprint reconciliation repeats a desired target")
		}
		desired[unit.Target], result.Desired[index] = normalized, normalized
	}
	applied := make(map[ResourceKey]AppliedUnit, len(result.Applied))
	for index, unit := range result.Applied {
		if !validKey(unit.Target) || unit.State != Applied && unit.State != Absent && unit.State != Uncertain ||
			(unit.State == Applied) == (unit.Fingerprint == Fingerprint{}) {
			return Snapshot{}, invalidSnapshot("blueprint reconciliation applied authority is invalid")
		}
		if _, duplicate := applied[unit.Target]; duplicate {
			return Snapshot{}, invalidSnapshot("blueprint reconciliation repeats an applied target")
		}
		writes, err := normalizeKeys(unit.UncertainWrites)
		if err != nil {
			return Snapshot{}, err
		}
		if unit.State == Uncertain && !slices.Contains(writes, unit.Target) ||
			unit.State != Uncertain && len(writes) != 0 {
			return Snapshot{}, invalidSnapshot("blueprint reconciliation uncertain effect scope is invalid")
		}
		unit.UncertainWrites = writes
		result.Applied[index] = unit
		applied[unit.Target] = unit
	}
	for key := range desired {
		if _, found := applied[key]; !found {
			return Snapshot{}, invalidSnapshot(
				"blueprint reconciliation target lacks explicit applied or absent authority",
			)
		}
	}
	if err := validateDependencies(desired); err != nil {
		return Snapshot{}, err
	}
	plans := make(map[string]bool, len(result.Executions))
	pending := make(map[ResourceKey]bool)
	held := make([]Unit, 0, len(result.Executions))
	for index, execution := range result.Executions {
		if ids.Validate(ids.KindPlan, execution.PlanID) != nil || ids.Validate(ids.KindTask, execution.TaskID) != nil ||
			plans[execution.PlanID] || execution.State != Pending && execution.State != Running && execution.State != Draining {
			return Snapshot{}, invalidSnapshot("blueprint reconciliation execution identity or state is invalid")
		}
		plans[execution.PlanID] = true
		unit, err := normalizeUnit(execution.Unit)
		if err != nil {
			return Snapshot{}, err
		}
		result.Executions[index].Unit = unit
		if execution.State != Pending {
			if conflictsAny(unit, held) {
				return Snapshot{}, invalidSnapshot("blueprint reconciliation has conflicting execution owners")
			}
			held = append(held, unit)
		} else if latest, exists := desired[unit.Target]; exists && sameUnit(unit, latest) {
			if pending[unit.Target] {
				return Snapshot{}, invalidSnapshot("blueprint reconciliation repeats a still-required pending execution")
			}
			pending[unit.Target] = true
		}
	}
	slices.SortFunc(result.Desired, func(left, right Unit) int { return compareKey(left.Target, right.Target) })
	slices.SortFunc(
		result.Executions,
		func(left, right Execution) int { return strings.Compare(left.PlanID, right.PlanID) },
	)
	return result, nil
}

func normalizeUnit(input Unit) (Unit, error) {
	if !validKey(input.Target) || input.Fingerprint == (Fingerprint{}) {
		return Unit{}, invalidSnapshot("blueprint reconciliation effective input is invalid")
	}
	var err error
	input.Reads, err = normalizeKeys(input.Reads)
	if err != nil {
		return Unit{}, err
	}
	input.Writes, err = normalizeKeys(input.Writes)
	if err != nil {
		return Unit{}, err
	}
	input.After, err = normalizeKeys(input.After)
	if err != nil {
		return Unit{}, err
	}
	if !slices.Contains(input.Writes, input.Target) || intersects(input.Reads, input.Writes) ||
		slices.Contains(input.After, input.Target) {
		return Unit{}, invalidSnapshot("blueprint reconciliation resource access is invalid")
	}
	return input, nil
}

func normalizeKeys(input []ResourceKey) ([]ResourceKey, error) {
	result := slices.Clone(input)
	slices.SortFunc(result, compareKey)
	for index, key := range result {
		if !validKey(key) || index > 0 && result[index-1] == key {
			return nil, invalidSnapshot("blueprint reconciliation resource set is invalid")
		}
	}
	return result, nil
}

func validKey(key ResourceKey) bool {
	switch key.Kind {
	case ids.KindEnvironment, ids.KindService, ids.KindEnvEntry, ids.KindVolume, ids.KindAttach,
		ids.KindRoute, ids.KindNetwork, ids.KindComponent, ids.KindSecret, ids.KindScript:
		return ids.Validate(key.Kind, key.ID) == nil
	default:
		return false
	}
}

func validateDependencies(units map[ResourceKey]Unit) error {
	remaining := make(map[ResourceKey]int, len(units))
	dependents := make(map[ResourceKey][]ResourceKey)
	ready := make([]ResourceKey, 0, len(units))
	for key, unit := range units {
		remaining[key] = len(unit.After)
		if len(unit.After) == 0 {
			ready = append(ready, key)
		}
		for _, dependency := range unit.After {
			if _, exists := units[dependency]; !exists {
				return invalidSnapshot("blueprint reconciliation dependency is absent")
			}
			dependents[dependency] = append(dependents[dependency], key)
		}
	}
	for len(ready) != 0 {
		key := ready[len(ready)-1]
		ready = ready[:len(ready)-1]
		delete(remaining, key)
		for _, dependent := range dependents[key] {
			remaining[dependent]--
			if remaining[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
	}
	if len(remaining) != 0 {
		return invalidSnapshot("blueprint reconciliation dependencies contain a cycle")
	}
	return nil
}
