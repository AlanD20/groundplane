package blueprintreconcile

import (
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Select returns deterministic decisions without mutating input or performing
// effects. The caller must atomically fence the complete source snapshot when
// cancelling pending work and admitting Ready units. It must reuse a matching
// pending plan rather than publish another execution. Running/Draining claims
// stay held until independently proven settled; cancellation is not that proof.
func Select(input Snapshot) (Selection, error) {
	snapshot, err := normalizeSnapshot(input)
	if err != nil {
		return Selection{}, err
	}
	desired := make(map[ResourceKey]Unit, len(snapshot.Desired))
	for _, unit := range snapshot.Desired {
		desired[unit.Target] = unit
	}
	applied := make(map[ResourceKey]AppliedUnit, len(snapshot.Applied))
	for _, unit := range snapshot.Applied {
		applied[unit.Target] = unit
	}
	result := Selection{}
	held := make([]Unit, 0, len(snapshot.Executions)+len(snapshot.Desired))
	continuing := make(map[ResourceKey]bool)
	for _, execution := range snapshot.Executions {
		latest, exists := desired[execution.Unit.Target]
		required := exists && sameUnit(latest, execution.Unit)
		switch execution.State {
		case Pending:
			if !required {
				result.CancelPending = append(result.CancelPending, execution.PlanID)
			}
		case Running:
			held = append(held, execution.Unit)
			if required {
				result.ContinueRunning = append(result.ContinueRunning, execution.PlanID)
				continuing[execution.Unit.Target] = true
			} else {
				result.CancelRunning = append(result.CancelRunning, execution.PlanID)
			}
		case Draining:
			held = append(held, execution.Unit)
		}
	}
	for _, unit := range snapshot.Desired {
		if continuing[unit.Target] {
			continue
		}
		if conflictsAny(unit, held) || !dependenciesApplied(unit, desired, applied, held) {
			result.Waiting = append(result.Waiting, unit.Target)
			continue
		}
		prior := applied[unit.Target]
		if prior.State == Uncertain {
			result.ResolveEffects = append(result.ResolveEffects, unit.Target)
			continue
		}
		if prior.State == Applied && prior.Fingerprint == unit.Fingerprint {
			result.Satisfied = append(result.Satisfied, unit.Target)
			continue
		}
		result.Ready = append(result.Ready, unit.Target)
		held = append(held, unit)
	}
	return result, nil
}

func dependenciesApplied(
	unit Unit,
	desired map[ResourceKey]Unit,
	applied map[ResourceKey]AppliedUnit,
	held []Unit,
) bool {
	for _, key := range unit.After {
		dependency := desired[key]
		prior := applied[key]
		if prior.State != Applied || prior.Fingerprint != dependency.Fingerprint ||
			pendingWritesAffect(dependency, held) {
			return false
		}
	}
	return true
}

func pendingWritesAffect(unit Unit, held []Unit) bool {
	for _, other := range held {
		if intersects(other.Writes, unit.Writes) || intersects(other.Writes, unit.Reads) {
			return true
		}
	}
	return false
}

func conflictsAny(unit Unit, held []Unit) bool {
	for _, other := range held {
		if conflicts(unit, other) {
			return true
		}
	}
	return false
}

func conflicts(left, right Unit) bool {
	return intersects(left.Writes, right.Writes) || intersects(left.Writes, right.Reads) ||
		intersects(left.Reads, right.Writes)
}

func intersects(left, right []ResourceKey) bool {
	for i, j := 0, 0; i < len(left) && j < len(right); {
		switch compared := compareKey(left[i], right[j]); {
		case compared == 0:
			return true
		case compared < 0:
			i++
		default:
			j++
		}
	}
	return false
}

func sameUnit(left, right Unit) bool {
	return left.Target == right.Target && left.Fingerprint == right.Fingerprint &&
		slices.Equal(
			left.Reads,
			right.Reads,
		) && slices.Equal(left.Writes, right.Writes) && slices.Equal(left.After, right.After)
}

func compareKey(left, right ResourceKey) int {
	if order := strings.Compare(string(left.Kind), string(right.Kind)); order != 0 {
		return order
	}
	return strings.Compare(left.ID, right.ID)
}

func invalidSnapshot(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}
