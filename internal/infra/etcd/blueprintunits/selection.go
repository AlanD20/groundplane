package blueprintunits

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Select evaluates the current durable plan against acknowledged effects and
// unfinished executions. A previous desired document or terminal Task status
// alone never counts as an applied result.
func Select(snapshot Snapshot) (blueprintreconcile.Selection, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID {
		return blueprintreconcile.Selection{}, errs.New(errs.KindStateConflict, "current Blueprint unit plan is unavailable")
	}
	desired := snapshot.Desired.Record.Units
	input := blueprintreconcile.Snapshot{
		Desired:    make([]blueprintreconcile.Unit, 0, len(desired)),
		Applied:    make([]blueprintreconcile.AppliedUnit, 0, len(snapshot.Applied)),
		Executions: make([]blueprintreconcile.Execution, 0, len(snapshot.Executions)),
	}
	for _, unit := range desired {
		converted, err := reconcileUnit(unit)
		if err != nil {
			return blueprintreconcile.Selection{}, err
		}
		input.Desired = append(input.Desired, converted)
	}
	for _, value := range snapshot.Applied {
		record := value.Record
		fingerprint, err := reconcileFingerprint(record.Fingerprint)
		if err != nil {
			return blueprintreconcile.Selection{}, err
		}
		input.Applied = append(input.Applied, blueprintreconcile.AppliedUnit{
			Target: reconcileKey(record.Target), State: blueprintreconcile.AppliedState(record.State),
			Fingerprint: fingerprint, AffectedWrites: reconcileKeys(record.AffectedWrites),
		})
	}
	for _, value := range snapshot.Executions {
		record := value.Record
		unit, err := reconcileUnit(record.Unit)
		if err != nil {
			return blueprintreconcile.Selection{}, err
		}
		input.Executions = append(input.Executions, blueprintreconcile.Execution{
			PlanID: record.PlanID, TaskID: record.TaskID, Epoch: record.Epoch,
			Unit: unit, State: blueprintreconcile.ExecutionState(record.State),
		})
	}
	return blueprintreconcile.Select(input)
}

func reconcileUnit(unit Unit) (blueprintreconcile.Unit, error) {
	fingerprint, err := reconcileFingerprint(unit.Fingerprint)
	if err != nil {
		return blueprintreconcile.Unit{}, err
	}
	return blueprintreconcile.Unit{
		Target: reconcileKey(unit.Target), Removal: unit.Removal, Fingerprint: fingerprint,
		Reads: reconcileKeys(unit.Reads), Writes: reconcileKeys(unit.Writes), After: reconcileKeys(unit.After),
	}, nil
}

func reconcileFingerprint(value string) (blueprintreconcile.Fingerprint, error) {
	var fingerprint blueprintreconcile.Fingerprint
	if value == "" {
		return fingerprint, nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(fingerprint) {
		return fingerprint, errs.New(errs.KindInternal, "Blueprint unit fingerprint is invalid")
	}
	copy(fingerprint[:], decoded)
	return fingerprint, nil
}

func reconcileKey(value ResourceKey) blueprintreconcile.ResourceKey {
	return blueprintreconcile.ResourceKey{Kind: value.Kind, ID: value.ID}
}

func reconcileKeys(values []ResourceKey) []blueprintreconcile.ResourceKey {
	if len(values) == 0 {
		return nil
	}
	result := make([]blueprintreconcile.ResourceKey, len(values))
	for index, value := range values {
		result[index] = reconcileKey(value)
	}
	return result
}
