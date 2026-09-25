package etcd

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// blueprintVolumeChildTaskMatches closes the durable shape used by one
// authored Volume ensure. Task planning decides from immutable identity
// authority whether the child needs its directory prerequisite.
func blueprintVolumeChildTaskMatches(task TaskRecord, unit blueprintunits.Unit) bool {
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	intent := task.Params[taskjournal.TaskBlueprintVolumeIntentParam]
	mode := task.Params[taskjournal.TaskBlueprintVolumeModeParam]
	return unit.Target.Kind == ids.KindVolume && !unit.Removal && unit.Target.ID == task.Target &&
		task.Type == taskjournal.TaskUpdate && task.Actor == taskjournal.TaskActorSystem &&
		task.Executor == taskjournal.TaskExecutorAgent &&
		task.Params[taskjournal.TaskBlueprintVolumeUnitParam] == task.Target &&
		task.Params[blueprints.EnvironmentDesiredRevisionParam] == parentID &&
		ids.Validate(ids.KindTask, parentID) == nil && parentID != task.ID &&
		(mode == taskjournal.TaskBlueprintVolumeModeCreate || mode == taskjournal.TaskBlueprintVolumeModeVerify) &&
		validBlueprintVolumeIntentDigest(intent) && len(task.Params) == 5 &&
		(len(task.Steps) == 1 || len(task.Steps) == 2) &&
		blueprintVolumeChildStepsMatch(task.Steps) &&
		len(task.Materializations) == 0 && task.EntryRuntime == nil && task.Configuration == nil &&
		len(task.ComponentActionStepIDs) == 0 && len(task.ManagedComponentTeardownSources) == 0
}

func blueprintVolumeChildStepsMatch(steps []taskjournal.TaskStepRecord) bool {
	for index, step := range steps {
		if step.Kind != taskjournal.TaskStepOperation ||
			ids.Validate(ids.KindStep, step.ID) != nil || index > 0 && steps[index-1].ID == step.ID {
			return false
		}
	}
	return true
}

func validBlueprintVolumeIntentDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func blueprintVolumeNeedsCreate(snapshot blueprintunits.Snapshot, unit blueprintunits.Unit) (bool, error) {
	if unit.Target.Kind != ids.KindVolume || unit.Removal {
		return false, errs.New(errs.KindStateConflict, "Blueprint Volume unit effect is invalid")
	}
	found := false
	state := blueprintunits.AppliedState("")
	for _, applied := range snapshot.Applied {
		if applied.Record.Target != unit.Target {
			continue
		}
		if found {
			return false, errs.New(errs.KindInternal, "Blueprint Volume applied authority is duplicated")
		}
		found = true
		state = applied.Record.State
	}
	if !found {
		return false, errs.New(errs.KindStateConflict, "Blueprint Volume applied authority is missing")
	}
	switch state {
	case blueprintunits.Absent:
		return true, nil
	case blueprintunits.Applied:
		return false, nil
	default:
		return false, errs.New(errs.KindStateConflict, "Blueprint Volume applied effect is not executable")
	}
}
