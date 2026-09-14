package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumTaskMaterializations       = 512
	generatedEnvironmentFormatVersion = 1
)

type TaskMaterializationSourceKind = taskmaterialization.SourceKind
type TaskEntryValueStorage = taskmaterialization.EntryValueStorage
type TaskMaterializationOutputKind = taskmaterialization.OutputKind
type TaskMaterializationRecord = taskmaterialization.Record
type TaskMaterializationSource = taskmaterialization.Source
type TaskBlueprintFileValueReference = taskmaterialization.BlueprintFileValueReference
type TaskComponentFileValueReference = taskmaterialization.ComponentFileValueReference
type TaskEntryValueReference = taskmaterialization.EntryValueReference
type TaskGeneratedEnvironmentValueReference = taskmaterialization.GeneratedEnvironmentValueReference
type TaskGeneratedEnvironmentEntryReference = taskmaterialization.GeneratedEnvironmentEntryReference
type TaskSecretValueReference = taskmaterialization.SecretValueReference

const (
	TaskMaterializationSourceBlueprintFile        = taskmaterialization.SourceBlueprintFile
	TaskMaterializationSourceComponentFile        = taskmaterialization.SourceComponentFile
	TaskMaterializationSourceEntryValue           = taskmaterialization.SourceEntryValue
	TaskMaterializationSourceGeneratedEnvironment = taskmaterialization.SourceGeneratedEnvironment
	TaskMaterializationSourceRemoval              = taskmaterialization.SourceRemoval
	TaskEntryValueStoragePlain                    = taskmaterialization.EntryValueStoragePlain
	TaskEntryValueStorageSecret                   = taskmaterialization.EntryValueStorageSecret
	TaskMaterializationOutputGeneratedEnvironment = taskmaterialization.OutputGeneratedEnvironment
	TaskMaterializationOutputPlainFile            = taskmaterialization.OutputPlainFile
	TaskMaterializationOutputSecretFile           = taskmaterialization.OutputSecretFile
	TaskMaterializationOutputRemoveGeneratedEnv   = taskmaterialization.OutputRemoveGeneratedEnv
	TaskMaterializationOutputRemovePlainFile      = taskmaterialization.OutputRemovePlainFile
	TaskMaterializationOutputRemoveSecretFile     = taskmaterialization.OutputRemoveSecretFile
)

func validateTaskMaterializationReferences(
	references []TaskMaterializationRecord,
	steps []TaskStepRecord,
	environmentID string,
	hasEnvironment bool,
	renderGeneration uint64,
) error {
	if len(references) == 0 {
		return nil
	}
	if !hasEnvironment || len(references) > maximumTaskMaterializations || len(references) > len(steps) {
		return errs.New(errs.KindValidationFailed, "task materialization references are invalid")
	}
	availableSteps := make(map[string]struct{}, len(steps))
	for _, step := range steps {
		availableSteps[step.ID] = struct{}{}
	}
	previousStepID := ""
	materializationIDs := make(map[string]struct{}, len(references))
	for _, reference := range references {
		if reference.StepID <= previousStepID || reference.EnvironmentID != environmentID {
			return errs.New(errs.KindValidationFailed, "task materialization references are not uniquely sorted")
		}
		if _, exists := availableSteps[reference.StepID]; !exists {
			return errs.New(errs.KindValidationFailed, "task materialization reference step is unknown")
		}
		if _, duplicate := materializationIDs[reference.MaterializationID]; duplicate {
			return errs.New(errs.KindValidationFailed, "task materialization id is duplicated")
		}
		if err := taskmaterialization.ValidateRecord(reference, renderGeneration); err != nil {
			return err
		}
		materializationIDs[reference.MaterializationID] = struct{}{}
		previousStepID = reference.StepID
	}
	return nil
}

func cloneTaskMaterializationReferences(references []TaskMaterializationRecord) []TaskMaterializationRecord {
	return taskmaterialization.Clone(references)
}
