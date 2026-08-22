package etcd

import (
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumTaskMaterializations       = 512
	maximumGeneratedEnvironmentValues = 4096
	generatedEnvironmentFormatVersion = 1
)

type TaskMaterializationSourceKind string

const (
	TaskMaterializationSourceBlueprintFile        TaskMaterializationSourceKind = "blueprint_file"
	TaskMaterializationSourceEntryValue           TaskMaterializationSourceKind = "entry_value"
	TaskMaterializationSourceGeneratedEnvironment TaskMaterializationSourceKind = "generated_environment"
)

type TaskEntryValueStorage string

const (
	TaskEntryValueStoragePlain  TaskEntryValueStorage = "plain"
	TaskEntryValueStorageSecret TaskEntryValueStorage = "secret"
)

// TaskMaterializationRecord is the durable Controller-only source ledger for
// one metadata-only Agent materialization step. It never contains value bytes.
type TaskMaterializationRecord struct {
	StepID        string                    `json:"step_id"`
	EnvironmentID string                    `json:"environment_id"`
	Source        TaskMaterializationSource `json:"source"`
}

// TaskMaterializationSource is a closed tagged union. Exactly one pointer must
// match Kind so the durable JSON cannot acquire fallback resolution behavior.
type TaskMaterializationSource struct {
	Kind                 TaskMaterializationSourceKind           `json:"kind"`
	BlueprintFile        *TaskBlueprintFileValueReference        `json:"blueprint_file,omitempty"`
	EntryValue           *TaskEntryValueReference                `json:"entry_value,omitempty"`
	GeneratedEnvironment *TaskGeneratedEnvironmentValueReference `json:"generated_environment,omitempty"`
}

type TaskBlueprintFileValueReference struct {
	RevisionID string `json:"revision_id"`
	Path       string `json:"path"`
}

// TaskEntryValueReference names one immutable value generation. Storage is
// explicit so plain desired values and encrypted subordinate values cannot be
// resolved through the wrong repository.
type TaskEntryValueReference struct {
	EntryID           string                `json:"entry_id"`
	ValueGenerationID string                `json:"value_generation_id"`
	Storage           TaskEntryValueStorage `json:"storage"`
}

type TaskGeneratedEnvironmentValueReference struct {
	FormatVersion uint32                                   `json:"format_version"`
	Values        []TaskGeneratedEnvironmentEntryReference `json:"values"`
}

type TaskGeneratedEnvironmentEntryReference struct {
	Name  string                  `json:"name"`
	Value TaskEntryValueReference `json:"value"`
}

func validateTaskMaterializationReferences(
	references []TaskMaterializationRecord,
	steps []TaskStepRecord,
	environmentID string,
	hasEnvironment bool,
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
	for _, reference := range references {
		if reference.StepID <= previousStepID || reference.EnvironmentID != environmentID {
			return errs.New(errs.KindValidationFailed, "task materialization references are not uniquely sorted")
		}
		if _, exists := availableSteps[reference.StepID]; !exists {
			return errs.New(errs.KindValidationFailed, "task materialization reference step is unknown")
		}
		if err := validateTaskMaterializationSource(reference.Source); err != nil {
			return err
		}
		previousStepID = reference.StepID
	}
	return nil
}

func validateTaskMaterializationSource(source TaskMaterializationSource) error {
	pointers := 0
	if source.BlueprintFile != nil {
		pointers++
	}
	if source.EntryValue != nil {
		pointers++
	}
	if source.GeneratedEnvironment != nil {
		pointers++
	}
	if pointers != 1 {
		return errs.New(errs.KindValidationFailed, "task materialization source union is invalid")
	}
	switch source.Kind {
	case TaskMaterializationSourceBlueprintFile:
		if source.BlueprintFile == nil || source.EntryValue != nil || source.GeneratedEnvironment != nil ||
			validateStableID(ids.KindTask, source.BlueprintFile.RevisionID) != nil ||
			validateEnvironmentBlueprintPath(source.BlueprintFile.Path) != nil {
			return errs.New(errs.KindValidationFailed, "Blueprint file materialization reference is invalid")
		}
	case TaskMaterializationSourceEntryValue:
		if source.EntryValue == nil || source.BlueprintFile != nil || source.GeneratedEnvironment != nil {
			return errs.New(errs.KindValidationFailed, "Entry value materialization reference is invalid")
		}
		return validateTaskEntryValueReference(*source.EntryValue)
	case TaskMaterializationSourceGeneratedEnvironment:
		if source.GeneratedEnvironment == nil || source.BlueprintFile != nil || source.EntryValue != nil {
			return errs.New(errs.KindValidationFailed, "generated Environment materialization reference is invalid")
		}
		return validateTaskGeneratedEnvironmentReference(*source.GeneratedEnvironment)
	default:
		return errs.New(errs.KindValidationFailed, "task materialization source kind is invalid")
	}
	return nil
}

func validateTaskEntryValueReference(reference TaskEntryValueReference) error {
	if validateStableID(ids.KindEnvEntry, reference.EntryID) != nil ||
		validateStableID(ids.KindConfig, reference.ValueGenerationID) != nil {
		return errs.New(errs.KindValidationFailed, "Entry value generation reference is invalid")
	}
	switch reference.Storage {
	case TaskEntryValueStoragePlain, TaskEntryValueStorageSecret:
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "Entry value storage kind is invalid")
	}
}

func validateTaskGeneratedEnvironmentReference(reference TaskGeneratedEnvironmentValueReference) error {
	if reference.FormatVersion != generatedEnvironmentFormatVersion ||
		len(reference.Values) > maximumGeneratedEnvironmentValues {
		return errs.New(errs.KindValidationFailed, "generated Environment reference format is invalid")
	}
	previousName := ""
	for _, value := range reference.Values {
		if value.Name <= previousName || !utf8.ValidString(value.Name) ||
			!environmentBlueprintInterpolationKey.MatchString(value.Name) {
			return errs.New(errs.KindValidationFailed, "generated Environment value names are not uniquely sorted")
		}
		if err := validateTaskEntryValueReference(value.Value); err != nil {
			return err
		}
		previousName = value.Name
	}
	return nil
}

func cloneTaskMaterializationReferences(
	references []TaskMaterializationRecord,
) []TaskMaterializationRecord {
	if references == nil {
		return nil
	}
	cloned := make([]TaskMaterializationRecord, len(references))
	for index, reference := range references {
		cloned[index] = reference
		source := reference.Source
		if source.BlueprintFile != nil {
			value := *source.BlueprintFile
			source.BlueprintFile = &value
		}
		if source.EntryValue != nil {
			value := *source.EntryValue
			source.EntryValue = &value
		}
		if source.GeneratedEnvironment != nil {
			value := *source.GeneratedEnvironment
			value.Values = append([]TaskGeneratedEnvironmentEntryReference(nil), value.Values...)
			source.GeneratedEnvironment = &value
		}
		cloned[index].Source = source
	}
	return cloned
}
