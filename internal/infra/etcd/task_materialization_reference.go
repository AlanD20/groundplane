package etcd

import (
	"crypto/sha256"
	"encoding/hex"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
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
	TaskMaterializationSourceComponentFile        TaskMaterializationSourceKind = "component_file"
	TaskMaterializationSourceEntryValue           TaskMaterializationSourceKind = "entry_value"
	TaskMaterializationSourceGeneratedEnvironment TaskMaterializationSourceKind = "generated_environment"
	TaskMaterializationSourceRemoval              TaskMaterializationSourceKind = "removal"
)

type TaskEntryValueStorage string

const (
	TaskEntryValueStoragePlain  TaskEntryValueStorage = "plain"
	TaskEntryValueStorageSecret TaskEntryValueStorage = "secret"
)

type TaskMaterializationOutputKind string

const (
	TaskMaterializationOutputGeneratedEnvironment TaskMaterializationOutputKind = "generated_env"
	TaskMaterializationOutputPlainFile            TaskMaterializationOutputKind = "plain_file"
	TaskMaterializationOutputSecretFile           TaskMaterializationOutputKind = "secret_file"
	TaskMaterializationOutputRemoveGeneratedEnv   TaskMaterializationOutputKind = "remove_generated_env"
	TaskMaterializationOutputRemovePlainFile      TaskMaterializationOutputKind = "remove_plain_file"
	TaskMaterializationOutputRemoveSecretFile     TaskMaterializationOutputKind = "remove_secret_file"
)

// TaskMaterializationRecord is the durable Controller-only source ledger for
// one metadata-only Agent materialization step. It never contains value bytes.
type TaskMaterializationRecord struct {
	StepID            string                        `json:"step_id"`
	MaterializationID string                        `json:"materialization_id"`
	EnvironmentID     string                        `json:"environment_id"`
	Destination       string                        `json:"destination"`
	ServiceID         string                        `json:"service_id,omitempty"`
	ServiceName       string                        `json:"service_name,omitempty"`
	OutputKind        TaskMaterializationOutputKind `json:"output_kind"`
	UID               uint32                        `json:"uid"`
	GID               uint32                        `json:"gid"`
	Mode              uint32                        `json:"mode"`
	Length            uint64                        `json:"length"`
	SHA256            string                        `json:"sha256"`
	Source            TaskMaterializationSource     `json:"source"`
}

// TaskMaterializationSource is a closed tagged union. Exactly one pointer must
// match Kind so the durable JSON cannot acquire fallback resolution behavior.
type TaskMaterializationSource struct {
	Kind                 TaskMaterializationSourceKind           `json:"kind"`
	BlueprintFile        *TaskBlueprintFileValueReference        `json:"blueprint_file,omitempty"`
	ComponentFile        *TaskComponentFileValueReference        `json:"component_file,omitempty"`
	EntryValue           *TaskEntryValueReference                `json:"entry_value,omitempty"`
	GeneratedEnvironment *TaskGeneratedEnvironmentValueReference `json:"generated_environment,omitempty"`
}

type TaskBlueprintFileValueReference struct {
	RevisionID string `json:"revision_id"`
	Path       string `json:"path"`
}

// TaskComponentFileValueReference names one deterministic generated file.
// RevisionID pins the immutable Blueprint input while ComponentID prevents a
// path collision from authorizing content rendered for another Component.
// RouteTaskID selects that Task's immutable candidate projection;
// empty selects the active projection used by ordinary reconciliation.
type TaskComponentFileValueReference struct {
	RevisionID  string `json:"revision_id"`
	ComponentID string `json:"component_id"`
	Path        string `json:"path"`
	RouteTaskID string `json:"route_task_id,omitempty"`
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
	Name   string                    `json:"name"`
	Value  TaskEntryValueReference   `json:"value"`
	Secret *TaskSecretValueReference `json:"secret,omitempty"`
}

// TaskSecretValueReference pins one reusable Secret without copying value
// bytes into desired state or the durable Task journal.
type TaskSecretValueReference struct {
	SecretID         string `json:"secret_id"`
	Revision         int64  `json:"revision"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
}

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
		if err := validateTaskMaterializationMetadata(reference, renderGeneration); err != nil {
			return err
		}
		if err := validateTaskMaterializationSource(reference.Source); err != nil {
			return err
		}
		if err := validateTaskMaterializationBinding(reference); err != nil {
			return err
		}
		materializationIDs[reference.MaterializationID] = struct{}{}
		previousStepID = reference.StepID
	}
	return nil
}

func validateTaskMaterializationBinding(reference TaskMaterializationRecord) error {
	valid := false
	switch reference.Source.Kind {
	case TaskMaterializationSourceBlueprintFile, TaskMaterializationSourceComponentFile:
		valid = reference.OutputKind == TaskMaterializationOutputPlainFile
	case TaskMaterializationSourceEntryValue:
		if reference.Source.EntryValue != nil {
			if reference.Source.EntryValue.Storage == TaskEntryValueStoragePlain {
				valid = reference.OutputKind == TaskMaterializationOutputPlainFile
			} else if reference.Source.EntryValue.Storage == TaskEntryValueStorageSecret {
				valid = reference.OutputKind == TaskMaterializationOutputSecretFile
			}
		}
	case TaskMaterializationSourceGeneratedEnvironment:
		valid = reference.OutputKind == TaskMaterializationOutputGeneratedEnvironment
	case TaskMaterializationSourceRemoval:
		valid = reference.OutputKind == TaskMaterializationOutputRemoveGeneratedEnv ||
			reference.OutputKind == TaskMaterializationOutputRemovePlainFile ||
			reference.OutputKind == TaskMaterializationOutputRemoveSecretFile
	}
	if !valid {
		return errs.New(errs.KindValidationFailed, "task materialization source and output kind are inconsistent")
	}
	return nil
}

func validateTaskMaterializationMetadata(reference TaskMaterializationRecord, renderGeneration uint64) error {
	if validateStableID(ids.KindConfig, reference.MaterializationID) != nil ||
		reference.Length > entrymaterialization.MaximumContentBytes {
		return errs.New(errs.KindValidationFailed, "task materialization metadata is invalid")
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil || len(digest) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "task materialization digest is invalid")
	}
	if reference.ServiceID != "" && (validateStableID(ids.KindService, reference.ServiceID) != nil ||
		!core.ValidEnvironmentComposeName(reference.ServiceName)) {
		return errs.New(errs.KindValidationFailed, "task materialization Service identity is invalid")
	}
	outputKind := entrymaterialization.OutputKind(0)
	switch reference.OutputKind {
	case TaskMaterializationOutputGeneratedEnvironment:
		outputKind = entrymaterialization.OutputGeneratedEnv
	case TaskMaterializationOutputPlainFile:
		outputKind = entrymaterialization.OutputPlainFile
	case TaskMaterializationOutputSecretFile:
		outputKind = entrymaterialization.OutputSecretFile
	case TaskMaterializationOutputRemoveGeneratedEnv:
		outputKind = entrymaterialization.OutputRemoveGeneratedEnv
	case TaskMaterializationOutputRemovePlainFile:
		outputKind = entrymaterialization.OutputRemovePlainFile
	case TaskMaterializationOutputRemoveSecretFile:
		outputKind = entrymaterialization.OutputRemoveSecretFile
	default:
		return errs.New(errs.KindValidationFailed, "task materialization output kind is invalid")
	}
	if err := entrymaterialization.ValidateMetadata(entrymaterialization.MetadataSpec{
		EnvironmentID: reference.EnvironmentID,
		Generation:    renderGeneration,
		Destination:   reference.Destination,
		ServiceID:     reference.ServiceID,
		ServiceName:   reference.ServiceName,
		OutputKind:    outputKind,
		UID:           reference.UID,
		GID:           reference.GID,
		Mode:          entrymaterialization.Mode(reference.Mode),
	}); err != nil {
		return errs.New(errs.KindValidationFailed, "task materialization output policy is invalid")
	}
	return nil
}

func validateTaskMaterializationSource(source TaskMaterializationSource) error {
	pointers := 0
	if source.BlueprintFile != nil {
		pointers++
	}
	if source.ComponentFile != nil {
		pointers++
	}
	if source.EntryValue != nil {
		pointers++
	}
	if source.GeneratedEnvironment != nil {
		pointers++
	}
	if source.Kind == TaskMaterializationSourceRemoval {
		if pointers != 0 {
			return errs.New(errs.KindValidationFailed, "removal materialization source union is invalid")
		}
		return nil
	}
	if pointers != 1 {
		return errs.New(errs.KindValidationFailed, "task materialization source union is invalid")
	}
	switch source.Kind {
	case TaskMaterializationSourceBlueprintFile:
		if source.BlueprintFile == nil || source.ComponentFile != nil || source.EntryValue != nil ||
			source.GeneratedEnvironment != nil ||
			validateStableID(ids.KindTask, source.BlueprintFile.RevisionID) != nil ||
			validateEnvironmentBlueprintPath(source.BlueprintFile.Path) != nil {
			return errs.New(errs.KindValidationFailed, "Blueprint file materialization reference is invalid")
		}
	case TaskMaterializationSourceComponentFile:
		if source.ComponentFile == nil || source.BlueprintFile != nil || source.EntryValue != nil ||
			source.GeneratedEnvironment != nil ||
			validateStableID(ids.KindTask, source.ComponentFile.RevisionID) != nil ||
			validateStableID(ids.KindComponent, source.ComponentFile.ComponentID) != nil ||
			validateEnvironmentBlueprintPath(source.ComponentFile.Path) != nil ||
			(source.ComponentFile.RouteTaskID != "" &&
				validateStableID(ids.KindTask, source.ComponentFile.RouteTaskID) != nil) {
			return errs.New(errs.KindValidationFailed, "Component file materialization reference is invalid")
		}
	case TaskMaterializationSourceEntryValue:
		if source.EntryValue == nil || source.BlueprintFile != nil || source.ComponentFile != nil ||
			source.GeneratedEnvironment != nil {
			return errs.New(errs.KindValidationFailed, "Entry value materialization reference is invalid")
		}
		return validateTaskEntryValueReference(*source.EntryValue)
	case TaskMaterializationSourceGeneratedEnvironment:
		if source.GeneratedEnvironment == nil || source.BlueprintFile != nil || source.ComponentFile != nil ||
			source.EntryValue != nil {
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
		hasEntry := value.Value.EntryID != "" || value.Value.ValueGenerationID != "" || value.Value.Storage != ""
		if hasEntry == (value.Secret != nil) {
			return errs.New(errs.KindValidationFailed, "generated Environment value source union is invalid")
		}
		if hasEntry {
			if err := validateTaskEntryValueReference(value.Value); err != nil {
				return err
			}
		} else if validateStableID(ids.KindSecret, value.Secret.SecretID) != nil || value.Secret.Revision <= 0 ||
			len(value.Secret.CiphertextSHA256) != sha256.Size*2 {
			return errs.New(errs.KindValidationFailed, "generated Environment Secret reference is invalid")
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
		if source.ComponentFile != nil {
			value := *source.ComponentFile
			source.ComponentFile = &value
		}
		if source.EntryValue != nil {
			value := *source.EntryValue
			source.EntryValue = &value
		}
		if source.GeneratedEnvironment != nil {
			value := *source.GeneratedEnvironment
			value.Values = append([]TaskGeneratedEnvironmentEntryReference(nil), value.Values...)
			for valueIndex := range value.Values {
				if value.Values[valueIndex].Secret != nil {
					secret := *value.Values[valueIndex].Secret
					value.Values[valueIndex].Secret = &secret
				}
			}
			source.GeneratedEnvironment = &value
		}
		cloned[index].Source = source
	}
	return cloned
}
