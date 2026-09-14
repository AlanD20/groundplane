package taskmaterialization

import (
	"crypto/sha256"
	"encoding/hex"
	"path"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumGeneratedEnvironmentValues = 4096
const generatedEnvironmentFormatVersion = 1

var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidateRecord checks the metadata and its closed immutable source together.
func ValidateRecord(record Record, generation uint64) error {
	if ids.Validate(ids.KindStep, record.StepID) != nil || ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil {
		return errs.New(errs.KindValidationFailed, "materialization identity is invalid")
	}
	if err := validateMetadata(record, generation); err != nil {
		return err
	}
	if err := validateSource(record.Source); err != nil {
		return err
	}
	return validateBinding(record)
}

func validServiceName(value string) bool {
	if value == "" || len(value) > 255 || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if character <= ' ' || character == '/' || character == '\\' {
			return false
		}
	}
	return true
}

func validateSourcePath(value string) error {
	if value == "" || !utf8.ValidString(value) || strings.ContainsRune(value, 0) || len(value) > 240 ||
		strings.Contains(value, `\`) || path.IsAbs(value) || path.Clean(value) != value || value == "." || strings.HasPrefix(value, "../") {
		return errs.New(errs.KindValidationFailed, "Blueprint revision path is invalid")
	}
	return nil
}

func validateBinding(reference Record) error {
	valid := false
	switch reference.Source.Kind {
	case SourceBlueprintFile, SourceComponentFile:
		valid = reference.OutputKind == OutputPlainFile
	case SourceEntryValue:
		if reference.Source.EntryValue != nil {
			if reference.Source.EntryValue.Storage == EntryValueStoragePlain {
				valid = reference.OutputKind == OutputPlainFile
			} else if reference.Source.EntryValue.Storage == EntryValueStorageSecret {
				valid = reference.OutputKind == OutputSecretFile
			}
		}
	case SourceGeneratedEnvironment:
		valid = reference.OutputKind == OutputGeneratedEnvironment
	case SourceRemoval:
		valid = reference.OutputKind == OutputRemoveGeneratedEnv ||
			reference.OutputKind == OutputRemovePlainFile ||
			reference.OutputKind == OutputRemoveSecretFile
	}
	if !valid {
		return errs.New(errs.KindValidationFailed, "task materialization source and output kind are inconsistent")
	}
	return nil
}

func validateMetadata(reference Record, renderGeneration uint64) error {
	if ids.Validate(ids.KindConfig, reference.MaterializationID) != nil ||
		reference.Length > entrymaterialization.MaximumContentBytes {
		return errs.New(errs.KindValidationFailed, "task materialization metadata is invalid")
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil || len(digest) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "task materialization digest is invalid")
	}
	if reference.ServiceID != "" && (ids.Validate(ids.KindService, reference.ServiceID) != nil ||
		!validServiceName(reference.ServiceName)) {
		return errs.New(errs.KindValidationFailed, "task materialization Service identity is invalid")
	}
	outputKind := entrymaterialization.OutputKind(0)
	switch reference.OutputKind {
	case OutputGeneratedEnvironment:
		outputKind = entrymaterialization.OutputGeneratedEnv
	case OutputPlainFile:
		outputKind = entrymaterialization.OutputPlainFile
	case OutputSecretFile:
		outputKind = entrymaterialization.OutputSecretFile
	case OutputRemoveGeneratedEnv:
		outputKind = entrymaterialization.OutputRemoveGeneratedEnv
	case OutputRemovePlainFile:
		outputKind = entrymaterialization.OutputRemovePlainFile
	case OutputRemoveSecretFile:
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

func validateSource(source Source) error {
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
	if source.Kind == SourceRemoval {
		if pointers != 0 {
			return errs.New(errs.KindValidationFailed, "removal materialization source union is invalid")
		}
		return nil
	}
	if pointers != 1 {
		return errs.New(errs.KindValidationFailed, "task materialization source union is invalid")
	}
	switch source.Kind {
	case SourceBlueprintFile:
		if source.BlueprintFile == nil || source.ComponentFile != nil || source.EntryValue != nil ||
			source.GeneratedEnvironment != nil ||
			ids.Validate(ids.KindTask, source.BlueprintFile.RevisionID) != nil ||
			validateSourcePath(source.BlueprintFile.Path) != nil {
			return errs.New(errs.KindValidationFailed, "Blueprint file materialization reference is invalid")
		}
	case SourceComponentFile:
		if source.ComponentFile == nil || source.BlueprintFile != nil || source.EntryValue != nil ||
			source.GeneratedEnvironment != nil ||
			ids.Validate(ids.KindTask, source.ComponentFile.RevisionID) != nil ||
			ids.Validate(ids.KindComponent, source.ComponentFile.ComponentID) != nil ||
			validateSourcePath(source.ComponentFile.Path) != nil ||
			(source.ComponentFile.RouteTaskID != "" &&
				ids.Validate(ids.KindTask, source.ComponentFile.RouteTaskID) != nil) {
			return errs.New(errs.KindValidationFailed, "Component file materialization reference is invalid")
		}
	case SourceEntryValue:
		if source.EntryValue == nil || source.BlueprintFile != nil || source.ComponentFile != nil ||
			source.GeneratedEnvironment != nil {
			return errs.New(errs.KindValidationFailed, "Entry value materialization reference is invalid")
		}
		return validateEntryValueReference(*source.EntryValue)
	case SourceGeneratedEnvironment:
		if source.GeneratedEnvironment == nil || source.BlueprintFile != nil || source.ComponentFile != nil ||
			source.EntryValue != nil {
			return errs.New(errs.KindValidationFailed, "generated Environment materialization reference is invalid")
		}
		return validateEnvironment(*source.GeneratedEnvironment)
	default:
		return errs.New(errs.KindValidationFailed, "task materialization source kind is invalid")
	}
	return nil
}

func validateEntryValueReference(reference EntryValueReference) error {
	if ids.Validate(ids.KindEnvEntry, reference.EntryID) != nil ||
		ids.Validate(ids.KindConfig, reference.ValueGenerationID) != nil {
		return errs.New(errs.KindValidationFailed, "Entry value generation reference is invalid")
	}
	switch reference.Storage {
	case EntryValueStoragePlain, EntryValueStorageSecret:
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "Entry value storage kind is invalid")
	}
}

func validateEnvironment(reference GeneratedEnvironmentValueReference) error {
	if reference.FormatVersion != generatedEnvironmentFormatVersion ||
		len(reference.Values) > maximumGeneratedEnvironmentValues {
		return errs.New(errs.KindValidationFailed, "generated Environment reference format is invalid")
	}
	previousName := ""
	for _, value := range reference.Values {
		if value.Name <= previousName || !utf8.ValidString(value.Name) ||
			!environmentName.MatchString(value.Name) {
			return errs.New(errs.KindValidationFailed, "generated Environment value names are not uniquely sorted")
		}
		hasEntry := value.Value.EntryID != "" || value.Value.ValueGenerationID != "" || value.Value.Storage != ""
		if hasEntry == (value.Secret != nil) {
			return errs.New(errs.KindValidationFailed, "generated Environment value source union is invalid")
		}
		if hasEntry {
			if err := validateEntryValueReference(value.Value); err != nil {
				return err
			}
		} else if ids.Validate(ids.KindSecret, value.Secret.SecretID) != nil || value.Secret.Revision <= 0 ||
			len(value.Secret.CiphertextSHA256) != sha256.Size*2 {
			return errs.New(errs.KindValidationFailed, "generated Environment Secret reference is invalid")
		}
		previousName = value.Name
	}
	return nil
}

func Clone(
	references []Record,
) []Record {
	if references == nil {
		return nil
	}
	cloned := make([]Record, len(references))
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
			value.Values = append([]GeneratedEnvironmentEntryReference(nil), value.Values...)
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
