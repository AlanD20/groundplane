package backinghook

import (
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ValidateConfiguration validates operator-authored hook configuration before
// it is persisted or frozen into a Task. Secret references are resolved and
// scope-checked by the owning application service.
func ValidateConfiguration(configuration Configuration) error {
	definitions := []*Definition{
		configuration.Attach,
		configuration.Detach,
		configuration.BeforeStop,
		configuration.AfterStart,
	}
	for _, definition := range definitions {
		if definition != nil {
			if err := validateAuthoredDefinition(*definition); err != nil {
				return err
			}
		}
	}
	if err := validateSchema(configuration.Facts); err != nil {
		return err
	}
	if len(configuration.Facts) != 0 && configuration.Attach == nil {
		return errs.New(errs.KindValidationFailed, "backing hook facts require an attach hook")
	}
	if len(configuration.Inputs) != 0 && allDefinitionsNil(definitions) {
		return errs.New(errs.KindValidationFailed, "backing hook inputs require a hook")
	}
	seen := make(map[string]struct{}, len(configuration.Inputs))
	generated := false
	for _, input := range configuration.Inputs {
		if !ValidKey(input.Key) || input.Key == "HOST" {
			return errs.New(errs.KindValidationFailed, "backing hook input key is invalid or reserved")
		}
		if _, duplicate := seen[input.Key]; duplicate {
			return errs.New(errs.KindValidationFailed, "backing hook input key is duplicated")
		}
		seen[input.Key] = struct{}{}
		sources := 0
		if input.Value != nil {
			sources++
			if !utf8.ValidString(*input.Value) || strings.IndexByte(*input.Value, 0) >= 0 {
				return errs.New(errs.KindValidationFailed, "backing hook literal input is invalid")
			}
		}
		if input.SecretRef != "" {
			sources++
			if !utf8.ValidString(input.SecretRef) || strings.IndexByte(input.SecretRef, 0) >= 0 {
				return errs.New(errs.KindValidationFailed, "backing hook secret input is invalid")
			}
		}
		if input.Generate != "" {
			sources++
			if input.Generate != GeneratePassword {
				return errs.New(errs.KindValidationFailed, "backing hook generated input is invalid")
			}
			generated = true
		}
		if sources != 1 {
			return errs.New(errs.KindValidationFailed, "backing hook input must set exactly one source")
		}
	}
	if generated && (configuration.BeforeStop != nil || configuration.AfterStart != nil) {
		return errs.New(errs.KindValidationFailed, "generated backing hook input is attach-scoped")
	}
	return nil
}

func validateAuthoredDefinition(definition Definition) error {
	if len(definition.Command) == 0 || len(definition.Command) > maximumCommandItems ||
		definition.TimeoutSeconds == 0 || definition.TimeoutSeconds > MaximumTimeoutSeconds {
		return errs.New(errs.KindValidationFailed, "backing hook definition is invalid")
	}
	commandBytes := 0
	for index, argument := range definition.Command {
		commandBytes += len(argument)
		if (index == 0 && argument == "") || !utf8.ValidString(argument) ||
			strings.IndexByte(argument, 0) >= 0 || commandBytes > maximumCommandBytes {
			return errs.New(errs.KindValidationFailed, "backing hook command is invalid")
		}
	}
	return nil
}

func allDefinitionsNil(definitions []*Definition) bool {
	for _, definition := range definitions {
		if definition != nil {
			return false
		}
	}
	return true
}

func CloneConfiguration(source *Configuration) *Configuration {
	if source == nil {
		return nil
	}
	clone := *source
	clone.Attach = cloneDefinition(source.Attach)
	clone.Detach = cloneDefinition(source.Detach)
	clone.BeforeStop = cloneDefinition(source.BeforeStop)
	clone.AfterStart = cloneDefinition(source.AfterStart)
	clone.Facts = append([]FactDefinition(nil), source.Facts...)
	clone.Inputs = append([]InputDefinition(nil), source.Inputs...)
	for index := range clone.Inputs {
		if source.Inputs[index].Value != nil {
			value := *source.Inputs[index].Value
			clone.Inputs[index].Value = &value
		}
	}
	return &clone
}

func cloneDefinition(source *Definition) *Definition {
	if source == nil {
		return nil
	}
	clone := *source
	clone.Command = append([]string(nil), source.Command...)
	return &clone
}

func EqualConfiguration(left, right *Configuration) bool {
	if left == nil || right == nil {
		return left == right
	}
	if !equalDefinition(left.Attach, right.Attach) ||
		!equalDefinition(left.Detach, right.Detach) ||
		!equalDefinition(left.BeforeStop, right.BeforeStop) ||
		!equalDefinition(left.AfterStart, right.AfterStart) ||
		!slices.Equal(left.Facts, right.Facts) || len(left.Inputs) != len(right.Inputs) {
		return false
	}
	for index := range left.Inputs {
		leftInput, rightInput := left.Inputs[index], right.Inputs[index]
		if leftInput.Key != rightInput.Key || leftInput.SecretRef != rightInput.SecretRef ||
			leftInput.Generate != rightInput.Generate || !equalStringPointer(leftInput.Value, rightInput.Value) {
			return false
		}
	}
	return true
}

func equalDefinition(left, right *Definition) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.TimeoutSeconds == right.TimeoutSeconds && slices.Equal(left.Command, right.Command)
}

func equalStringPointer(left, right *string) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}
