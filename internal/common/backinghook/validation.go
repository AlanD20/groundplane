package backinghook

import (
	"bytes"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumTimeoutSeconds = 900
	MaximumResultBytes    = 64 * 1024
	maximumCommandItems   = 64
	maximumCommandBytes   = 64 * 1024
	maximumKeyBytes       = 128
)

// Validate checks the complete sealed hook request before any subprocess is
// started. Secret-bearing values remain caller-owned byte slices.
func Validate(definition Definition, input Input, schema []FactDefinition) error {
	if len(definition.Command) == 0 || len(definition.Command) > maximumCommandItems ||
		definition.TimeoutSeconds == 0 || definition.TimeoutSeconds > MaximumTimeoutSeconds {
		return errs.New(errs.KindValidationFailed, "backing hook definition is invalid")
	}
	commandBytes := 0
	for index, argument := range definition.Command {
		commandBytes += len(argument)
		if (index == 0 && argument == "") || !utf8.ValidString(argument) || strings.IndexByte(argument, 0) >= 0 ||
			commandBytes > maximumCommandBytes {
			return errs.New(errs.KindValidationFailed, "backing hook command is invalid")
		}
	}
	if !validContext(input.Context) {
		return errs.New(errs.KindValidationFailed, "backing hook context is invalid")
	}
	if err := validateValues(input.Values, "input"); err != nil {
		return err
	}
	if err := validateValues(input.Facts, "fact"); err != nil {
		return err
	}
	return validateSchema(schema)
}

func validateValues(values []Value, kind string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !ValidKey(value.Key) || bytes.IndexByte(value.Value, 0) >= 0 {
			return errs.Newf(errs.KindValidationFailed, "backing hook %s is invalid", kind)
		}
		if _, exists := seen[value.Key]; exists {
			return errs.Newf(errs.KindValidationFailed, "backing hook %s is duplicated", kind)
		}
		seen[value.Key] = struct{}{}
	}
	return nil
}

func validateSchema(schema []FactDefinition) error {
	seen := make(map[string]struct{}, len(schema))
	for _, definition := range schema {
		if !ValidKey(definition.Key) {
			return errs.New(errs.KindValidationFailed, "backing hook fact schema is invalid")
		}
		if _, exists := seen[definition.Key]; exists {
			return errs.New(errs.KindValidationFailed, "backing hook fact schema is duplicated")
		}
		seen[definition.Key] = struct{}{}
	}
	return nil
}

// ValidKey reports whether a key can be safely projected into GP_INPUT_* or
// GP_FACT_* without changing its spelling.
func ValidKey(value string) bool {
	if value == "" || len(value) > maximumKeyBytes {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if (character < 'A' || character > 'Z') && (character < 'a' || character > 'z') &&
			(index == 0 || character < '0' || character > '9') &&
			character != '_' {
			return false
		}
	}
	return true
}

func validEvent(event Event) bool {
	switch event {
	case Attach, Detach, BeforeStop, AfterStart:
		return true
	default:
		return false
	}
}

func validContext(value Context) bool {
	if !validEvent(value.Event) || ids.Validate(ids.KindService, value.BackingServiceID) != nil {
		return false
	}
	consumerIDs := []struct {
		kind  ids.Kind
		value string
	}{
		{kind: ids.KindAttach, value: value.AttachID},
		{kind: ids.KindTenant, value: value.TenantID},
		{kind: ids.KindProject, value: value.ProjectID},
		{kind: ids.KindEnvironment, value: value.EnvironmentID},
		{kind: ids.KindService, value: value.ServiceID},
	}
	switch value.Event {
	case Attach, Detach:
		for _, consumerID := range consumerIDs {
			if ids.Validate(consumerID.kind, consumerID.value) != nil {
				return false
			}
		}
	case BeforeStop, AfterStart:
		for _, consumerID := range consumerIDs {
			if consumerID.value != "" {
				return false
			}
		}
	}
	return true
}
