package composekey

import "github.com/AlanD20/groundplane/pkg/errs"

// Validate enforces the Compose specification's resource-key grammar used by
// service, network, volume, config, and secret map keys.
func Validate(value string) error {
	if value == "" {
		return errs.New(errs.KindValidationFailed, "Compose resource name is required")
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return errs.New(
			errs.KindValidationFailed,
			"Compose resource name must match [A-Za-z0-9._-]+",
		)
	}
	return nil
}
