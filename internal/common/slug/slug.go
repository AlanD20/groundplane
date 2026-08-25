// Package slug owns the canonical product slug grammar shared by durable and
// application boundaries.
package slug

import "github.com/AlanD20/groundplane/pkg/errs"

// Valid reports whether value is one lowercase ASCII hyphen label: 1-63
// bytes, alphanumeric ends, and no consecutive hyphens.
func Valid(value string) bool {
	if len(value) == 0 || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	previousHyphen := false
	for index := range len(value) {
		character := value[index]
		letter := character >= 'a' && character <= 'z'
		digit := character >= '0' && character <= '9'
		if !letter && !digit && character != '-' || character == '-' && previousHyphen {
			return false
		}
		previousHyphen = character == '-'
	}
	return true
}

// Validate classifies a noncanonical product slug at the shared grammar
// boundary while allowing callers to name the field in the public error.
func Validate(field string, value string) error {
	if Valid(value) {
		return nil
	}
	return errs.Newf(
		errs.KindValidationFailed,
		"%s must be a lowercase ASCII hyphen label of 1-63 bytes",
		field,
	)
}
