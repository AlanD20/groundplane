// Package postgresidentity validates the one PostgreSQL identity generated
// for a credential-backed Attach. It is shared by typed execution boundaries
// that must accept Controller output without broadening the grammar.
package postgresidentity

const (
	maximumIdentityBytes = 63
	randomTailBytes      = 6
)

// ValidGenerated accepts exactly <service-name>_<six-lowercase-ULID-tail>.
// Service names retain the ASCII identity-atom grammar used at generation,
// including hyphens and underscores.
func ValidGenerated(value string) bool {
	separator := len(value) - randomTailBytes - 1
	if separator <= 0 || len(value) > maximumIdentityBytes || value[separator] != '_' ||
		!validServiceName(value[:separator]) {
		return false
	}
	for _, character := range []byte(value[separator+1:]) {
		if !validLowerULIDCharacter(character) {
			return false
		}
	}
	return true
}

func validServiceName(value string) bool {
	for _, character := range []byte(value) {
		if character == '_' || character == '-' || character == '.' ||
			character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' {
			continue
		}
		return false
	}
	return value != ""
}

func validLowerULIDCharacter(character byte) bool {
	return character >= '0' && character <= '9' ||
		character >= 'a' && character <= 'h' ||
		character >= 'j' && character <= 'k' ||
		character >= 'm' && character <= 'n' ||
		character >= 'p' && character <= 't' ||
		character >= 'v' && character <= 'z'
}
