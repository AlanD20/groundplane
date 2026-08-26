// Package dnsname owns the canonical DNS hostname grammar shared by Route and
// CoreDNS validation.
package dnsname

const maxNameLength = 253

// Valid reports whether value is a canonical lowercase ASCII DNS name within
// the standard 253-byte hostname limit.
func Valid(value string) bool {
	return ValidWithin(value, maxNameLength)
}

// ValidWithin reports whether value is a canonical lowercase ASCII DNS name
// within maxLength bytes. Callers use the bounded form for DNS wire contexts
// that reserve bytes for other record data.
func ValidWithin(value string, maxLength int) bool {
	if maxLength <= 0 || value == "" || len(value) > maxLength || value[len(value)-1] == '.' {
		return false
	}
	for _, label := range splitLabels(value) {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for index := range len(label) {
			character := label[index]
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
				return false
			}
		}
	}
	return true
}

func splitLabels(value string) []string {
	labels := make([]string, 0, 4)
	start := 0
	for index := range value {
		if value[index] != '.' {
			continue
		}
		labels = append(labels, value[start:index])
		start = index + 1
	}
	return append(labels, value[start:])
}
