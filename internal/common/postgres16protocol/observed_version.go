package postgres16protocol

import (
	"strconv"
	"strings"
)

const MaximumVersionBytes = 128

// ParseServerVersion retains the observed patch version. The helper admits only
// the server/tool family compiled into this release; image tags are not evidence.
func ParseServerVersion(output string) (string, error) {
	value := strings.TrimSpace(output)
	if len(value) == 0 || len(value) > MaximumVersionBytes || strings.ContainsAny(value, "\r\n\t") {
		return "", invalid("PostgreSQL observed server version is invalid")
	}
	fields := strings.Fields(value)
	version := fields[0]
	if err := validateVersionNumber(version); err != nil {
		return "", err
	}
	for _, character := range value {
		if character < 0x20 || character > 0x7e {
			return "", invalid("PostgreSQL observed server version is invalid")
		}
	}
	return version, nil
}

// ParseToolVersion extracts the actual version from a fixed client's --version
// response, not an operator-supplied string or the container image label.
func ParseToolVersion(program, output string) (string, error) {
	if program != "pg_dump" && program != "pg_restore" && program != "psql" {
		return "", invalid("PostgreSQL version probe program is invalid")
	}
	value := strings.TrimSpace(output)
	if len(value) > MaximumVersionBytes || strings.ContainsAny(value, "\r\n\t") {
		return "", invalid("PostgreSQL observed tool version is invalid")
	}
	prefix := program + " (PostgreSQL) "
	if !strings.HasPrefix(value, prefix) {
		return "", invalid("PostgreSQL observed tool version is invalid")
	}
	return ParseServerVersion(strings.TrimPrefix(value, prefix))
}

func validateVersionNumber(value string) error {
	parts := strings.Split(value, ".")
	if len(parts) != 2 || parts[0] != strconv.FormatUint(uint64(PostgreSQLMajor), 10) {
		return invalid("PostgreSQL observed version is unsupported")
	}
	minor, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil || strconv.FormatUint(minor, 10) != parts[1] {
		return invalid("PostgreSQL observed version is invalid")
	}
	return nil
}
