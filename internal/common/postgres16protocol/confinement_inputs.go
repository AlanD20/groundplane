package postgres16protocol

import (
	"bytes"
	"github.com/AlanD20/groundplane/internal/common/postgresidentity"
	"strings"
	"unicode/utf8"
)

func validateExactFile(identity ConfinementFileIdentity, expectedPath string, expectedMode uint32) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if identity.Path != expectedPath || identity.UID != 0 || identity.GID != 0 || identity.Mode != expectedMode {
		return invalidConfinement("postgres helper fixed file identity is invalid")
	}
	return nil
}

func validateClientFile(identity ConfinementFileIdentity, expectedPath string) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if identity.Path != expectedPath || identity.UID != 0 || identity.GID != 0 || identity.Mode&0o6022 != 0 {
		return invalidConfinement("postgres helper client file identity is invalid")
	}
	return nil
}

func validText(value string) bool {
	return value != "" && len(value) <= confinementMaximumTextBytes && utf8.ValidString(value) &&
		!strings.ContainsRune(value, 0)
}

func validGroups(groups []uint32) bool {
	if len(groups) > confinementMaximumGroups {
		return false
	}
	for index := 1; index < len(groups); index++ {
		if groups[index-1] >= groups[index] {
			return false
		}
	}
	return true
}

func validArguments(arguments [][]byte) bool {
	if len(arguments) == 0 || len(arguments) > confinementMaximumArguments {
		return false
	}
	total := uint64(0)
	for _, argument := range arguments {
		if len(argument) == 0 || len(argument) > confinementMaximumArgumentBytes || bytes.IndexByte(argument, 0) >= 0 {
			return false
		}
		total += uint64(len(argument))
		if total > confinementMaximumArgumentsBytes {
			return false
		}
	}
	return true
}

func validClientArguments(operation Operation, arguments [][]byte) bool {
	values := make([]string, len(arguments))
	for index := range arguments {
		values[index] = string(arguments[index])
	}
	switch operation {
	case OperationProbePGDump:
		return equalArgumentValues(values, []string{"pg_dump", "--version"})
	case OperationProbePGRestore:
		return equalArgumentValues(values, []string{"pg_restore", "--version"})
	case OperationProbePSQL:
		return equalArgumentValues(values, []string{"psql", "--version"})
	case OperationServerMajor:
		return validPSQLArguments(
			values,
			"SELECT pg_catalog.current_setting('server_version_num')::integer / 10000;",
		)
	case OperationDump:
		return validDumpArguments(values)
	case OperationRestoreList:
		return equalArgumentValues(values, []string{"pg_restore", "--list", "--no-password"})
	case OperationTerminateDBConnections:
		return validPSQLArguments(
			values,
			"SELECT pg_catalog.coalesce(pg_catalog.bool_and("+
				"pg_catalog.pg_terminate_backend(a.pid)), true) "+
				"FROM pg_catalog.pg_stat_activity AS a "+
				"WHERE a.datname = pg_catalog.current_database() "+
				"AND a.pid <> pg_catalog.pg_backend_pid();",
		)
	case OperationAssertZeroDBConnections:
		return validPSQLArguments(
			values,
			"SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity AS a "+
				"WHERE a.datname = pg_catalog.current_database() "+
				"AND a.pid <> pg_catalog.pg_backend_pid();",
		)
	case OperationRestoreApply:
		return validRestoreApplyArguments(values)
	case OperationPostRestoreVerify:
		return validPSQLArguments(values, "SELECT pg_catalog.current_database();")
	default:
		return false
	}
}

func validDumpArguments(arguments []string) bool {
	if len(arguments) != 10 || !equalArgumentValues(arguments[:8], []string{
		"pg_dump",
		"--format=custom",
		"--compress=0",
		"--no-owner",
		"--no-acl",
		"--host=/var/run/postgresql",
		"--username=postgres",
		"--no-password",
	}) {
		return false
	}
	return validGeneratedArgument(arguments[8], "--role=") &&
		validGeneratedArgument(arguments[9], "--dbname=")
}

func validRestoreApplyArguments(arguments []string) bool {
	if len(arguments) != 12 || !equalArgumentValues(arguments[:10], []string{
		"pg_restore",
		"--clean",
		"--if-exists",
		"--no-owner",
		"--no-acl",
		"--exit-on-error",
		"--single-transaction",
		"--host=/var/run/postgresql",
		"--username=postgres",
		"--no-password",
	}) {
		return false
	}
	return validGeneratedArgument(arguments[10], "--role=") &&
		validGeneratedArgument(arguments[11], "--dbname=")
}

func validPSQLArguments(arguments []string, sql string) bool {
	if len(arguments) != 11 || !equalArgumentValues(arguments[:9], []string{
		"psql",
		"--no-psqlrc",
		"--quiet",
		"--tuples-only",
		"--no-align",
		"--set=ON_ERROR_STOP=1",
		"--host=/var/run/postgresql",
		"--username=postgres",
		"--no-password",
	}) {
		return false
	}
	return validGeneratedArgument(arguments[9], "--dbname=") && arguments[10] == "--command="+sql
}

func validGeneratedArgument(value, prefix string) bool {
	return strings.HasPrefix(value, prefix) && postgresidentity.ValidGenerated(strings.TrimPrefix(value, prefix))
}

func equalArgumentValues(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validEnvironment(environment [][]byte) bool {
	expected := Environment()
	if len(environment) != len(expected) {
		return false
	}
	for index := range expected {
		if len(environment[index]) == 0 || len(environment[index]) > 256 ||
			!bytes.Equal(environment[index], []byte(expected[index])) {
			return false
		}
	}
	return true
}
