package postgres16helper

import (
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	grounderrs "github.com/AlanD20/groundplane/pkg/errs"
)

func clientArguments(request postgres16protocol.Request) ([][]byte, error) {
	if err := request.Validate(); err != nil || request.Operation == postgres16protocol.OperationStop {
		return nil, grounderrs.New(grounderrs.KindValidationFailed, "postgres helper client request is invalid")
	}
	var arguments []string
	switch request.Operation {
	case postgres16protocol.OperationProbePGDump:
		arguments = []string{"pg_dump", "--version"}
	case postgres16protocol.OperationProbePGRestore:
		arguments = []string{"pg_restore", "--version"}
	case postgres16protocol.OperationProbePSQL:
		arguments = []string{"psql", "--version"}
	case postgres16protocol.OperationServerVersion:
		arguments = append(psqlArguments(request.Database),
			"--command=SELECT pg_catalog.current_setting('server_version');")
	case postgres16protocol.OperationDump:
		arguments = []string{
			"pg_dump", "--format=custom", "--compress=0", "--no-owner", "--no-acl",
			"--host=/var/run/postgresql", "--username=postgres", "--no-password",
			"--role=" + request.Role, "--dbname=" + request.Database,
		}
	case postgres16protocol.OperationRestoreList:
		arguments = []string{"pg_restore", "--list", "--no-password"}
	case postgres16protocol.OperationTerminateDBConnections:
		arguments = append(psqlArguments(request.Database),
			"--command=SELECT COALESCE(pg_catalog.bool_and("+
				"pg_catalog.pg_terminate_backend(a.pid)), true) "+
				"FROM pg_catalog.pg_stat_activity AS a "+
				"WHERE a.datname = pg_catalog.current_database() "+
				"AND a.pid <> pg_catalog.pg_backend_pid();")
	case postgres16protocol.OperationAssertZeroDBConnections:
		arguments = append(psqlArguments(request.Database),
			"--command=SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity AS a "+
				"WHERE a.datname = pg_catalog.current_database() "+
				"AND a.pid <> pg_catalog.pg_backend_pid();")
	case postgres16protocol.OperationRestoreApply:
		arguments = []string{
			"pg_restore", "--clean", "--if-exists", "--no-owner", "--no-acl",
			"--exit-on-error", "--single-transaction", "--host=/var/run/postgresql",
			"--username=postgres", "--no-password",
			"--role=" + request.Role, "--dbname=" + request.Database,
		}
	case postgres16protocol.OperationPostRestoreVerify:
		arguments = append(psqlArguments(request.Database),
			"--command=SELECT pg_catalog.current_database();")
	default:
		return nil, grounderrs.New(grounderrs.KindValidationFailed, "postgres helper client operation is invalid")
	}
	encoded := make([][]byte, len(arguments))
	for index, argument := range arguments {
		encoded[index] = []byte(argument)
	}
	return encoded, nil
}

func psqlArguments(database string) []string {
	return []string{
		"psql", "--no-psqlrc", "--quiet", "--tuples-only", "--no-align",
		"--set=ON_ERROR_STOP=1", "--host=/var/run/postgresql", "--username=postgres",
		"--no-password", "--dbname=" + database,
	}
}
