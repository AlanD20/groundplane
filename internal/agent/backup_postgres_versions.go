package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backuppostgres"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Version proofs come from the exact attested container selected for the dump.
// The recorder persists them before starting the effect-bearing operation.
func observePostgresArchive(ctx context.Context, executor *postgres16execution.Executor,
	container postgres16execution.Container, step *agentpb.BackupStepAuthority, database string,
) (backuppostgres.ArchiveEvidence, error) {
	archive := backuppostgres.ArchiveEvidence{PGDumpMajor: postgres16protocol.PostgreSQLMajor,
		AdapterContractVersion: postgres16protocol.AdapterContractVersion}
	for _, probe := range []struct {
		operation postgres16protocol.Operation
		program   string
	}{
		{postgres16protocol.OperationProbePGDump, "pg_dump"},
		{postgres16protocol.OperationProbePGRestore, "pg_restore"},
		{postgres16protocol.OperationProbePSQL, "psql"},
		{postgres16protocol.OperationServerVersion, ""},
	} {
		selectedDatabase := ""
		if probe.program == "" {
			selectedDatabase = database
		}
		request, err := newPostgresRequest(probe.operation, step, selectedDatabase, "", 0, postgres16protocol.Digest{})
		if err != nil {
			return archive, err
		}
		result, err := executor.Execute(ctx, container, request, nil, nil)
		if err != nil {
			return archive, err
		}
		if probe.program == "" {
			archive.SourceServerVersion, err = postgres16protocol.ParseServerVersion(string(result.Proof))
		} else {
			var version string
			version, err = postgres16protocol.ParseToolVersion(probe.program, string(result.Proof))
			if probe.program == "pg_dump" {
				archive.BackupToolVersion = version
			}
		}
		if err != nil {
			return archive, err
		}
	}
	return archive, archive.Validate()
}
