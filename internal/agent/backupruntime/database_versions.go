package backupruntime

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/databaseversion"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/docker/mysql84execution"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ObserveDatabaseVersions(
	ctx context.Context,
	probe *agentpb.DatabaseVersionProbe,
) (target databaseversion.Target, resultErr error) {
	step := probe.GetAuthority()
	if step.GetRestore().GetPostgres() != nil {
		authority, err := postgresSourceAuthority(step, probe.Services, probe.Artifacts)
		if err != nil {
			return databaseversion.Target{}, err
		}
		executor, err := postgres16execution.New(authority.index, nativePostgresArchitecture(), nil)
		if err != nil {
			return databaseversion.Target{}, err
		}
		defer func() {
			if err := executor.Close(); err != nil {
				resultErr = errs.WrapJoined(errs.KindInternal, resultErr, err)
			}
		}()
		container, err := executor.ResolveContainer(ctx, authority.selection)
		if err != nil {
			return databaseversion.Target{}, err
		}
		return observePostgresTarget(ctx, executor, container, step, authority.database)
	}
	if step.GetRestore().GetMysql() != nil {
		authority, err := mysqlSourceAuthority(step, probe.Services, probe.Artifacts)
		if err != nil {
			return databaseversion.Target{}, err
		}
		executor, err := mysql84execution.New(authority.selection, nil)
		if err != nil {
			return databaseversion.Target{}, err
		}
		defer func() {
			if err := executor.Close(); err != nil {
				resultErr = errs.WrapJoined(errs.KindInternal, resultErr, err)
			}
		}()
		container, err := executor.ResolveContainer(ctx)
		if err != nil {
			return databaseversion.Target{}, err
		}
		return observeMySQLTarget(ctx, executor, container, step, authority.database)
	}
	return databaseversion.Target{}, invalidDatabaseStaging()
}

func observePostgresTarget(ctx context.Context, executor *postgres16execution.Executor,
	container postgres16execution.Container, step *agentpb.BackupStepAuthority, database string,
) (databaseversion.Target, error) {
	target := databaseversion.Target{Family: "postgres", ContainerID: container.ID, ImageID: container.ImageID}
	for _, operation := range []postgres16protocol.Operation{postgres16protocol.OperationServerVersion, postgres16protocol.OperationProbePGRestore} {
		selectedDatabase := ""
		if operation == postgres16protocol.OperationServerVersion {
			selectedDatabase = database
		}
		request, err := newPostgresRequest(operation, step, selectedDatabase, "", 0, postgres16protocol.Digest{})
		if err != nil {
			return target, err
		}
		result, err := executor.Execute(ctx, container, request, nil, nil)
		if err != nil {
			return target, err
		}
		if operation == postgres16protocol.OperationServerVersion {
			target.ServerVersion, err = postgres16protocol.ParseServerVersion(string(result.Proof))
		} else {
			target.RestoreToolVersion, err = postgres16protocol.ParseToolVersion("pg_restore", string(result.Proof))
		}
		if err != nil {
			return target, err
		}
	}
	return target, target.Validate()
}

func observeMySQLTarget(ctx context.Context, executor *mysql84execution.Executor,
	container mysql84execution.Container, step *agentpb.BackupStepAuthority, database string,
) (databaseversion.Target, error) {
	target := databaseversion.Target{Family: "mysql", ContainerID: container.ID, ImageID: container.ImageID}
	for _, operation := range []mysql84protocol.Operation{mysql84protocol.OperationServerVersion, mysql84protocol.OperationRestoreToolVersion} {
		selectedDatabase := ""
		if operation == mysql84protocol.OperationServerVersion {
			selectedDatabase = database
		}
		request, err := newMySQLRequest(operation, step, selectedDatabase, "", 0, 0, mysql84protocol.Digest{})
		if err != nil {
			return target, err
		}
		result, err := executor.Execute(ctx, container, request, nil, nil)
		if err != nil {
			return target, err
		}
		if operation == mysql84protocol.OperationServerVersion {
			target.ServerVersion = strings.TrimSpace(string(result.Proof))
		} else {
			target.RestoreToolVersion = strings.TrimSpace(string(result.Proof))
		}
	}
	return target, target.Validate()
}
